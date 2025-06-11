#!/usr/bin/env node
/**
 * nftParser.ts • v3.4 (strict-mode, zero deps кроме axios / cheerio / he)
 *
 * Умеет:
 *   • возобновляться с места остановки;
 *   • параллельно сканировать (CONCURRENCY потоков);
 *   • автосохраняться каждые SAVE_EVERY записей;
 *   • мягко «освежать» старые записи (SYNC_BATCH за запуск);
 *   • корректно вытягивать title / image / description даже у «лёгких» страниц.
 *
 * npm i axios cheerio he
 * npm i -D typescript ts-node @types/node
 *
 * SAVE_EVERY=100 SYNC_BATCH=50 CONCURRENCY=10 npx ts-node nftParser.ts
 */

import axios from "axios";
import * as fs from "fs/promises";
import * as cheerio from "cheerio";
import he from "he";
import { Agent as HttpAgent } from "http";
import { Agent as HttpsAgent } from "https";
import * as path from "path";

/*─────────────────── types ───────────────────*/

interface NftMeta {
  slug:        string;
  title:       string;
  image:       string;
  description: string;
  attrs:       Record<string, string>;
  syncedAt?:   string;
}

/*─────────────────── consts ──────────────────*/

const ID_MAP_URL = "https://cdn.changes.tg/gifts/id-to-name.json";
const BASE_URL   = (slug: string): string => `https://t.me/nft/${slug}`;
const OUT_FILE   = "nft-metadata.json";

const SAVE_EVERY  = Number(process.env.SAVE_EVERY  ?? 100);
const SYNC_BATCH  = Number(process.env.SYNC_BATCH  ?? 50);
const CONCURRENCY = Number(process.env.CONCURRENCY ?? 10);
const DATA_DIR    = process.env.DATA_DIR ?? "data";
const CHUNK_SIZE  = Number(process.env.CHUNK_SIZE ?? 5000); // записей в одном JSON-файле

/*─────────────────── helpers ─────────────────*/

const escapeRegExp = (s: string): string =>
  s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

const clean = (s: string = ""): string =>
  s.replace(/[\u0000-\u001F\u007F-\u00A0\u200B-\u200F]/g, "").trim();

function pickMeta($: cheerio.CheerioAPI, prop: string): string {
  const sel = `meta[property^="${escapeRegExp(prop)}" i]`;
  const val = $(sel).first().attr("content") ?? "";
  return clean(he.decode(val));
}

function pickImage($: cheerio.CheerioAPI): string {
  const fromMeta =
    pickMeta($, "og:image") || pickMeta($, "twitter:image");
  if (fromMeta) return fromMeta;

  const svgImage =
    $('image[xlink\\:href]').attr("xlink:href") ??
    $("image[href]").attr("href") ??
    "";
  return clean(svgImage);
}

function pickTitle($: cheerio.CheerioAPI): string {
  return (
    pickMeta($, "og:title") ||
    pickMeta($, "twitter:title") ||
    $('svg text').first().text().trim() ||
    clean($("title").text())
  );
}

function pickDescription($: cheerio.CheerioAPI): string {
  return (
    pickMeta($, "og:description") ||
    pickMeta($, "twitter:description") ||
    clean($(".tgme_gift_description").text())
  );
}

function slugParts(slug: string): [string, number] {
  const m = slug.match(/^(.*)-(\d+)$/);
  return m ? [m[1], Number(m[2])] : [slug, 0];
}

/*─────────────────── axios ───────────────────*/

const httpAgent  = new HttpAgent({ keepAlive: true, maxSockets: CONCURRENCY });
const httpsAgent = new HttpsAgent({ keepAlive: true, maxSockets: CONCURRENCY });

const client = axios.create({
  httpAgent,
  httpsAgent,
  timeout: 15_000,
  headers: { "User-Agent": "Mozilla/5.0 (nftParser/3.4)" },
});

/*─────────────────── async-pool ──────────────*/

async function asyncPool<T, R>(
  limit: number,
  items: readonly T[],
  iterator: (item: T, index: number) => Promise<R>,
): Promise<R[]> {
  const ret: R[] = new Array(items.length);
  const executing = new Set<Promise<void>>();

  for (let i = 0; i < items.length; i++) {
    const task = (async (): Promise<void> => {
      ret[i] = await iterator(items[i], i);
    })();

    executing.add(task);
    task.finally(() => executing.delete(task));

    if (executing.size >= limit) await Promise.race(executing);
  }
  await Promise.all(executing);
  return ret;
}

/*────────────────── scrapeOne ───────────────*/

async function scrapeOne(slug: string): Promise<NftMeta | null> {
  const res = await client.get<string>(BASE_URL(slug), {
    validateStatus: (s): boolean => s < 500,
    responseType: "text",
  });
  if (res.status !== 200) return null;

  const $ = cheerio.load(res.data) as cheerio.CheerioAPI;
  const attrs: Record<string, string> = {};

  $(".tgme_gift_table tr").each((_, el): void => {
    const key = clean($(el).find("th").text());
    const raw = clean($(el).find("td").text());
    const val = raw.replace(/^[\u115F\u200B\s]+|[\u115F\u200B\s]+$/g, ""); // режем пустышки
    if (key && val) attrs[key] = val;
  });

  return {
    slug,
    title:       pickTitle($),
    image:       pickImage($),
    description: pickDescription($),
    attrs,
    syncedAt:    new Date().toISOString(),
  };
}

/*────────────────── persistence ─────────────*/

function chunkFileName(idx: number): string {
  const base = idx === 0 ? "nft-metadata.json" : `nft-metadata-${idx}.json`;
  return path.join(DATA_DIR, base);
}

async function ensureDataDir(): Promise<void> {
  await fs.mkdir(DATA_DIR, { recursive: true }).catch(() => {/* ignore */});
}

async function listChunks(): Promise<string[]> {
  await ensureDataDir();
  const inDir  = await fs.readdir(DATA_DIR).catch(() => [] as string[]);
  const inRoot = await fs.readdir(".");
  const files = [...inDir.map(f => path.join(DATA_DIR, f)), ...inRoot]
    .filter(f => /(^|\/|^)nft-metadata(?:-\d+)?\.json$/.test(f));

  return files
    .sort((a, b) => {
      const fileName = (p: string): string => path.basename(p);
      const ai = fileName(a) === "nft-metadata.json" ? 0 : Number(fileName(a).match(/-(\d+)\.json$/)?.[1] ?? 0);
      const bi = fileName(b) === "nft-metadata.json" ? 0 : Number(fileName(b).match(/-(\d+)\.json$/)?.[1] ?? 0);
      return ai - bi;
    });
}

async function save(map: Map<string, NftMeta>): Promise<void> {
  await ensureDataDir();
  const all = Array.from(map.values());
  const chunks: string[] = [];

  for (let i = 0; i < all.length; i += CHUNK_SIZE) {
    const file = chunkFileName(chunks.length);
    const slice = all.slice(i, i + CHUNK_SIZE);
    await fs.writeFile(file, JSON.stringify(slice, null, 2));
    chunks.push(path.resolve(file));
  }

  // удалить лишние файлы (в data/ и в корне), оставшиеся от прошлых запусков
  const existing = await listChunks();
  for (const f of existing) {
    if (!chunks.includes(path.resolve(f))) {
      await fs.unlink(f).catch(() => {/* ignore */});
    }
  }

  console.log(`💾  saved ${map.size} records into ${chunks.length} chunks (${DATA_DIR}/)`);
}

/*────────────────── main ────────────────────*/

(async (): Promise<void> => {
  /* 1) load previous dump */
  let previous: NftMeta[] = [];
  try {
    const chunkFiles = await listChunks();
    for (const f of chunkFiles) {
      const data = JSON.parse(await fs.readFile(f, "utf8"));
      previous.push(...data);
    }
  } catch {/* first run or corrupt */}

  const map = new Map<string, NftMeta>(previous.map(e => [e.slug, e]));

  /* 2) resume indices */
  const maxIdx: Record<string, number> = {};
  for (const { slug } of previous) {
    const [base, idx] = slugParts(slug);
    maxIdx[base] = Math.max(maxIdx[base] ?? 0, idx);
  }

  /* 3) fetch id-map */
  const idMap = (await client.get<Record<string, string>>(ID_MAP_URL)).data;
  const bases = Object.values(idMap).map(b => b.replace(/\s+/g, ""));

  let added = 0;

  await asyncPool<string, void>(
    CONCURRENCY,
    bases,
    async (base): Promise<void> => {
      let i = (maxIdx[base] ?? 0) + 1;
      for (;;) {
        const slug = `${base}-${i}`;
        if (map.has(slug)) { i++; continue; }

        const meta = await scrapeOne(slug);
        if (!meta) break;

        map.set(slug, meta);
        added++;
        console.log(`+ ${slug}`);

        if (map.size % SAVE_EVERY === 0) await save(map);
        i++;
      }
    },
  );

  /* 4) light sync pass */
  if (added === 0 && map.size) {
    const oldest = Array.from(map.values())
      .sort(
        (a, b) =>
          (new Date(a.syncedAt ?? 0).getTime() || 0) -
          (new Date(b.syncedAt ?? 0).getTime() || 0),
      )
      .slice(0, SYNC_BATCH);

    await asyncPool<NftMeta, void>(
      CONCURRENCY,
      oldest,
      async (rec): Promise<void> => {
        const fresh = await scrapeOne(rec.slug);
        if (!fresh) return;

        const equal =
          JSON.stringify({ ...rec, syncedAt: undefined }) ===
          JSON.stringify({ ...fresh, syncedAt: undefined });

        map.set(
          rec.slug,
          equal
            ? { ...rec,   syncedAt: new Date().toISOString() }
            : { ...fresh, syncedAt: new Date().toISOString() },
        );
      },
    );
  }

  await save(map);
  console.log(`\nDone: +${added} new • total ${map.size} • concurrency ${CONCURRENCY}`);
})().catch(err => {
  console.error("fatal:", err);
  process.exit(1);
});
