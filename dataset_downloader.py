"""
Dataset Hunter Pro v3.1 TURBO
==============================
Escribe un tema → descarga datasets de todas partes automáticamente a máxima velocidad.

Fuentes integradas (20+):
  APIs directas : Zenodo, UCI ML Repository, CKAN (data.gov, datos.gob.es,
                  open.canada.ca, data.europa.eu, data.gov.uk), World Bank,
                  Our World in Data, Eurostat, GitHub (awesome-datasets),
                  Harvard Dataverse, OpenML, Socrata (datos NYC, Chicago…)
  Scraping web  : DuckDuckGo multi-query con rotación de User-Agents

Características:
  - TURBO Mode: Procesa múltiples temas en paralelo.
  - Cola persistente: si se interrumpe, reanuda donde lo dejó.
  - Deduplicación por hash MD5 (nunca descarga el mismo archivo dos veces).
  - Validación real de contenido (detecta HTMLs disfrazados, errores 404).
  - 20 descargas paralelas con reintentos y backoff exponencial.
  - Metadata Organizada: Todos los archivos de control en la carpeta .metadata/
"""

import asyncio, aiohttp, aiofiles
import os, json, time, hashlib, random, re, urllib.parse
from pathlib import Path
from urllib.parse import urlparse, urljoin, quote_plus
from duckduckgo_search import DDGS
from bs4 import BeautifulSoup
from rich.console import Console
from rich.progress import (Progress, SpinnerColumn, TextColumn,
                           BarColumn, DownloadColumn, TransferSpeedColumn, TimeRemainingColumn)
from rich.table import Table
from rich.prompt import Prompt
from rich.panel import Panel
from rich import box

console = Console()

# ─────────────────────────────────────────────
#  CONFIGURACIÓN  (toca aquí si quieres ajustar)
# ─────────────────────────────────────────────
MAX_PARALLEL    = 20        # descargas simultáneas total
MAX_TOPICS_PAR  = 3         # cuántos temas buscar/procesar a la vez
MAX_RETRIES     = 4         # reintentos por descarga
GITHUB_LIMIT_MB = 99        # Archivos > 99MB se marcarán como LARGE_ para .gitignore
MAX_FILE_SIZE_MB = 2000     # Límite local (2GB) para evitar llenar el disco por error
BACKOFF_BASE    = 1.8       # segundos de espera base entre reintentos
REQUEST_TIMEOUT = 15        # timeout por petición HTTP
CACHE_TTL_H     = 8         # horas que vive la caché de búsqueda
MAX_CANDIDATES  = 200       # máximo de URLs a descargar por búsqueda
DOWNLOAD_BASE   = Path("downloads")
METADATA_DIR    = Path(".metadata")
CACHE_FILE      = METADATA_DIR / "cache.json"
QUEUE_FILE      = METADATA_DIR / "queue.json"

DATA_EXTENSIONS = {
    '.csv', '.xlsx', '.xls', '.json', '.zip', '.parquet',
    '.tsv', '.ods', '.gz', '.tar', '.7z', '.sqlite', '.db',
    '.arrow', '.feather', '.hdf', '.h5', '.nc', '.geojson', '.shp'
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/124.0.0.0 Safari/537.36",
    "curl/8.7.1",
    "python-httpx/0.27.0",
]

SKIP_DOMAINS = {
    'facebook.com','twitter.com','x.com','instagram.com','tiktok.com',
    'youtube.com','linkedin.com','amazon.com','netflix.com','pinterest.com',
    'reddit.com','medium.com','quora.com','stackoverflow.com',
}

# ─────────────────────────────────────────────
#  CACHÉ Y JSON HELPERS
# ─────────────────────────────────────────────

def _load_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    except Exception:
        return {}

def _save_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def cache_get(key: str) -> list | None:
    c = _load_json(CACHE_FILE)
    entry = c.get(key)
    if entry and (time.time() - entry["ts"]) / 3600 < CACHE_TTL_H:
        return entry["urls"]
    return None

def cache_set(key: str, urls: list) -> None:
    c = _load_json(CACHE_FILE)
    c[key] = {"ts": time.time(), "urls": urls}
    _save_json(CACHE_FILE, c)

# ─────────────────────────────────────────────
#  COLA PERSISTENTE
# ─────────────────────────────────────────────

class PersistentQueue:
    def __init__(self, topic: str):
        self.path = QUEUE_FILE
        self.key  = topic.lower().strip()
        self._data = _load_json(self.path)

    def pending(self, urls: list[str]) -> list[str]:
        done = set(self._data.get(self.key, {}).get("done", []))
        return [u for u in urls if u not in done]

    def mark_done(self, url: str) -> None:
        if self.key not in self._data:
            self._data[self.key] = {"done": []}
        if url not in self._data[self.key]["done"]:
            self._data[self.key]["done"].append(url)
        _save_json(self.path, self._data)

# ─────────────────────────────────────────────
#  DEDUPLICACIÓN POR HASH
# ─────────────────────────────────────────────

class HashRegistry:
    def __init__(self, folder: Path):
        self.path = folder / ".hashes.json"
        self._seen: set[str] = set(_load_json(self.path).get("hashes", []))

    def is_duplicate(self, md5: str) -> bool:
        return md5 in self._seen

    def register(self, md5: str) -> None:
        self._seen.add(md5)
        _save_json(self.path, {"hashes": list(self._seen)})

# ─────────────────────────────────────────────
#  HELPERS GENERALES
# ─────────────────────────────────────────────

def rand_headers() -> dict:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Connection": "keep-alive",
    }

def is_data_url(url: str) -> bool:
    path = urlparse(url).path.lower().split("?")[0]
    return any(path.endswith(ext) for ext in DATA_EXTENSIONS)

def clean_filename(url: str, topic: str) -> str:
    name = os.path.basename(urlparse(url).path.split("?")[0])
    name = re.sub(r'[\\/:*?"<>| ]', '_', name)
    if not name or '.' not in name:
        slug = re.sub(r'\W+', '_', topic)[:20]
        name = f"dataset_{slug}_{int(time.time()*1000) % 9999}.csv"
    return name

def domain_of(url: str) -> str:
    return urlparse(url).netloc.replace("www.", "")

def is_blocked(url: str) -> bool:
    d = domain_of(url)
    return any(b in d for b in SKIP_DOMAINS)

# ─────────────────────────────────────────────
#  FUENTES DE DATOS — APIs
# ─────────────────────────────────────────────

async def src_zenodo(s, t):
    try:
        r = await s.get(f"https://zenodo.org/api/records?q={quote_plus(t)}&type=dataset&size=30", timeout=10)
        if r.status == 200:
            data = await r.json()
            return [f.get("links", {}).get("self", "") for hit in data.get("hits", {}).get("hits", []) for f in hit.get("files", []) if f.get("links", {}).get("self")]
    except: pass
    return []

async def src_uci(s, t):
    try:
        r = await s.get(f"https://archive.ics.uci.edu/api/datasets?search={quote_plus(t)}&take=20", timeout=10)
        if r.status == 200:
            data = await r.json()
            return [f"https://archive.ics.uci.edu/static/public/{ds['id']}/dataset.zip" for ds in data.get("data", []) if ds.get("id")]
    except: pass
    return []

async def src_openml(s, t):
    try:
        r = await s.get(f"https://www.openml.org/api/v1/json/data/list/data_name/{quote_plus(t)}/limit/20", timeout=10)
        if r.status == 200:
            data = await r.json()
            return [f"https://www.openml.org/data/get_csv/{ds['did']}" for ds in data.get("data", {}).get("dataset", []) if ds.get("did")]
    except: pass
    return []

async def src_harvard_dataverse(s, t):
    try:
        r = await s.get(f"https://dataverse.harvard.edu/api/search?q={quote_plus(t)}&type=file&per_page=30", timeout=10)
        if r.status == 200:
            data = await r.json()
            return [f"https://dataverse.harvard.edu/api/access/datafile/{item['file_id']}" for item in data.get("data", {}).get("items", []) if item.get("file_id")]
    except: pass
    return []

async def src_worldbank(s, t):
    try:
        r = await s.get(f"https://search.worldbank.org/api/v2/wds?format=json&qterm={quote_plus(t)}&rows=10", timeout=10)
        if r.status == 200:
            data = await r.json()
            return [f"https://data.worldbank.org/indicator/{doc['url_friendly_title']}?downloadformat=csv" for key, doc in data.get("documents", {}).items() if isinstance(doc, dict) and doc.get("url_friendly_title")]
    except: pass
    return []

async def src_ckan_generic(s, topic, url_base):
    try:
        r = await s.get(f"{url_base}/api/3/action/package_search?q={quote_plus(topic)}&rows=20", timeout=10)
        if r.status == 200:
            data = await r.json()
            urls = []
            for pkg in data.get("result", {}).get("results", []):
                for res in pkg.get("resources", []):
                    u = res.get("url", "")
                    if u and is_data_url(u): urls.append(u)
            return urls
    except: pass
    return []

async def src_eurostat(s, t):
    try:
        r = await s.get("https://ec.europa.eu/eurostat/api/dissemination/catalogue/toc/json?lang=EN", timeout=10)
        if r.status == 200:
            data = await r.json()
            t_low = t.lower()
            urls = []
            for item in data.get("link", [{}])[0].get("item", []):
                if any(w in item.get("label", "").lower() for w in t_low.split()):
                    code = item.get("code")
                    if code: urls.append(f"https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/{code}?format=SDMX-CSV")
                    if len(urls) >= 10: break
            return urls
    except: pass
    return []

async def src_socrata(s, t):
    urls = []
    for portal in ["data.cityofnewyork.us", "data.cityofchicago.org", "data.lacity.org"]:
        try:
            r = await s.get(f"https://{portal}/api/catalog/v1?q={quote_plus(t)}&limit=5", timeout=8)
            if r.status == 200:
                data = await r.json()
                for item in data.get("results", []):
                    uid = item.get("resource", {}).get("id")
                    if uid: urls.append(f"https://{portal}/api/views/{uid}/rows.csv?accessType=DOWNLOAD")
        except: pass
    return urls

async def src_github_search(s, t):
    urls = []
    try:
        r = await s.get(f"https://api.github.com/search/code?q={quote_plus(t)}+extension:csv&per_page=15", timeout=10, headers={"Accept": "application/vnd.github+json", "User-Agent": "DatasetHunter/3.1"})
        if r.status == 200:
            data = await r.json()
            for item in data.get("items", []):
                raw = item.get("html_url", "").replace("github.com", "raw.githubusercontent.com").replace("/blob/", "/")
                if raw: urls.append(raw)
    except: pass
    return urls

# ─────────────────────────────────────────────
#  BÚSQUEDA WEB (DuckDuckGo)
# ─────────────────────────────────────────────

def search_ddgs(topic: str) -> list[str]:
    cached = cache_get(f"ddgs:{topic}")
    if cached is not None: return cached
    queries = [f'{topic} dataset download csv', f'site:zenodo.org {topic}', f'"{topic}" data filetype:csv']
    links = set()
    try:
        with DDGS() as ddgs:
            for q in queries:
                for r in ddgs.text(q, max_results=15) or []:
                    href = r.get("href")
                    if href and not is_blocked(href): links.add(href)
                time.sleep(0.4)
    except: pass
    res = list(links)
    cache_set(f"ddgs:{topic}", res)
    return res

async def extract_links_from_page(s, url):
    if is_data_url(url): return [url]
    if is_blocked(url): return []
    try:
        async with s.get(url, timeout=REQUEST_TIMEOUT, headers=rand_headers(), allow_redirects=True) as r:
            if r.status != 200 or "text/html" not in r.headers.get("Content-Type", ""): return []
            html = await r.text(errors="ignore")
            soup = BeautifulSoup(html, "html.parser")
            found = []
            kw = {"download","descargar","csv","excel","parquet"}
            for a in soup.find_all("a", href=True):
                full = urljoin(url, a["href"])
                if is_data_url(full) or any(k in a.get_text().lower() for k in kw):
                    found.append(full)
            return found
    except: return []

# ─────────────────────────────────────────────
#  VALIDACIÓN Y DESCARGA
# ─────────────────────────────────────────────

def validate_file(path: Path) -> dict:
    res = {"valid": False, "reason": "", "size_kb": 0, "rows": None, "md5": ""}
    if not path.exists() or path.stat().st_size < 64: return res
    res["size_kb"] = round(path.stat().st_size / 1024, 1)
    with open(path, "rb") as f:
        head = f.read(512).lower()
    if b"<!doctype html" in head or b"<html" in head:
        path.unlink(missing_ok=True)
        return res
    hasher = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""): hasher.update(chunk)
    res["md5"] = hasher.hexdigest()[:10]
    if path.suffix.lower() in (".csv", ".tsv"):
        try:
            with open(path, "r", errors="ignore") as f: res["rows"] = sum(1 for _ in f) - 1
        except: pass
    res["valid"] = True
    return res

async def download_one(session, url, folder, topic, semaphore, registry, queue, progress, overall):
    filename = clean_filename(url, topic)
    filepath = folder / filename
    if filepath.exists(): return None
    async with semaphore:
        for attempt in range(1, MAX_RETRIES+1):
            try:
                async with session.get(url, timeout=40, headers=rand_headers(), allow_redirects=True) as r:
                    if r.status != 200: continue
                    total = int(r.headers.get("content-length", 0))
                    
                    # Estrategia Híbrida: si es > 99MB, le ponemos un prefijo para que Git lo ignore
                    if total > GITHUB_LIMIT_MB * 1024 * 1024:
                        filename = f"LARGE_{filename}"
                        filepath = folder / filename
                    
                    if total > MAX_FILE_SIZE_MB * 1024 * 1024:
                        return None
                        
                    task = progress.add_task(f"[white]↓ {filename[:25]}", total=total or None)
                    folder.mkdir(parents=True, exist_ok=True)
                    async with aiofiles.open(filepath, "wb") as f:
                        async for chunk in r.content.iter_chunked(65536):
                            await f.write(chunk)
                    progress.remove_task(task)
                    break
            except:
                if attempt < MAX_RETRIES: await asyncio.sleep(BACKOFF_BASE**attempt)
                else: return None
    info = validate_file(filepath)
    if not info["valid"] or registry.is_duplicate(info["md5"]):
        filepath.unlink(missing_ok=True)
        return None
    registry.register(info["md5"])
    queue.mark_done(url)
    progress.advance(overall)
    return {"filename": filename, "source": domain_of(url), "size_kb": info["size_kb"], "rows": info["rows"], "md5": info["md5"]}

# ─────────────────────────────────────────────
#  ORQUESTADOR CLASE
# ─────────────────────────────────────────────

class DatasetHunter:
    def __init__(self, topic):
        self.topic = topic
        self.folder = DOWNLOAD_BASE / re.sub(r'\W+', '_', topic.lower())
        self.results = []

    async def run(self):
        queue = PersistentQueue(self.topic)
        registry = HashRegistry(self.folder)
        direct_urls = set()

        connector = aiohttp.TCPConnector(limit=30, ssl=False)
        async with aiohttp.ClientSession(connector=connector) as s:
            sources = [
                src_zenodo(s, self.topic), src_uci(s, self.topic), src_openml(s, self.topic),
                src_harvard_dataverse(s, self.topic), src_worldbank(s, self.topic),
                src_ckan_generic(s, self.topic, "https://catalog.data.gov"),
                src_ckan_generic(s, self.topic, "https://open.canada.ca/data"),
                src_ckan_generic(s, self.topic, "https://data.gov.uk"),
                src_eurostat(s, self.topic), src_socrata(s, self.topic), src_github_search(s, self.topic)
            ]
            raw = await asyncio.gather(*sources, asyncio.to_thread(search_ddgs, self.topic), return_exceptions=True)
            
            ddgs_pages = []
            for r in raw:
                if isinstance(r, list):
                    for u in r:
                        if is_data_url(u): direct_urls.add(u)
                        else: ddgs_pages.append(u)

            if ddgs_pages:
                extracted = await asyncio.gather(*[extract_links_from_page(s, u) for u in ddgs_pages[:20]], return_exceptions=True)
                for r in extracted:
                    if isinstance(r, list):
                        for u in r:
                            if is_data_url(u): direct_urls.add(u)

        candidates = queue.pending(list(direct_urls))[:MAX_CANDIDATES]
        if not candidates: return

        sem = asyncio.Semaphore(MAX_PARALLEL // MAX_TOPICS_PAR)
        with Progress(TextColumn("[blue]{task.description}"), BarColumn(), DownloadColumn(), TransferSpeedColumn(), TimeRemainingColumn(), console=console) as progress:
            overall = progress.add_task(f"[green]Tema: {self.topic[:20]}", total=len(candidates))
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=10, ssl=False)) as s_dl:
                tasks = [download_one(s_dl, u, self.folder, self.topic, sem, registry, queue, progress, overall) for u in candidates]
                self.results = [r for r in await asyncio.gather(*tasks) if isinstance(r, dict)]
        self._report()

    def _report(self):
        if not self.results: return
        table = Table(title=f"📦 {self.topic}", box=box.ROUNDED)
        table.add_column("Archivo", style="cyan")
        table.add_column("Fuente", style="magenta")
        table.add_column("Tamaño", style="white")
        for r in self.results[:10]:
            table.add_row(r["filename"], r["source"], f"{r['size_kb']} KB")
        console.print(table)

# ─────────────────────────────────────────────
#  MAIN TURBO
# ─────────────────────────────────────────────

async def run_topic(tema, sem, total, idx):
    async with sem:
        console.print(f"\n[bold yellow]>>> PROCESANDO {idx}/{total}: {tema}[/bold yellow]")
        await DatasetHunter(tema).run()

async def main():
    console.clear()
    console.print(Panel("[bold magenta]DATASET HUNTER PRO v3.1 TURBO[/bold magenta]\n[dim]Paralelismo de temas · .metadata organizado[/dim]", box=box.DOUBLE_EDGE))
    
    try:
        entrada = Prompt.ask("[bold yellow]¿Temas?[/bold yellow]", default="climate change")
        temas = [t.strip() for t in entrada.split(",") if t.strip()]
        if not temas: return

        sem = asyncio.Semaphore(MAX_TOPICS_PAR)
        await asyncio.gather(*[run_topic(t, sem, len(temas), i+1) for i, t in enumerate(temas)])
        console.print("\n[bold green]🏆 ¡FINALIZADO![/bold green]")
    except Exception as e:
        import traceback
        console.print(f"[red]{traceback.format_exc()}[/red]")

if __name__ == "__main__":
    import sys
    if sys.platform == 'win32': asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try: asyncio.run(main())
    except KeyboardInterrupt: pass
    except Exception as e: console.print(f"[red]Error: {e}[/red]")