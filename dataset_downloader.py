import os
import requests
import time
from duckduckgo_search import DDGS
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, DownloadColumn, TransferSpeedColumn, TimeRemainingColumn
from rich.table import Table
from rich.prompt import Prompt
from rich.panel import Panel

console = Console()

class DatasetDownloader:
    def __init__(self, topic, max_results=25):
        self.topic = topic
        self.max_results = max_results
        self.folder = os.path.join("downloads", topic.replace(" ", "_").lower())
        self.downloaded_files = []
        
        if not os.path.exists(self.folder):
            os.makedirs(self.folder)

    def is_direct_link(self, url):
        """Verifica si es un formato de datos conocido."""
        extensions = ('.csv', '.xlsx', '.xls', '.json', '.zip', '.parquet', '.tsv', '.ods')
        return any(url.lower().endswith(ext) for ext in extensions)

    def clean_filename(self, url):
        path = urlparse(url).path
        filename = os.path.basename(path)
        filename = filename.split('?')[0]
        if not filename or '.' not in filename:
            # Generar nombre basado en el dominio y la query si no hay extensión
            domain = urlparse(url).netloc.replace(".", "_")
            filename = f"data_{domain}_{int(time.time())}.csv"
        return filename

    def search_datasets(self):
        """Busca links en DuckDuckGo con varios dorks."""
        queries = [
            f'"{self.topic}" dataset download filetype:csv',
            f'"{self.topic}" csv data download',
            f'"{self.topic}" xlsx database',
            f'index of "{self.topic}" .csv'
        ]
        
        links = set()
        
        with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), transient=True) as progress:
            task = progress.add_task(f"[cyan]Buscando '{self.topic}' en la red...", total=len(queries))
            
            with DDGS() as ddgs:
                for query in queries:
                    try:
                        results = ddgs.text(query, max_results=10)
                        if results:
                            for r in results:
                                links.add(r['href'])
                    except Exception:
                        pass
                    progress.advance(task)
                    time.sleep(0.5)
        
        return list(links)

    def extract_links_from_page(self, url):
        """Escanea una página en busca de botones de descarga o links directos."""
        if self.is_direct_link(url):
            return [url]
        
        # Filtros de seguridad y ruido
        blacklist = ['facebook.com', 'twitter.com', 'linkedin.com', 'google.com', 'youtube.com', 'amazon.com']
        if any(b in url for b in blacklist):
            return []

        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
            response = requests.get(url, timeout=5, headers=headers)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                found = []
                # Buscar en todos los enlaces
                for a in soup.find_all('a', href=True):
                    href = a['href']
                    full_url = urljoin(url, href)
                    if self.is_direct_link(full_url):
                        found.append(full_url)
                return found
        except:
            pass
        return []

    def download(self, url, progress):
        """Gestor de descarga con validación de tipo."""
        filename = self.clean_filename(url)
        filepath = os.path.join(self.folder, filename)
        
        if os.path.exists(filepath):
            return False

        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            # Stream=True para no cargar todo en RAM de golpe
            with requests.get(url, stream=True, timeout=12, headers=headers) as r:
                r.raise_for_status()
                
                # Ignorar si es un HTML camuflado
                if 'text/html' in r.headers.get('Content-Type', ''):
                    return False
                
                total_size = int(r.headers.get('content-length', 0))
                task = progress.add_task(f"[white]↓ {filename[:30]}...", total=total_size)
                
                with open(filepath, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=16384):
                        if chunk:
                            f.write(chunk)
                            progress.update(task, advance=len(chunk))
                
                self.downloaded_files.append((filename, url))
                return True
        except:
            return False

    def start(self):
        console.print(Panel(f"[bold green]Iniciando descarga masiva para:[/bold green] [bold yellow]{self.topic}[/bold yellow]"))
        
        initial_links = self.search_datasets()
        if not initial_links:
            console.print("[bold red]No se encontró nada. Intenta con un tema más general.[/bold red]")
            return

        # Fase de expansión: Buscar links directos dentro de los resultados
        direct_download_urls = set()
        with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), transient=True) as progress:
            task = progress.add_task("[yellow]Extrayendo links de descarga...", total=len(initial_links))
            for link in initial_links:
                found = self.extract_links_from_page(link)
                for f in found:
                    direct_download_urls.add(f)
                progress.advance(task)

        if not direct_download_urls:
            console.print("[yellow]Se encontraron páginas interesantes, pero ninguna permite descarga directa automática sin login.[/yellow]")
            console.print("[cyan]Links sugeridos para revisión manual:[/cyan]")
            for l in list(initial_links)[:5]:
                console.print(f" - {l}")
            return

        # Fase de descarga
        console.print(f"[bold blue]Datasets detectados:[/bold blue] [bold white]{len(direct_download_urls)}[/bold white]")
        
        with Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(bar_width=None),
            DownloadColumn(),
            TransferSpeedColumn(),
            TimeRemainingColumn(),
        ) as progress:
            for url in list(direct_download_urls)[:self.max_results]:
                self.download(url, progress)

        self.final_report()

    def final_report(self):
        if not self.downloaded_files:
            console.print("\n[bold red]Terminado: 0 archivos descargados (los sitios bloquearon el acceso).[/bold red]")
            return

        table = Table(title="📦 Datasets Listos", border_style="bright_blue")
        table.add_column("Archivo", style="cyan")
        table.add_column("Servidor", style="dim")

        for name, url in self.downloaded_files:
            domain = urlparse(url).netloc
            table.add_row(name, domain)

        console.print("\n")
        console.print(table)
        console.print(f"\n[bold green]✅ ¡Proceso completado![/bold green]")
        console.print(f"Los archivos están en: [underline yellow]{os.path.abspath(self.folder)}[/underline yellow]")

if __name__ == "__main__":
    console.clear()
    console.print(Panel("[bold magenta]DATASET HUNTER PRO v1.1[/bold magenta]\n[dim]Modo de descarga múltiple activado[/dim]", expand=False))
    
    console.print("[cyan]Puedes introducir uno o varios temas separados por comas (ej: Bitcoin, Clima Madrid, NBA stats)[/cyan]")
    entrada = Prompt.ask("[bold yellow]¿Qué temas quieres buscar hoy?[/bold yellow]", default="World Cup data")
    
    # Dividir la entrada por comas y limpiar espacios
    temas = [t.strip() for t in entrada.split(",") if t.strip()]
    
    with Progress(SpinnerColumn(), TextColumn("[bold cyan]Procesando lista de temas..."), transient=True) as p:
        for tema in temas:
            hunter = DatasetDownloader(tema)
            hunter.start()
            console.print("\n" + "─" * 50 + "\n")
    
    console.print("[bold green]🏆 ¡Todas las búsquedas completadas![/bold green]")
