# 🚀 Dataset Hunter Pro v3.3 - The Ultimate Scouter

[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Maintenance](https://img.shields.io/badge/Maintained%3F-yes-green.svg)](https://github.com/)

**Dataset Hunter Pro v3.0** is the most powerful version yet. No more manual searching on Kaggle or Google. Just enter a topic, and it will fetch datasets from over 20 professional sources and the open web simultaneously.

---

## 🇺🇸 English Version

*   **⚡ Optimized Concurrency**: Handles 20+ parallel downloads and 3 simultaneous topics (Turbo Mode).
*   **🛡️ Content Guard**: Advanced header analysis to filter out HTML errors, 404s, and "hidden" login walls.
*   **💾 Smart Hybrid Strategy**: Downloads everything, but automatically prefixes files over 100MB with `LARGE_` and ignores them in Git. This keeps your GitHub repository clean while having all data locally.

### 🛠️ Technology Stack
- **Async Engine:** `aiohttp` & `aiofiles`
- **Search Engine:** `duckduckgo-search` (multi-query optimized)
- **Scraping:** `BeautifulSoup4`
- **UI:** `Rich` (Full professional dashboard)

### 📥 Installation
```bash
pip install -r requirements.txt
```

### 🚀 Usage
```bash
python dataset_downloader.py
```
> Enter multiple topics separated by commas: `Inflation Data, NBA Player Stats, COVID-19 Genomics`

---

## 🇪🇸 Versión en Español

*   **⚡ Modo Turbo**: Procesa 3 temas a la vez y hasta 20 descargas simultâneas.
*   **🛡️ Estrategia Híbrida**: Los archivos > 100MB se marcan como `LARGE_` y se ignoran en Git automáticamente. Descarga total sin romper tu repo.
*   **📊 Validación Pro**: Detecta errores HTML disfrazados de datasets y los elimina automáticamente.

### 📋 Estructura de Archivos
- `dataset_downloader.py`: Motor Turbo v3.3.
- `.metadata/`: Carpeta con caché, cola de estado y hashes (organizado).
- `downloads/`: Los datos se guardan aquí. Los archivos pesados tienen el prefijo `LARGE_`.

---

### ⚠️ Disclaimer
Educational use only. Respect `robots.txt` and the terms of service of each data provider.

Uso educativo. Respeta siempre el `robots.txt` y los términos de servicio de los proveedores de datos.
