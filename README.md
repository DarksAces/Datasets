# 🚀 Dataset Hunter Pro v3.9 - Stable God Mode

[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Status](https://img.shields.io/badge/Status-God%20Mode%20Active-magenta.svg)](https://github.com/)

**Dataset Hunter Pro v3.9** is the definitive evolution. This version is designed for massive data collection while maintaining a lightweight, compliant, and rock-solid GitHub repository.

---

## 🇺🇸 Key Features (God Mode)

*   **⚡ Ultra-Parallel Engine**: Processes **10 topics simultaneously** with up to **50-100 parallel download threads**.
*   **🔍 GitHub Deep Scraper**: Advanced search for `.csv`, `.json`, `.parquet`, and `.xlsx` files directly inside GitHub repositories.
*   **🛡️ Strict GitHub Shield**: Automatically skips any file over **99MB** to ensure 100% compatibility with GitHub syncing limits.
*   **🧠 Smart Metadata Management**: Self-cleaning `cache.json` and `queue.json` to prevent control files from exceeding GitHub size limits.
*   **🖥️ Full CLI Support**: Pass lists of hundreds of topics directly via command line to bypass Windows terminal input limits.
*   **💎 Hash Deduplication**: Uses MD5 fingerprinting to ensure you never download the same file twice, even if found through multiple sources.

### 📥 Installation
```bash
pip install -r requirements.txt
```

### 🚀 Usage
**Recommended (CLI Mode):**
```bash
python dataset_downloader.py "Topic 1, Topic 2, Topic 3..."
```

**Interactive Mode:**
```bash
python dataset_downloader.py
```

---

## 🇪🇸 Características Principales (Modo Dios)

*   **⚡ Motor Ultra-Paralelo**: Procesa **10 temas a la vez** y gestiona hasta **100 hilos** de red de forma estable.
*   **🔍 GitHub Deep Scraper**: Búsqueda avanzada de múltiples formatos (`.csv`, `.json`, `.parquet`, `.xlsx`) dentro de la propia infraestructura de GitHub.
*   **🛡️ Escudo Estricto GitHub**: Omite automáticamente archivos de **+99MB**. Calidad garantizada para un repositorio ágil y compartible.
*   **🧠 Gestión Inteligente**: Auto-limpieza de archivos de caché y metadatos para que tu repositorio nunca se bloquee por tamaño.
*   **🖥️ Soporte CLI Total**: Permite pasar listas masivas de temas como argumentos del script, evitando errores de buffer al pegar texto.

### 📋 Estructura de Archivos
- `dataset_downloader.py`: Motor v3.9 Stable.
- `.metadata/`: Control centralizado de caché, cola de progreso y hashes MD5.
- `downloads/`: Colección organizada por carpetas temáticas.

---

### ⚠️ Disclaimer / Aviso Legal
Educational use only. Respect `robots.txt` and the terms of service of each data provider. All downloaded data is automatically validated to ensure it is actual data and not HTML error pages.

Uso educativo únicamente. Respeta siempre el `robots.txt` y los términos de servicio. Todos los archivos son validados para asegurar que contienen datos reales y no páginas de error.
