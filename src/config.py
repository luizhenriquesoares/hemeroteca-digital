"""Configurações globais do projeto."""

import os
from pathlib import Path

# Diretórios
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
IMAGES_DIR = DATA_DIR / "images"
TEXT_DIR = DATA_DIR / "text"
CHUNKS_DIR = DATA_DIR / "chunks"
CACHE_DIR = DATA_DIR / "cache"
STRUCTURED_DIR = DATA_DIR / "structured"
LOGS_DIR = BASE_DIR / "logs"
CHROMA_DIR = DATA_DIR / "chromadb"

# Criar diretórios se não existirem
for d in [IMAGES_DIR, TEXT_DIR, CHUNKS_DIR, CACHE_DIR, STRUCTURED_DIR, LOGS_DIR, CHROMA_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# URLs da Hemeroteca Digital (redirecionou de memoria.bn.br para memoria.bn.gov.br)
HDB_BASE_URL = "https://memoria.bn.gov.br"
HDB_SEARCH_URL = f"{HDB_BASE_URL}/hdb/"
HDB_DOCREADER_URL = f"{HDB_BASE_URL}/docreader/docreader.aspx"

# Estado alvo (sigla como aparece no dropdown do site)
UF_ALVO = "PE"

# Períodos disponíveis na HDB (décadas)
PERIODOS_HDB = [
    "1740 - 1749", "1760 - 1769",
    "1800 - 1809", "1810 - 1819", "1820 - 1829", "1830 - 1839",
    "1840 - 1849", "1850 - 1859", "1860 - 1869", "1870 - 1879",
    "1880 - 1889", "1890 - 1899",
    "1900 - 1909", "1910 - 1919", "1920 - 1929", "1930 - 1939",
    "1940 - 1949", "1950 - 1959", "1960 - 1969", "1970 - 1979",
    "1980 - 1989", "1990 - 1999",
    "2000 - 2009", "2010 - 2019", "2020 - 2026",
]

# Selenium
CHROME_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)
PAGE_LOAD_TIMEOUT = 60
IMPLICIT_WAIT = 10
CLICK_PAUSE = 0.5  # segundos entre cliques
HUMAN_DELAY_MIN = 0.5  # delay mínimo
HUMAN_DELAY_MAX = 1.5  # delay máximo

# OCR
TESSDATA_DIR = DATA_DIR / "tessdata"
TESSDATA_DIR.mkdir(parents=True, exist_ok=True)
os.environ["TESSDATA_PREFIX"] = str(TESSDATA_DIR)
TESSERACT_LANG = "por"  # português
TESSERACT_PSM = 3  # automatic page segmentation

# RAG
CHUNK_SIZE = 1000  # caracteres por chunk
CHUNK_OVERLAP = 200  # sobreposição entre chunks
EMBEDDING_MODEL = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
CHROMA_COLLECTION = "hemeroteca_pe"
STRUCTURED_DB = STRUCTURED_DIR / "hemeroteca.db"
