# Hemeroteca Digital PE

Pipeline completo para captura, OCR, correção via LLM e busca RAG de jornais históricos de Pernambuco da [Hemeroteca Digital Brasileira](https://memoria.bn.gov.br/hdb/) (Biblioteca Nacional).

## Pipeline

```
Hemeroteca Digital (memoria.bn.gov.br)
        │
        ▼
  1. CAPTURA HI-RES (Selenium + undetected-chromedriver)
  Baixa imagens em resolução máxima (6464x8940) via DocReader
        │
        ▼
  2. OCR (Tesseract + detecção de colunas)
  Segmentação por colunas + PSM 6, skip header dinâmico
        │
        ▼
  3. CORREÇÃO LLM (Claude Opus 4.6)
  Reconstrói frases, preserva ortografia histórica
        │
        ▼
  4. CHUNKING (LangChain)
  Divide textos corrigidos em chunks de 1000 chars
        │
        ▼
  5. INDEXAÇÃO (ChromaDB)
  Embeddings multilíngue + busca semântica
        │
        ▼
  6. RAG (GPT-4o-mini + streaming SSE)
  Frontend web com busca inteligente
```

## Pré-requisitos

- Python 3.11+
- Google Chrome 146+
- Claude Code CLI (opcional, para correção LLM)
- OpenAI API key (opcional, para RAG frontend)

## Instalação

```bash
# Ambiente virtual
python3 -m venv .venv
source .venv/bin/activate

# Dependências
python3 -m pip install -r requirements.txt

# Modelos Tesseract (por, eng) - baixar em data/tessdata/
# Disponíveis em https://github.com/tesseract-ocr/tessdata
```

## Comandos principais

### Listagem e captura
```bash
# Listar acervos de PE disponíveis
python3 main.py listar

# OCR hi-res (captura + OCR + delete imagem em um passo)
python3 main.py ocr-hires --bib 029033_02 --workers 4

# Teste em amostra (2 páginas, mantém imagens para validação)
python3 main.py ocr-hires --bib 029033_02 --max-pages 2 --keep-images
```

### Correção LLM
```bash
# Corrigir textos com OpenAI
python3 main.py corrigir --provider openai --model gpt-4o-mini --workers 2

# Corrigir textos com Claude Code CLI
python3 main.py corrigir --provider claude --model opus --workers 2

# Ou Sonnet/Haiku
python3 main.py corrigir --provider claude --model sonnet --workers 3

# Benchmark da mesma página entre OpenAI mini, OpenAI max e Claude
python3 main.py benchmark-correcao data/text/029033_01/029033_01_00066.txt --sample-chars 2000
```

### Indexação RAG
```bash
# Chunking (usa _corrigido.txt automaticamente se existir)
python3 main.py chunkar

# Indexar no ChromaDB
python3 main.py indexar

# Busca semântica CLI
python3 main.py buscar "joão affonso botelho"
```

### Frontend web
```bash
# Inicia servidor FastAPI com RAG streaming
python3 -m uvicorn src.web.api:app --host 0.0.0.0 --port 8000

# Abrir http://localhost:8000
```

## Resolução de captura

| Nível | Resolução | Uso |
|---|---|---|
| Low-res (antigo) | 548x915 | Thumbnails do DocReader |
| **Hi-res (atual)** | **6140x8940** | Captura via HiddenSize postback |

## Qualidade OCR (benchmark Diário de Pernambuco 1840)

| Método | % palavras reais |
|---|---|
| Low-res + PSM 3 | 51% |
| Low-res + 2 colunas | 51% |
| Hi-res + PSM 3 | 64% |
| **Hi-res + colunas + skip header** | **58-67%** |
| **Hi-res + Claude Opus 4.6** | **68-73%** (publicação) |

## Estrutura

```
hemeroteca-digital/
├── main.py                        # Entrypoint fino do CLI
├── web.py                         # Compat wrapper para a API web
├── src/
│   ├── cli/
│   │   └── app.py                 # CLI principal (click)
│   ├── web/
│   │   └── api.py                 # FastAPI oficial: busca + RAG + SSE
│   ├── scraping/
│   │   ├── driver.py              # undetected-chromedriver / Cloudflare bypass
│   │   ├── acervos.py             # Listagem de acervos PE
│   │   ├── scraper.py             # Captura legacy + CAPTCHA
│   │   ├── hires_pipeline.py      # Pipeline hi-res
│   │   └── parallel.py            # Orquestração paralela
│   ├── processing/
│   │   ├── ocr.py                 # OCR Tesseract + seleção conservadora
│   │   ├── metadata_enrichment.py # Enriquecimento de metadados sem refazer OCR
│   │   ├── correcao_provider.py   # Switch OpenAI/Claude para correção
│   │   ├── llm_correcao.py        # Correção via OpenAI API
│   │   ├── llm_correcao_claude.py # Correção via Claude Code CLI
│   │   ├── chunker.py             # Split de textos (LangChain)
│   │   ├── indexer.py             # ChromaDB + embeddings
│   │   └── search.py              # Busca híbrida e evidências
│   ├── structured/
│   │   ├── models.py              # Modelos de domínio estruturado
│   │   ├── entities.py            # Extração de entidades
│   │   ├── relations.py           # Extração de relações
│   │   ├── repository.py          # Persistência SQLite
│   │   └── service.py             # Orquestração da camada estruturada
│   ├── benchmarks/
│   │   ├── correcao.py            # Benchmark OpenAI mini/max vs Claude
│   │   └── ocr.py                 # Benchmark OCR salvo vs OCR adaptativo
│   ├── config.py                  # Paths, URLs, constantes
│   └── __init__.py                # Aliases lazy para compatibilidade legada
├── frontend/
│   └── index.html                 # UI web com ficha historiográfica
├── scripts/
│   └── experimental/              # Scripts e testes exploratórios isolados
├── data/                          # (gitignored)
│   ├── images/                    # Imagens temporárias (hi-res)
│   ├── text/                      # OCR bruto + _corrigido.txt
│   ├── chunks/                    # JSONL por acervo
│   ├── chromadb/                  # Índice vetorial persistente
│   ├── structured/                # SQLite e artefatos estruturados
│   └── tessdata/                  # Modelos Tesseract (por, eng)
└── logs/                          # (gitignored)
```

## Recursos técnicos

- **Bypass Cloudflare**: `undetected-chromedriver` + Chrome visível (passa challenge JS)
- **CAPTCHA auto-solve**: OCR via Tesseract na imagem do challenge (fallback manual)
- **Hi-res pipeline**: Seta `HiddenSize=6464x8940` no DocReader via JS postback
- **Detecção de colunas**: Header dinâmico via projeção de pixels escuros + 4 colunas uniformes no body
- **Correção histórica**: Prompt preserva ortografia da época (Commandante, Escripturario, etc)
- **Retomada**: Cache de progresso por acervo em `data/cache/hires_progress.json`

## Observações

- Os IPs de datacenter (VPS) são bloqueados pelo Cloudflare da BN
- IPs residenciais funcionam com `undetected-chromedriver` em modo visível
- O Chrome do usuário precisa estar em versão compatível com a ChromeDriver baixada
- A Hemeroteca Digital **não tem API pública** — captura é via Selenium
