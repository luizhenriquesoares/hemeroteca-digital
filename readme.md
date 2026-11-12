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

### Leitura visão-primeiro (benchmark)
```bash
# Conta tokens de entrada das páginas douradas sem chamar o modelo (grátis)
python3 main.py benchmark-visao --golden --dry-run

# Uma página, duas configurações (gera custo de API)
python3 main.py benchmark-visao --page 029033_01_00001 --config sonnet-full --config opus-structure

# Todas as configurações nas páginas douradas, extrapolando para 150k páginas
python3 main.py benchmark-visao --golden --extrapolar 150000

# Pela assinatura do Claude Code (claude -p headless), sem créditos de API
python3 main.py benchmark-visao --page 029033_01_00001 --config sonnet-full --backend claude-code
```
Com `--backend claude-code` o custo por página é uma estimativa "API equivalente" (tiles pela fórmula de pixels + saída real);
o custo informado pelo Claude Code inclui o overhead do harness e fica em `cost_usd_reported`.
Configurações: `opus-full`, `opus-structure`, `sonnet-full`, `sonnet-structure`, `haiku-full`, `haiku-structure`.
Páginas douradas em `src/vision/golden.json`; as sem imagem local precisam de `ocr-hires --keep-images`.
Resultados em `data/benchmarks/visao/<timestamp>/` (`summary.md`, `summary.json`, um JSON por página x config).

### Variáveis de ambiente
- `ANTHROPIC_API_KEY`: leitura visão-primeiro e correção via API. Sem ela, o leitor usa o perfil de `ant auth login` (CLI da Anthropic: `brew install anthropics/tap/ant`). Nos dois casos a organização precisa ter créditos de API no Console.
- `OPENAI_API_KEY`: RAG e correção via OpenAI.
- `HEMEROTECA_UPLOAD_SECRET`: habilita os endpoints temporários de upload (`/api/_upload_chunk`); sem valor ficam desligados.

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

### Pré-processamento CLAHE e escolha por consenso (2026-09-10)

O pré-processamento padrão passou de binarização global para **CLAHE** em escala
de cinza, sem filtro de mediana (`method="clahe"` em `preprocessar_imagem`; o
caminho antigo continua em `method="binarize"`). A escolha entre as variantes
deixou de ser o maior `score_ocr_text` e passou a ser a leitura que mais
**concorda** com as outras (`select_best_by_consensus`).

Medido na capa de 1825 em resolução cheia (6491×8940), contra a leitura do Opus
verificada nos pixels, 22 âncoras checadas:

| caminho | F1 | âncoras | tempo |
|---|---|---|---|
| seleção antiga por score (`singlecol`) | 0,447 | 8/22 | 32 s |
| melhor variante antiga (`fullpage`) | 0,502 | 10/22 | 32 s |
| **CLAHE + consenso** | **0,512** | **13/22** | **22 s** |

`score_ocr_text` premiava **caracteres por linha** (peso 0,14) e saturava a
contagem de palavras em 120 (peso 0,18). Os dois termos favoreciam a variante
que lê um jornal de duas colunas como uma coluna só — a pior segmentação — e
85% do score era constante entre variantes. O termo de densidade foi removido e
a cobertura passou a crescer com raiz quadrada, sem teto.

Não melhoraram nesta página, em resolução cheia: Otsu, binarização adaptativa
(Sauvola), normalização por divisão do fundo, filtro de mediana antes do CLAHE e
corte nas calhas detectadas em vez de larguras iguais. A precisão ficou entre
0,42 e 0,48 em todas as variantes — metade do que o Tesseract escreve não está na
referência, e nenhum ajuste de pixel mudou isso. Uma página só, de tipos grandes:
vale confirmar em página densa antes de reprocessar o corpus.

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
│   ├── vision/
│   │   ├── schema.py              # PageExtraction: segmentos, pessoas, relações
│   │   ├── tiles.py               # Recorte da página em tiles legíveis
│   │   ├── reader.py              # Leitura via Claude com saída estruturada
│   │   ├── benchmark.py           # Benchmark configs x páginas (tokens, custo, qualidade)
│   │   └── golden.json            # Páginas douradas do benchmark
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

### Recortes por layout (experimental)

`--tiling layout` detecta espaços e linhas separadoras em uma cópia reduzida da página,
separa cabeçalhos e colunas e dimensiona os recortes pela altura estimada dos caracteres.
Envia também uma visão geral de até 1000 px, marcada como orientação, sem transcrição.
O original não é binarizado, apagado, rotacionado nem cortado nas margens: a detecção
serve apenas para escolher caixas, com sobreposição. Se não encontrar separações,
subdivide a região pelo tamanho; a detecção não garante o layout correto.

Prévia totalmente local, sem API nem Claude Code:

```bash
.venv/bin/python main.py benchmark-visao --page 029033_01_00001 --tiling layout --preview
```

A prévia grava `layout.jpg`, os JPEGs exatos enviados ao modelo e `manifest.json`
com caixas no original, resolução e tokens estimados por pixels. A visão geral entra
na soma. Requer OpenCV (`opencv-python-headless`, incluído em `requirements.txt`).

Comparação controlada pela assinatura (executar separadamente e usar diretórios distintos):

```bash
.venv/bin/python main.py benchmark-visao --page 029033_01_00001 --backend claude-code --config opus-full --effort low --tiling grid --out data/benchmarks/visao/control-grid
.venv/bin/python main.py benchmark-visao --page 029033_01_00001 --backend claude-code --config opus-full --effort low --tiling layout --out data/benchmarks/visao/control-layout
```

A grade fixa continua sendo o padrão. `--grid` só vale para `--tiling grid`.
`--effort` permite comparar `full` e `structure` no mesmo nível; não se aplica ao Haiku.
O JSON do benchmark registra estratégia, esforço e geometria dos recortes. Os arquivos
usados pelo Claude Code ficam isolados por chamada para impedir que execuções simultâneas
sobrescrevam imagens. Revise nomes, omissões e duplicações antes de escolher a estratégia.

`--golden` lê os IDs do manifesto; não captura imagens. A captura por ID existe
desde 2026-09-18 (`ocr-hires --pages <ids>` ou `--golden --keep-images`); a
validação nas páginas densas segue pendente, agora só por revisão humana.


### Captura por ID e TLS verificado (2026-09-18)

`ocr-hires --pages 029033_01_09202,029033_04_00002` captura só as páginas pedidas;
`--golden` usa o manifesto `src/vision/golden.json`. O DocReader é aberto uma vez
por acervo e a navegação pula direto para cada página via `hPagFis` — a varredura
sequencial é inviável para páginas espalhadas por acervos de milhares de páginas.

As 20 páginas douradas estão em `data/images/`: 19 novas, em 5 acervos, todas com
CAPTCHA resolvido automaticamente. As densas de 1860 chegam a 43 mil caracteres de
OCR, contra ~2 mil da capa de 1825.

A verificação TLS voltou a ficar **ligada**. O servidor da BN envia apenas o
certificado folha, sem o intermediário `Certum DV TLS G2 R39 CA`; o macOS valida
pelo chaveiro, o Python não, e o projeto contornava com `ssl.CERT_NONE` — o que
deixava aberto a interceptação o download das imagens, por onde viaja o cookie de
sessão. `src/scraping/tls.py` monta um bundle com o intermediário, buscado na URL
de `CA Issuers` do próprio certificado, e o guarda em `data/cache/ca_bundle.pem`.


### Avaliação offline por trechos

`python -m src.vision.evaluation --reference <referencia.json> --result <resultado.json> --out <pasta>`
compara transcrições já salvas, sem modelo ou rede. `--result` é repetível. A referência contém
`status` e `excerpts` com `id`, `page` e `text`. O alinhamento busca o melhor trecho dentro
da transcrição e registra o texto encontrado, erros por caractere (CER), por palavra (WER)
e repetições exatas. Normaliza apenas NFC e espaços, preservando caixa, acentos e grafia.
Resultados de `structure` e leituras com erro são excluídos da comparação.

Este teste mede só os trechos escolhidos: não prova completude, segmentação ou precisão
biográfica da página. Referências transcritas por assistente precisam de revisão humana.

### Mesa de pesquisa local

A rota `/` apresenta a nova mesa de pesquisa; a interface anterior permanece em
`/avancado`. Execute `.venv/bin/python -m uvicorn web:app --host 127.0.0.1 --port 8765`
e abra `http://127.0.0.1:8765`.

A mesa reúne busca por entidades ou palavras, filtros de ano, grafo por camadas,
registros de evidência e leitor com imagem/transcrição e busca dentro da página.
A consulta textual usa `modo=literal`: todas as palavras, ignorando caixa e acentos,
com anos filtrados antes do limite. Não faz expansão de grafias históricas. O índice
SQLite FTS5 em `data/cache/text_search.sqlite` é derivado dos chunks locais (ou textos
quando não há chunks) e é reconstruído quando os arquivos mudam. Não usa modelos.
Páginas sem ano ficam fora das consultas com período. A transcrição mantém os erros
do OCR existente; imagens ausentes são indicadas no leitor.

Os vínculos repetidos são agrupados apenas na apresentação, preservando todos os
IDs de extração. O painel permite selecionar cada registro e consultar sua evidência
e revisão. Coocorrência não é apresentada como parentesco. Os dados originais não
são fundidos nem corrigidos automaticamente. Testes locais: `pytest tests -q`;
os scripts experimentais de captura dependem de navegador e rede.

### Arquivo vivo: cinema, caderno e confronto

A entrada agora oferece caminhos temáticos e uma viagem documental pelo nome
Botelho. Qualquer busca textual ou entidade selecionada também pode virar uma
sequência no **Cinema do Acervo**. Cenas são páginas distintas, ordenadas pelo ano
registrado na busca; páginas sem ano ficam ao final. Dentro do mesmo ano a ordem
é por acervo/página, não por uma data exata inferida. A sequência é uma amostra dos
resultados retornados, não uma cronologia completa ou biografia validada.

O cinema permite reproduzir, pausar, avançar, escolher uma cena e mudar o ritmo.
O movimento só começa por ação do usuário; a reprodução pausa ao abrir uma fonte
ou ocultar a aba. `prefers-reduced-motion` desliga os efeitos visuais. Quando não
existe imagem local, a cena usa o OCR com a marca “não é fac-símile”.

No **Caderno de investigação**, salve páginas ou selecione um trecho da transcrição
para guardá-lo. Escreva notas, exporte em Markdown com referências, percorra suas
fontes como filme ou selecione duas para **confrontar lado a lado**. O caderno usa
`localStorage` (até 100 fontes), fica neste navegador e não sincroniza entre
computadores. Exportar mantém uma cópia portátil. Retirar uma fonte oferece Desfazer.
A seleção de outro trecho da mesma página atualiza seu trecho, preservando as notas.

Validação da lógica do front: `node --test tests/test_experience.mjs`. Os módulos
`experience.js`, `experience-model.mjs` e `experience.css` separam esta experiência
da navegação de pesquisa existente. Não há novas dependências nem chamadas a LLM.

### Laboratório de descobertas

Seis instrumentos ficam em **Vá além da busca**, na entrada da aplicação:

- **Pergunta:** reúne até 18 fontes por até três consultas textuais; aceita termos explícitos separados por ponto e vírgula e extrai termos entre aspas da pergunta. Salva até dez investigações neste navegador, com hipótese editável e exportação. Não gera uma resposta histórica sem evidências.
- **Detetive de identidades:** compara dois ou três registros por grafias, anos e contextos extraídos (até 40 páginas e 40 relações por registro). Cada contexto abre sua evidência. Coocorrência não entra como atributo de identidade. Não funde registros.
- **Encontros:** até duas ligações entre entidades, com orientação e evidência de cada relação, excluindo revisões rejeitadas e relações sem trecho. Limites: 250 relações por ponta, 12 caminhos, oito páginas de coocorrência apresentadas separadamente. Um resultado vazio não prova ausência de vínculo.
- **Atlas:** pesquisa lugares no OCR, percorre os anos da amostra (até 50 resultados), mostra fontes sem data separadamente e permite anotar até 20 posições modernas aproximadas, com exportação GeoJSON. Não geocodifica automaticamente endereços históricos. A posição inicial é uma referência geral do Recife, baseada na [tabela do IBGE](https://www.ibge.gov.br/biblioteca/visualizacao/periodicos/2/bn_2022_v30_art02.pdf); o mapa atual usa tiles OpenStreetMap apenas para a área visível, com cache normal do navegador e [atribuição/política de uso](https://operations.osmfoundation.org/policies/tiles/). O mapa requer internet; as fontes são locais.
- **Próxima pista:** parte da última página salva, encontra registros compartilhados com outras fontes e explica o motivo de cada sugestão. Mostra até seis pistas, filtrando ruído e repetição de nomes; não conclui que homônimos são uma só pessoa.
- **Documentário:** roteiro de até 12 cenas, legendas editáveis, reordenação, duração de 2 a 30 segundos por cena, prévia e vídeo local de 1280×720. Usa imagem disponível ou OCR identificado; exporta roteiro com referências. Áudio opcional vem de arquivo anexado (até 30 MB), não de síntese automática. O roteiro persiste neste navegador, mas o arquivo de áudio precisa ser anexado novamente após recarregar. A exportação ocorre em tempo real com a aba visível, usando o formato suportado pelo navegador (WebM/MP4). Ocultar a aba ou fechar o estúdio interrompe a gravação.

As consultas são feitas em `/api/lab/*` sobre a base local. Nenhum desses instrumentos
consome Claude ou API de LLM. Dados do caderno, investigações, atlas e roteiro ficam
no navegador; não há sincronização entre computadores. Exporte cópias para preservar
as descobertas. Imagens e extrações incompletas continuam limitando o que pode ser
concluído historicamente.

### OCR local experimental sem créditos

`python main.py ocr-local --image data/images/029033_01/029033_01_00001.jpg --scale 0.5 --out data/experimentos/ocr-local`

Preserva o original e salva texto bruto + metadados em um diretório separado.
Não sobrescreve resultados existentes. Imagens menores que 5000 pixels no lado
maior não são reduzidas. Em cinco trechos provisórios da capa de 1825, com o mesmo
Tesseract e tessdata do projeto, escala 0.5 reduziu CER de 30,6% para 12,5%.
Isso não valida completude da página, entidades ou qualidade em outras edições.
A captura hi-res agora identifica a versão instalada do Chrome em vez de exigir 146.

#### Revisão em múltiplas escalas

Acrescente `--review` ao `ocr-local` para gerar três leituras próximas da escala
escolhida, JSON com palavras/confiança/coordenadas no original e HTML autônomo
com alternativas e recortes embutidos. O texto principal permanece intacto;
inserções, omissões e substituições são sinalizadas para conferência. Concordância
não prova acerto, e a confiança do Tesseract não é uma probabilidade calibrada.
Imagens pequenas mantêm escala 1 e recebem somente uma leitura.

Experimento na capa de 1825: Sauvola, Otsu adaptativo, compensação de iluminação
e correção global de inclinação não melhoraram os trechos provisórios; não foram
ativados no comando. Escalas 0.4, 0.45, 0.55 e 0.6 também perderam para 0.5.
O modo de revisão melhora a auditabilidade; não há redução adicional de CER
comprovada sobre a configuração 0.5. Referência técnica:
https://tesseract-ocr.github.io/tessdoc/ImproveQuality.html

#### Correção automática experimental baseada em releituras

`python main.py ocr-local --image data/images/029033_01/029033_01_00001.jpg --correct --out data/experimentos/correcao`

O modo `--correct` segmenta a página em linhas e volta aos pixels originais.
Cada linha recebe releituras com alturas de 40, 60 e 80 pixels, sem ampliar
linhas pequenas. A saída combina caracteres alinhados por maioria estrita,
com empate favorável ao original. Imagens idênticas não recebem votos repetidos.
Fragmentos curtos e alterações em tokens numéricos são mantidos para revisão.
A correção guarda `.raw.txt`, `.txt`, JSON com hipóteses, coordenadas, hashes do
modelo/imagem e HTML autônomo com recortes de cada alteração.

A técnica combina ideias de reconhecimento por linhas e votação; não é uma
implementação dos modelos Calamari ou dos ensembles neurais da AAAI, nem uma
invenção científica comprovadamente inédita. Os votos vêm do mesmo motor e são
correlacionados. Não há treinamento ou acesso ao gabarito durante a correção.
O resultado ainda exige validação humana e não recupera linhas omitidas pela
segmentação inicial. `--correct` já inclui HTML e não se combina com `--review`.

#### Refinamento experimental v2

Acrescente `--refine` a `ocr-local --correct` para uma segunda votação, usando
PSM 13 (linha sem os ajustes usuais de segmentação) e PSM 7 com fechamento
morfológico em cinza 2×2. Ambas as vistas usam altura máxima de 60 pixels.
O consenso v1 é preservado no JSON (`consensus_v1`) junto às novas hipóteses.
A proteção de números e fragmentos curtos também vale nesta etapa.

Na mesma capa, CER passou de 8,71% para 8,48% no grupo A e de 10,24% para 7,23%
no grupo B. WER regrediu de 33,33% para 34,67% em A e melhorou de 34,48% para
27,59% em B. Ambos os grupos agora são dados de desenvolvimento reutilizados;
não representam validação externa. O modo v1 permanece padrão. Não é uma LLM
nem fine-tuning: são hipóteses visuais adicionais com o mesmo Tesseract.
