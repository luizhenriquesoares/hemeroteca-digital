# Estimativa de Custos — Hemeroteca Digital de Pernambuco

## Visão geral do projeto

Plataforma de pesquisa historiográfica que digitaliza, transcreve, indexa e torna pesquisável o acervo completo de jornais históricos de Pernambuco disponíveis na Biblioteca Nacional (1825 a 1990), totalizando **1.125.402 páginas** de **303 periódicos**.

A plataforma oferece:
- Busca semântica assistida por inteligência artificial
- Extração automática de pessoas, cargos, relações familiares, processos judiciais e eventos
- Grafos de relações entre pessoas, instituições e lugares
- Fichas historiográficas com evidências documentais linkadas à página original do jornal

---

## Escopo do acervo

| Dado | Valor |
|------|-------|
| Periódicos | 303 |
| Páginas totais | 1.125.402 |
| Período | 1825 a 1990 |
| Fonte | Biblioteca Nacional — Hemeroteca Digital Brasileira |

### Principais periódicos

| Periódico | Páginas | Período |
|-----------|---------|---------|
| Diario de Pernambuco | ~600.000 | 1825-1989 |
| Jornal do Recife | 124.859 | 1858-1938 |
| Jornal Pequeno | 92.165 | 1898-1955 |
| A Provincia | 71.614 | 1872-1933 |
| Diário da Manhã | 65.241 | 1930-1979 |

---

## Fases do projeto

### Fase 1 — Setup (meses 1 a 4)

Captura, transcrição e indexação do acervo completo.

#### 1.1 Captura de páginas (scraping hi-res)

O sistema acessa a Hemeroteca Digital da BN via navegador automatizado, configura a resolução máxima do visualizador DocReader (6.464 × 8.940 pixels), captura cada página como imagem JPG e extrai o texto via OCR.

| Item | Detalhe |
|------|---------|
| Páginas a capturar | 1.117.402 (restantes) |
| Resolução | 6.464 × 8.940 px por página |
| Velocidade | ~200 páginas/hora por worker |
| Workers paralelos | 2 (limitação do site da BN) |
| Tempo estimado | ~116 dias corridos |
| Custo direto | $0 (usa infraestrutura do projeto) |

#### 1.2 OCR e transcrição via GPT-4o Vision

Cada página capturada é enviada como imagem diretamente ao **GPT-4o Vision** (OpenAI), que realiza num único passo:

1. **Transcrição completa** — leitura da imagem com separação correta de colunas
2. **Correção de texto** — interpretação de caligrafia e artefatos gráficos
3. **Extração estruturada** — identificação automática de pessoas, cargos, relações e eventos

Esta abordagem substitui o OCR convencional (Tesseract), que não consegue separar colunas em jornais do séc. XIX e produz texto ilegível. O GPT-4o Vision lê diretamente a imagem, entende o layout visual e extrai informação estruturada com precisão comprovada em testes (caso Jacintho Affonso Botelho: único modelo que identificou corretamente 3 sentenças judiciais com réu, vítima e resultado em página com 4 colunas misturadas).

| Item | Detalhe |
|------|---------|
| Páginas a processar | 1.125.402 |
| Modelo | GPT-4o Vision (OpenAI) |
| Custo por página | $0,01 |
| **Custo total OCR + Extração** | **$11.254** |
| Resultado esperado | ~4.500.000 entidades, ~1.125.000 relações biográficas |

**Detalhamento do custo GPT-4o Vision (medido em PoC real):**
- Modelo: `gpt-4o` com input de imagem (detail: high)
- Input: ~1.230 tokens por página (fixo, independente da resolução da imagem)
- Output: ~780 tokens/página (texto transcrito + JSON estruturado)
- Preço input: $2,50 / 1M tokens
- Preço output: $10,00 / 1M tokens
- Custo médio por página: **$0,01** (medido em PoC: $0,03 para 3 chamadas = $0,01/chamada)
- Saldo OpenAI antes do teste: $17,31 → depois: $17,28 (gasto real confirmado)

**Por que GPT-4o Vision em vez de OCR convencional:**
- Jornais históricos têm 3-4 colunas que o Tesseract mistura
- Caligrafia, manchas e degradação do papel confundem OCR tradicional
- GPT-4o Vision entende o layout visual e separa colunas corretamente
- Extração de entidades e relações no mesmo passo (sem pipeline intermediário)
- Qualidade comprovada: em testes comparativos, foi o único modelo (vs Claude Opus, Claude Sonnet, GPT-4o-mini) que extraiu corretamente nomes, cargos e relações judiciais de páginas com OCR degradado

#### 1.3 Indexação vetorial

O texto transcrito é dividido em chunks de ~1.000 caracteres e indexado por embeddings semânticos para permitir busca por significado (não apenas por palavra exata).

| Item | Detalhe |
|------|---------|
| Modelo de embedding | OpenAI `text-embedding-3-small` (1.536 dimensões) |
| Chunks estimados | ~3.376.000 (3 chunks/página média) |
| Tokens estimados | ~1 bilhão |
| Custo | ~$20 (one-time) |
| Banco vetorial | ChromaDB (persistente em disco) |
| Re-indexação | Mesmo custo (~$20) quando necessário |

---

### Fase 2 — Operação (contínua)

Plataforma em produção, acessível a pesquisadores.

#### 2.1 Infraestrutura AWS

| Recurso | Especificação | Custo mensal |
|---------|--------------|-------------|
| **EC2 t3.xlarge** | 4 vCPU, 16GB RAM — servidor da aplicação (API + frontend + ChromaDB) | $120 |
| **EC2 t3.medium** | 2 vCPU, 4GB RAM — scraping e processamento OCR batch | $30 |
| **EBS gp3 500GB** | Armazenamento SSD para banco de dados, chunks e índice vetorial | $50 |
| **S3 Standard 2TB** | Backup das imagens hi-res capturadas (~2TB total estimado) | $46 |
| **Transferência** | ~500GB/mês de tráfego de saída | $45 |
| **Route53** | Domínio + DNS + SSL | $5 |
| **TOTAL MENSAL** | | **$296/mês** |
| **TOTAL ANUAL** | | **$3.552/ano** |

**Justificativa da EC2 t3.xlarge (16GB):**
- ChromaDB com 3.3M chunks ocupa ~4GB em memória
- Modelo de embedding é chamado via API (não roda local)
- API FastAPI + SQLite servindo múltiplos usuários simultâneos
- Margem para picos de uso

**Justificativa da EC2 t3.medium:**
- Usado apenas durante a Fase 1 (captura + OCR)
- Pode ser desligada após conclusão da captura
- Economia: $30/mês × 4 meses = $120 total

#### 2.2 Pesquisas com IA (RAG)

Cada pesquisa na plataforma combina busca textual histórica + busca semântica, e opcionalmente gera uma resposta sintetizada por IA com fontes documentais.

| Item | Detalhe |
|------|---------|
| Modelo | GPT-4o (OpenAI) |
| Custo por pesquisa | ~$0,01 |
| Pesquisas incluídas no orçamento | **6.000/mês** (200/dia) |
| Custo mensal | **$60/mês** |
| Custo anual | **$720/ano** |

**O que cada pesquisa consome:**
- Embedding da query: ~100 tokens ($0,000002)
- Geração da resposta: ~2.000 tokens input + ~1.000 output ($0,01)
- Total: ~$0,01 por pesquisa

**Nota:** Pesquisas que não usam o modo IA (busca textual ou semântica pura) custam apenas o embedding da query (~$0,000002), praticamente zero.

---

## Resumo financeiro

### Ano 1 — Setup + Operação

| Categoria | Item | Custo USD | Custo BRL |
|-----------|------|-----------|-----------|
| **Setup** | OCR + Extração via GPT-4o Vision (1.125.402 páginas) | $11.254 | R$ 61.897 |
| **Setup** | Indexação vetorial (embedding) | $20 | R$ 110 |
| **Setup** | EC2 t3.medium scraping (4 meses) | $120 | R$ 660 |
| **Operação** | AWS infraestrutura (12 meses) | $3.552 | R$ 19.536 |
| **Operação** | Pesquisas RAG (72.000 pesquisas) | $720 | R$ 3.960 |
| | **TOTAL ANO 1** | **$15.666** | **R$ 86.163** |

### Anos seguintes — Operação

| Categoria | Item | Custo USD | Custo BRL |
|-----------|------|-----------|-----------|
| **Operação** | AWS infraestrutura | $3.552 | R$ 19.536 |
| **Operação** | Pesquisas RAG (72.000 pesquisas) | $720 | R$ 3.960 |
| | **TOTAL ANUAL** | **$4.272** | **R$ 23.496** |

---

## Entregas

### O que a plataforma oferece ao pesquisador

1. **Busca por nome, cargo, lugar ou pergunta livre** em 1.125.402 páginas de jornais
2. **Resposta sintetizada por IA** com fontes verificáveis (periódico, página, ano)
3. **Fichas de pessoas** com variantes nominais, cargos, relações familiares e evidências
4. **Grafos de relações** (quem era casado com quem, escravo de quem, réu em qual processo)
5. **Timeline temporal** com picos de menções e eventos documentais
6. **Link direto** para a página original do jornal na Biblioteca Nacional
7. **Navegação por sobrenome** para pesquisa genealógica
8. **Trilhas temáticas** (Governo, Comércio, Igreja, Geografia)

### Números de referência (produção atual com 8.000 páginas)

| Métrica | Valor atual | Projeção acervo completo |
|---------|------------|-------------------------|
| Páginas processadas | 8.000 | 1.125.402 |
| Entidades extraídas | 152.000 | ~21.000.000 |
| Relações biográficas | 2.460 | ~1.125.000 |
| Chunks indexados | 50.000 | ~3.400.000 |
| Tipos de relação | 38 | 38+ |

### Tipos de relação extraídos automaticamente

- Filiação (filho de, filha de)
- Matrimônio (casado com, viúva de)
- Cargos (nomeado para, eleito)
- Judicial (réu, absolvido, condenado, vítima)
- Escravidão (escravo de, liberto, fugido, alforria)
- Residência (morador em)
- Comércio (sócio de, credor, devedor)
- Viagem (embarcou, chegou, partiu)
- Óbito (faleceu)
- Assinatura (assinado por)
- Militar (desertor, recruta)

---

## Câmbio utilizado

Todas as conversões BRL usam a taxa de R$ 5,50 por USD, sujeita a variação.

## Observações

- Os custos de API (OpenAI) são baseados na tabela de preços vigente em abril de 2026
- O scraping respeita os termos de uso da Biblioteca Nacional e aplica delays entre requisições
- Todo o conteúdo processado mantém referência à fonte original (periódico, acervo, página)
- O código é open-source e pode ser auditado
