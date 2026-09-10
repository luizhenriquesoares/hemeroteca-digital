# Plano de Integração Genealógica — Açores + Pernambuco

## Visão

Cruzar registros paroquiais dos Açores (batismos, casamentos, óbitos) com os jornais de Pernambuco para construir linhas genealógicas completas — do nascimento nos Açores até a vida documentada em Recife.

**Exemplo**: Jacinto Soares Botelho nasceu em Capelas (Açores), migrou para Recife, aparece nos jornais do Diário de Pernambuco.

## Fontes disponíveis

### Açores — Biblioteca Digital dos Açores
- URL: `culturacores.azores.gov.pt/biblioteca_digital/`
- Formato: viewer HTML com JPGs de alta resolução
- Padrão de URL: `SMG-PD-{PARISH}-{TYPE}-{YEARS}_master/.../_{PAGE}.jpg`
- Tipos: B (Batismos), C (Casamentos), O (Óbitos)
- Cobertura: séc. XVIII-XIX, todas as ilhas

### Pernambuco — Hemeroteca Digital BN (já implementado)
- 6.241 páginas, 35K chunks, 1.688 relações biográficas
- 54 child_of, 14 widow_of, 1.569 holds_role

## Arquitetura

```
Açores (parish records)          Pernambuco (jornais)
    ┌─────────────┐              ┌─────────────┐
    │  Scraping    │              │  Scraping    │ ← já implementado
    │  (JPG)       │              │  (hi-res)    │
    └──────┬──────┘              └──────┬──────┘
           │                            │
    ┌──────▼──────┐              ┌──────▼──────┐
    │  OCR        │              │  OCR         │ ← já implementado
    │  (gpt-4o)   │              │  (Tesseract) │
    └──────┬──────┘              └──────┬──────┘
           │                            │
    ┌──────▼──────┐              ┌──────▼──────┐
    │  Extração   │              │  Extração    │ ← já implementado
    │  Estruturada│              │  Estruturada │
    │  (registros)│              │  (entidades) │
    └──────┬──────┘              └──────┬──────┘
           │                            │
           └──────────┬─────────────────┘
                      │
               ┌──────▼──────┐
               │  Matching   │
               │  & Linking  │
               └──────┬──────┘
                      │
               ┌──────▼──────┐
               │  Árvore     │
               │  Genealógica│
               └─────────────┘
```

## Fase 1: Scraping dos Açores

### 1.1 Descoberta de acervos
- Listar todas as paróquias disponíveis na biblioteca digital
- Classificar por tipo (B/C/O) e período
- Priorizar: São Miguel (maioria dos emigrantes para PE)

### 1.2 Download das imagens
```python
# Padrão de URL já identificado:
# https://culturacores.azores.gov.pt/biblioteca_digital/
#   {COLLECTION}/{COLLECTION}_master/{COLLECTION}_JPG/{COLLECTION}_{PAGE}.jpg
```

Implementação:
- Novo módulo: `src/scraping/acores_scraper.py`
- Download sequencial (respeitar rate limits)
- Salvar em `data/acores/{parish}/{type}/{page}.jpg`
- Progresso resumível

## Fase 2: OCR dos registros paroquiais

### 2.1 OCR com GPT-4o (melhor para manuscrito)
Registros paroquiais são **manuscritos** — Tesseract não funciona bem. GPT-4o Vision é a melhor opção:

```python
# Enviar imagem + prompt específico para registros paroquiais
prompt = """
Transcreva este registro paroquial português do século XVIII/XIX.
Extraia os seguintes campos:
- Tipo: batismo / casamento / óbito
- Data do evento
- Nome do batizado/noivo/falecido
- Pai (nome completo)
- Mãe (nome completo)
- Padrinhos (se batismo)
- Noiva (se casamento)
- Local/freguesia
- Observações
Mantenha a ortografia original.
"""
```

### 2.2 Estruturação dos registros
Novo modelo:
```python
@dataclass
class ParishRecord:
    record_type: str      # "baptism", "marriage", "death"
    event_date: str       # data do evento
    person_name: str      # nome principal
    father_name: str      # pai
    mother_name: str      # mãe
    spouse_name: str      # cônjuge (casamento)
    godparents: list[str] # padrinhos
    parish: str           # freguesia
    island: str           # ilha
    source_page: str      # referência da página
    raw_text: str         # transcrição completa
```

## Fase 3: Banco de dados genealógico

### 3.1 Novas tabelas
```sql
CREATE TABLE parish_records (
    id INTEGER PRIMARY KEY,
    record_type TEXT NOT NULL,  -- baptism, marriage, death
    event_date TEXT,
    person_name TEXT NOT NULL,
    father_name TEXT,
    mother_name TEXT,
    spouse_name TEXT,
    godparents_json TEXT DEFAULT '[]',
    parish TEXT NOT NULL,
    island TEXT NOT NULL,
    source_collection TEXT,
    source_page TEXT,
    raw_text TEXT,
    confidence REAL DEFAULT 0.8,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE genealogy_links (
    id INTEGER PRIMARY KEY,
    parish_record_id INTEGER REFERENCES parish_records(id),
    entity_id INTEGER REFERENCES entities(id),
    link_type TEXT NOT NULL,  -- "same_person", "parent", "child", "spouse"
    confidence REAL DEFAULT 0.5,
    evidence TEXT,
    reviewer TEXT DEFAULT 'auto',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
```

### 3.2 Schema da árvore genealógica
```sql
CREATE TABLE family_tree (
    id INTEGER PRIMARY KEY,
    person_id INTEGER,           -- entity_id ou parish_record person
    person_name TEXT NOT NULL,
    birth_date TEXT,
    birth_place TEXT,
    death_date TEXT,
    death_place TEXT,
    father_id INTEGER REFERENCES family_tree(id),
    mother_id INTEGER REFERENCES family_tree(id),
    source_type TEXT,            -- "parish_record", "newspaper", "manual"
    source_ids_json TEXT,        -- IDs dos registros fonte
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
```

## Fase 4: Matching & Linking

### 4.1 Algoritmo de cruzamento
1. Normalizar nomes (Botelho ↔ Botelhos, Soares ↔ Soarez)
2. Comparar sobrenomes entre Açores e Pernambuco
3. Verificar plausibilidade temporal (nascido 1790 nos Açores → mencionado 1830 em PE = plausível)
4. Cruzar nomes de pais/padrinhos com outras menções

### 4.2 Scoring
```python
def match_score(parish_record, newspaper_entity):
    score = 0.0
    # Sobrenome idêntico
    if same_surname(parish_record.person_name, newspaper_entity.canonical_name):
        score += 0.4
    # Nome completo similar
    if name_similarity(parish_record.person_name, newspaper_entity.canonical_name) > 0.8:
        score += 0.3
    # Período plausível (nascimento + 20-60 anos = menção no jornal)
    if temporal_plausible(parish_record.event_date, newspaper_entity.first_year):
        score += 0.2
    # Pai/mãe mencionado nos jornais
    if parent_in_newspapers(parish_record.father_name, parish_record.mother_name):
        score += 0.1
    return score
```

## Fase 5: Frontend — Linha do tempo genealógica

### 5.1 Timeline visual
```
1790 ──── Batismo em Capelas, São Miguel (registro paroquial)
           Pais: José Soares Botelho e Maria da Conceição
           Padrinhos: Manuel de Sousa, Ana Pereira
1815 ──── Casamento com Teresa de Jesus (registro paroquial)
1820 ──── Primeira menção em Pernambuco (Diário de Pernambuco)
           "Jacinto Soares Botelho, morador no Recife"
1825 ──── Mencionado como comerciante (jornal)
1831 ──── Mencionado com cargo de Capitão (jornal)
1837 ──── Última menção documentada
```

### 5.2 Árvore visual expandida
```
                José Soares Botelho (Açores, ~1760)
                ├── casado com Maria da Conceição
                │
    ┌───────────┼───────────┐
    │           │           │
Jacinto      Manuel      Ana
(Açores→PE)  (Açores)    (Açores)
 ├── casado com Teresa de Jesus
 │
 ├── João Soares Botelho (PE, ~1820)
 └── Maria Soares Botelho (PE, ~1822)
```

## Estimativa de custo

| Etapa | Custo | Tempo |
|-------|-------|-------|
| Scraping Açores (1 paróquia, 149 páginas) | $0 | 1 sessão |
| OCR com GPT-4o Vision (149 páginas manuscritas) | ~$5-10 | 2h |
| Extração estruturada | $0 | 1 sessão |
| Matching & linking | $0 | 1 sessão |
| Frontend árvore genealógica | $0 | 1 sessão |

Para escalar para todas as paróquias de São Miguel (~50 paróquias × ~200 páginas):
- OCR: ~$500-1000
- Tempo: ~1 semana

## Prioridade de implementação

1. **Scraper dos Açores** — download das imagens
2. **OCR com GPT-4o Vision** — transcrição dos manuscritos
3. **Extração estruturada** — registros paroquiais → banco
4. **Matching** — cruzar com entidades de PE
5. **Frontend** — timeline + árvore visual
