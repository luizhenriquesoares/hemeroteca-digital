# Plano de Descoberta e Valor — Hemeroteca Digital PE

## Contexto

O projeto já tem um pipeline robusto (scraping → OCR → correção → chunking → indexação → extração estruturada → busca/RAG), mas o frontend depende do usuário já saber o que procurar. A base contém 111K entidades, 155K menções, 316K relações e 4K páginas de jornais de Pernambuco (1825–1840) — material riquíssimo que precisa ser **revelado**, não apenas consultado.

**Problema central**: a ferramenta é reativa. O usuário abre a página, vê um campo de busca vazio e não sabe por onde começar.

**Objetivo deste plano**: transformar a homepage de "campo de busca com radar" em uma **porta de entrada que convida à exploração**, aproveitando os dados já estruturados e expandindo a extração para gerar mais valor historiográfico.

---

## Diagnóstico da base atual

| Métrica | Valor | Implicação |
|---------|-------|------------|
| Entidades com ≥10 menções | 528 | Pool suficiente para descoberta rotativa |
| Relações biográficas (family) | 4 | Muito poucas — precisam de expansão |
| Relações holds_role | 1.202 | Boa base para perfis de cargo |
| Relações mentioned_with | ~314K | Rede de co-ocorrência densa, subaproveitada |
| Sobrenomes distintos | 42.478 | Busca por sobrenome viável |
| Páginas com >150 entidades | ~50 | Boas candidatas para "episódios" |
| Anos cobertos | 1825–1840 | Recorte concentrado, fácil de narrar |
| Entidades revisadas | 0 de 104K | Curadoria ainda não começou |
| Confiança média | ~0.62 | Maioria em faixa média — precisa curadoria |

---

## Fase A — Descoberta passiva na homepage

**Meta**: o usuário abre o frontend e imediatamente vê algo interessante.
**Esforço**: baixo. **Custo API**: zero. **Dependências**: nenhuma.

### A1. Personagem do dia

Mostrar um card rotativo na homepage com uma pessoa diferente por dia, escolhida entre as 528 entidades com ≥10 menções.

**Dados exibidos**:
- Nome canônico + variantes
- Cargo (se holds_role existir)
- Período de atividade (first_year – last_year)
- Contagem de menções e relações
- Um snippet real do jornal (a menção com maior confiança)
- Botão "ver perfil completo"

**Implementação backend**:

```
# Novo endpoint: GET /api/discovery/featured-entity
# Lógica: entity_id = entities_com_10plus_mencoes[day_of_year % len(pool)]
# Retorna: serialize_entity(entity) simplificado
```

- Arquivo: `src/web/api.py` — novo endpoint
- Query: `src/structured/repository.py` — novo método `get_featured_entity(seed: int)`
  - Buscar entidades tipo "person" com ≥10 menções, sem noise, com relações
  - Ordenar por (mentions DESC, relations DESC)
  - Selecionar por `seed % count`
  - Retornar entity + top snippet + relações resumidas
- Frontend: `frontend/index.html` — novo card acima do search box
  - Chamar no `init()`, renderizar como card com estilo hero
  - Clicar abre o dossier existente

**Critérios de qualidade para o pool**:
- `assess_discovery_noise()` retorna `is_probable_noise: false`
- Pelo menos 2 menções com confidence ≥ 0.6
- Pelo menos 1 relação que não seja `mentioned_with`
- Nome com ≥ 3 tokens (evitar nomes genéricos)

### A2. Perguntas-gatilho com dados reais

Substituir as research_questions genéricas por perguntas concretas que usam dados reais do banco.

**Formato atual** (genérico):
> "O que explica o pico de menções em 1837 neste recorte?"

**Formato proposto** (concreto e sedutor):
> "Quem era Pires Figueredo Camargo, citado 169 vezes como presidente da Província?"

**Implementação**:

- Arquivo: `src/structured/repository.py` — refatorar geração de `research_questions` no `get_discovery_overview()`
- Lógica:
  1. Para cada top person com holds_role → "Quem era {nome}, {cargo} citado {N} vezes entre {ano_ini} e {ano_fim}?"
  2. Para relações familiares → "Qual a relação entre {A} e {B} nos jornais de {período}?"
  3. Para instituições com muitas menções → "O que a {instituição} representava em {ano}?"
  4. Para picos temporais → "O que mudou em {ano} para gerar {N} menções — o dobro do ano anterior?"
  5. Para co-menções fortes → "{A} e {B} aparecem juntos em {N} páginas. Qual a conexão?"
- Cada pergunta é clicável e dispara busca RAG com o texto da pergunta

### A3. "Neste período" — contexto temporal automático

Card que mapeia a década atual (ou o período do filtro) para o recorte histórico.

**Exemplo**:
> **Pernambuco, 1831**
> 659 páginas processadas · 20.336 menções
> Figuras em destaque: Antonio Francisco Nunes (47 menções), Joze Rodrigues Pinheiro (47)
> Instituições ativas: Governo (312), Câmara (280), Alfândega (45)
> → Explorar este ano

**Implementação**:
- Reusar dados do `get_discovery_overview()` filtrado por ano
- Frontend: card entre o hero e o search box
- Seletor de ano clicável na timeline já existente — ao clicar, preenche o card

---

## Fase B — Caminhos de exploração guiada

**Meta**: dar ao usuário "trilhas" temáticas para entrar na base sem precisar formular query.
**Esforço**: médio. **Custo API**: zero. **Dependências**: Fase A parcialmente completa.

### B1. Trilhas temáticas

Cards de navegação na homepage, cada um levando a uma view filtrada.

| Trilha | Filtro aplicado | Dados disponíveis |
|--------|----------------|-------------------|
| **Famílias de Pernambuco** | Relações family (child_of, spouse_of, parent_of, widow_of) | 4 relações hoje — cresce com Fase D |
| **Poder e Governo** | Instituições "Governo", "Camara" + cargos | 1.861 + 1.495 menções, 1.202 holds_role |
| **Comércio e Porto** | Instituições "Alfandega", "Companhia" + embarcações | 267 + 363 menções |
| **Igreja e Clero** | Instituição "Igreja" + cargo "Padre", "Vigário" | 210 + 270 + 14 menções |
| **Geografia do poder** | Top places + resident_of | Olinda 360, Recife 161, Pernambuco 189 |

**Implementação backend**:

```
# Novo endpoint: GET /api/discovery/trail/{trail_name}
# trail_name: "familias" | "governo" | "comercio" | "igreja" | "geografia"
# Retorna: { entities[], relations[], pages[], summary }
```

- Arquivo: `src/structured/repository.py` — novo método `get_trail(trail_name, limit)`
  - Cada trail é um conjunto de queries predefinidas (filtro por predicado, tipo, instituição)
  - Retorna top entities + relações + hotspot pages para aquele tema
- Frontend: cards clicáveis na homepage, cada um renderiza uma mini-view com os dados da trail

### B2. Navegação por sobrenome

Campo separado na homepage: "Digite um sobrenome e descubra quem aparece nos jornais".

**Implementação**:

- Reusar `/api/entity/search?q={sobrenome}` já existente
- Frontend: input dedicado com autocomplete
  - Ao digitar, mostrar sugestões com contagem de menções
  - Ao selecionar, mostrar lista agrupada:
    - Todos os indivíduos com aquele sobrenome
    - Suas variantes (Botelho/Botelhos, Benedicto/Benedito)
    - Período de atividade
    - Cargos
    - Link para perfil completo
- Backend: novo método `search_by_surname(surname)` que faz `LIKE '%surname%'` no `normalized_name` e agrupa por base nominal

### B3. Timeline clicável

Tornar a timeline do Radar interativa: clicar em um ano abre um painel lateral com os dados daquele período.

**Implementação**:
- Frontend: adicionar handler de click nos bars do Pulso Temporal
- Ao clicar em um ano:
  - Preencher `filter-year-from` e `filter-year-to` com o mesmo ano
  - Disparar `loadDiscoveryOverview()` com filtro de ano
  - Scroll para a seção de resultados
- Sem mudança no backend — reusar endpoint existente

---

## Fase C — Narrativas automáticas

**Meta**: transformar dados brutos em texto legível sem esperar o usuário perguntar.
**Esforço**: médio. **Custo API**: baixo (LLM). **Dependências**: Fase A.

### C1. Resumo narrativo do acervo

Um parágrafo de abertura gerado por LLM que contextualiza o acervo inteiro.

**Exemplo**:
> O Diário de Pernambuco entre 1825 e 1840 documenta um período de intensa transformação política na província. Os registros revelam 528 figuras com presença significativa, com destaque para autoridades do Governo provincial e da Câmara. Capitães, doutores e padres dominam os cargos mencionados. O pico de atividade editorial em 1837, com mais de 42 mil menções, sugere um momento de efervescência política ou editorial que merece investigação.

**Implementação**:

- Arquivo: `src/web/rag_service.py` — nova função `generate_corpus_summary(overview_data)`
- Lógica:
  1. Chamar `get_discovery_overview()` sem filtros
  2. Montar prompt com os dados agregados (timeline, top people, institutions, roles)
  3. Chamar LLM uma vez (gpt-4o-mini, ~500 tokens)
  4. Cachear resultado em `data/cache/corpus_summary.json` por 24h
- Endpoint: `GET /api/discovery/summary` — retorna texto cacheado
- Frontend: renderizar como primeiro parágrafo da seção de descoberta

### C2. Biografias automáticas para top entidades

Para as 50–100 pessoas mais mencionadas, pré-gerar mini-biografias.

**Implementação**:

- Script CLI: `python main.py gerar-biografias --top 100`
- Lógica por entidade:
  1. Buscar entity + story + mentions + relations via repository
  2. Montar prompt com contexto estruturado (não chunks brutos)
  3. Chamar LLM (~300 tokens por biografia)
  4. Salvar em `data/structured/bios/{entity_id}.json`
- Endpoint: estender `GET /api/entity/{id}` para incluir `bio` se existir
- Frontend: mostrar bio no topo do dossier, antes das relações
- Custo estimado: 100 entidades × ~500 tokens = ~50K tokens ≈ US$0.01

### C3. Perguntas geradas por entidade

Para cada perfil de entidade, sugerir 3 perguntas específicas.

**Implementação**:
- No `get_entity_story()`, já temos `research_questions`
- Melhorar as perguntas usando relações concretas:
  - Se tem holds_role → "Que atos de {nome} como {cargo} aparecem nos jornais?"
  - Se tem co-menções fortes → "Qual a relação entre {nome} e {outro} em {período}?"
  - Se aparece em múltiplos jornais → "Por que {nome} é citado tanto no {jornal_A} quanto no {jornal_B}?"
- Perguntas clicáveis disparam busca RAG

---

## Fase D — Enriquecimento da extração

**Meta**: extrair mais relações biográficas para alimentar todas as features acima.
**Esforço**: alto. **Custo API**: zero. **Dependências**: nenhuma (pode rodar em paralelo).

### D1. Novos padrões de relação

Adicionar ao `src/structured/relations.py`:

```python
# Nomeação para cargo
# "nomeado/nomeada para o cargo de...", "eleito/eleita..."
("appointed_to", re.compile(...), 0.85, "probable")

# Falecimento
# "faleceu...", "falleceo...", "obituário de..."
("deceased", re.compile(...), 0.80, "hypothesis")

# Viagem / embarque
# "embarcou no porto de...", "partiu para...", "chegou de..."
("traveled_to", re.compile(...), 0.75, "hypothesis")

# Assinatura / autoria
# "assinado por...", "o abaixo assinado...", "requerimento de..."
("signed_by", re.compile(...), 0.78, "hypothesis")

# Relação financeira / comercial
# "credor de...", "devedor de...", "fiador de..."
("financial_relation", re.compile(...), 0.72, "hypothesis")
```

**Arquivo**: `src/structured/relations.py`
- Adicionar ao `RELATION_PATTERNS`
- Testar com amostras reais do corpus (extrair 10 páginas com `sqlite3`)
- Adicionar testes em `tests/test_structured.py`

### D2. Extração de eventos

Novo módulo `src/structured/events.py` para extrair acontecimentos datados.

**Tipos de evento**:
- Nomeação ("foi nomeado...")
- Sessão legislativa ("na sessão de..." + data)
- Edital ("faz saber que...")
- Embarque/chegada ("embarcou...", "chegou...")
- Falecimento ("faleceo...")

**Modelo de dados**:
```python
@dataclass
class ExtractedEvent:
    event_type: str          # "nomeacao", "sessao", "edital", "embarque", "obito"
    description: str         # texto curto descritivo
    date_text: str           # data como aparece no jornal
    participants: list[str]  # nomes das entidades envolvidas
    place: str               # lugar mencionado
    evidence_quote: str      # trecho original
    confidence: float
```

**Schema SQL** (nova tabela):
```sql
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,
    description TEXT,
    date_text TEXT,
    year INTEGER,
    page_id INTEGER REFERENCES pages(id),
    chunk_id TEXT,
    evidence_quote TEXT,
    confidence REAL DEFAULT 0.7,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS event_participants (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id INTEGER REFERENCES events(id),
    entity_id INTEGER REFERENCES entities(id),
    role_in_event TEXT  -- "nomeado", "nomeante", "falecido", "passageiro"
);
```

**Implementação**:
- `src/structured/events.py` — padrões regex para cada tipo
- `src/structured/schema.py` — adicionar tabelas
- `src/structured/service.py` — integrar ao `process_chunk()`
- `src/structured/repository.py` — métodos CRUD para events
- `src/web/api.py` — endpoint `GET /api/events?year=1831&type=nomeacao`
- Frontend: timeline de eventos na homepage e nos perfis de entidade

### D3. Melhoria de relações existentes

- Guardar múltiplas evidências por relação (hoje é 1:1, schema já suporta N:1)
- Melhorar confiança das relações existentes com contexto cruzado:
  - Se A→B (child_of) e B→C (spouse_of), aumentar confiança de ambas
  - Se A holds_role "Coronel" em 3 páginas diferentes, consolidar como "confirmed"
- Re-executar extração nos acervos já processados com novos padrões

---

## Fase E — Visualizações exploratórias

**Meta**: formas visuais de entrar na base.
**Esforço**: médio-alto. **Custo API**: zero. **Dependências**: Fases A e B.

### E1. Grafo de relações como homepage alternativa

Mostrar um subgrafo das 30 entidades mais conectadas (excluindo mentioned_with) como ponto de entrada visual.

**Implementação**:
- Reusar código D3.js já existente no frontend
- Novo endpoint: `GET /api/discovery/featured-graph?limit=30`
  - Query: entidades com mais relações não-mentioned_with
  - Retornar nodes + edges no formato já usado pelo graph viewer
- Frontend: renderizar como card expansível na homepage
  - Clicar num nó abre o dossier
  - Clicar numa aresta mostra a evidência

### E2. Mapa de calor temporal

Visualização de densidade por ano × tipo de entidade.

**Implementação**:
- Dados: reusar timeline do discovery overview, desagregar por entity_type
- Frontend: grid colorido (heatmap) com anos nas colunas e tipos nas linhas
- Clicar em uma célula filtra o radar para aquele ano + tipo

### E3. Nuvem de sobrenomes

Nuvem de palavras com os sobrenomes mais frequentes, dimensionados por menção.

**Implementação**:
- Backend: query de last token do canonical_name para persons, agrupado
- Frontend: renderizar com D3 cloud ou CSS simples
- Clicar num sobrenome dispara a busca por sobrenome (B2)

---

## Cronograma sugerido

### Sprint 1 — Homepage que convida (Fase A)

| Tarefa | Estimativa | Prioridade |
|--------|-----------|------------|
| A1. Personagem do dia — backend | 1 sessão | P0 |
| A1. Personagem do dia — frontend | 1 sessão | P0 |
| A2. Perguntas-gatilho com dados reais | 1 sessão | P0 |
| A3. Card "Neste período" | 1 sessão | P1 |
| B3. Timeline clicável | 1 sessão | P1 |

**Entrega**: homepage com personagem do dia, perguntas concretas, e timeline clicável.

### Sprint 2 — Trilhas e sobrenomes (Fase B)

| Tarefa | Estimativa | Prioridade |
|--------|-----------|------------|
| B2. Busca por sobrenome — backend | 1 sessão | P0 |
| B2. Busca por sobrenome — frontend | 1 sessão | P0 |
| B1. Trilhas temáticas — "Poder e Governo" | 1 sessão | P1 |
| B1. Trilhas temáticas — "Comércio e Porto" | 1 sessão | P1 |
| B1. Trilhas temáticas — "Igreja e Clero" | 1 sessão | P2 |

**Entrega**: busca por sobrenome funcional + 2 trilhas temáticas clicáveis.

### Sprint 3 — Narrativas e contexto (Fase C)

| Tarefa | Estimativa | Prioridade |
|--------|-----------|------------|
| C1. Resumo narrativo do acervo | 1 sessão | P0 |
| C3. Perguntas por entidade | 1 sessão | P1 |
| C2. Script de biografias automáticas | 1 sessão | P1 |
| C2. Integração bio no dossier | 1 sessão | P2 |

**Entrega**: resumo narrativo na homepage + biografias para top 100 entidades.

### Sprint 4 — Extração expandida (Fase D)

| Tarefa | Estimativa | Prioridade |
|--------|-----------|------------|
| D1. Padrões appointed_to, deceased | 1 sessão | P0 |
| D1. Padrões traveled_to, signed_by | 1 sessão | P1 |
| D1. Testes + validação no corpus | 1 sessão | P0 |
| D3. Múltiplas evidências por relação | 1 sessão | P1 |
| D2. Módulo de eventos — schema + extração | 2 sessões | P2 |
| D2. Módulo de eventos — API + frontend | 1 sessão | P2 |
| Re-executar extração nos acervos | 1 sessão | P0 |

**Entrega**: mais relações biográficas + eventos históricos extraídos.

### Sprint 5 — Visualizações (Fase E)

| Tarefa | Estimativa | Prioridade |
|--------|-----------|------------|
| E1. Grafo de relações na homepage | 1 sessão | P1 |
| E3. Nuvem de sobrenomes | 1 sessão | P2 |
| E2. Mapa de calor temporal | 1 sessão | P2 |

**Entrega**: ponto de entrada visual na homepage.

---

## Métricas de sucesso

| Métrica | Antes | Meta |
|---------|-------|------|
| Relações biográficas (family + role) | 1.206 | >5.000 |
| Entidades com biografia gerada | 0 | 100 |
| Trilhas temáticas disponíveis | 0 | 5 |
| Pontos de entrada na homepage (sem busca) | 1 (radar) | 6+ |
| Perguntas prontas com dados reais | 5 genéricas | 10+ concretas |
| Eventos históricos extraídos | 0 | >500 |
| Busca por sobrenome | inexistente | funcional |

---

## Relação com o roadmap existente (tasks.md)

Este plano se encaixa entre as Fases 6 (UI Historiográfica) e 7 (Revisão Humana) do roadmap atual:

- **Fase A-C** são extensões da **Fase 6** — melhorar a UI como instrumento de pesquisa
- **Fase D** complementa a **Fase 3** — expandir extração estruturada
- **Fase E** prepara terreno para a **Fase 9** — consulta baseada em grafo

O plano não substitui as fases pendentes (metadados, reindexação, revisão humana, grafo) — ele agrega valor ao que já existe enquanto essas fases avançam.

---

## Princípios de implementação

1. **Zero custo por padrão**: todas as features de descoberta funcionam sem chamada a LLM (exceto C1/C2, que são opcionais e cacheáveis).
2. **Incremental**: cada feature funciona independente — não precisa esperar as outras.
3. **Dados reais**: nenhuma feature mostra placeholder ou lorem ipsum — tudo vem do banco.
4. **Clicável**: toda informação exibida leva a algum lugar (perfil, página, busca).
5. **Verificável**: todo dado mostrado pode ser rastreado até a página original do jornal.
