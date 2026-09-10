# Status do Projeto

## Visão Geral

Este projeto evoluiu de um pipeline de scraping + OCR + busca para uma base historiográfica mais confiável, com foco em:

- preservar o OCR já existente quando ele for melhor
- enriquecer metadados editoriais das páginas
- extrair entidades e relações com evidência documental
- permitir verificação da página original do jornal
- preparar o terreno para um banco orientado a grafos

A direção técnica adotada foi: primeiro fortalecer qualidade, proveniência e consulta verificável; só depois expandir para grafo e revisão humana.

## O que já foi feito

### 1. Saneamento da base

- Correção de dependências principais em `requirements.txt`.
- Consolidação da entrada web em uma única aplicação FastAPI.
- Transformação de `web.py` em wrapper compatível para a API oficial.
- Ajustes no `README` para refletir comandos reais do projeto.

### 2. Busca e RAG

- Melhoria da busca híbrida para nomes históricos e variantes nominais.
- Normalização histórica para casos como:
  - `Benedito` / `Benedicto`
  - `Afonso` / `Affonso`
  - `d'Araujo` / `de Araujo`
  - `Botelho` / `Botelhos`
- RAG endurecido para responder com:
  - resumo
  - pessoas e entidades
  - evidências
  - fontes
- Inclusão de evidência focada e fontes mais legíveis no backend e no frontend.

### 3. Metadados editoriais

- Preservação de metadados no pipeline hi-res:
  - `jornal`
  - `periodico`
  - `ano`
  - `edicao`
  - `pagina_fisica`
  - `pagina_logica`
  - `pasta`
- Implementação de um modo seguro de enriquecimento de metadados sem refazer OCR:
  - atualiza apenas `.json`
  - preserva `.txt`
  - preserva `_corrigido.txt`
  - não altera chunks nem índice até comando explícito

### 4. OCR e benchmark

- OCR adaptativo implementado para teste e comparação.
- Decisão conservadora adotada:
  - manter o OCR antigo como padrão quando ele estiver melhor
  - usar o OCR novo apenas se superar claramente o antigo
- Benchmark OCR criado para comparar:
  - OCR salvo
  - OCR adaptativo
  - texto corrigido
- Benchmark melhorado com métricas historiográficas:
  - `historiographic_score`
  - `named_entity_ratio`
  - `heading_ratio`
  - `date_signal`
  - `operational_bad_page_score`
  - recomendação operacional: `manter`, `revisar`, `reprocessar`

### 5. Correção via LLM

- Unificação do fluxo de correção para alternar entre OpenAI e Claude.
- Criação de switch por provider/modelo.
- Benchmark de correção criado para comparar saídas por amostra.
- Conclusão prática até aqui:
  - Claude continua superior para correção histórica difícil
  - OpenAI mini é útil para escala/custo
  - o switch ficou pronto para alternância operacional

### 6. Camada estruturada antes do grafo

- Criação de uma camada estruturada em SQLite para persistir:
  - páginas
  - entidades
  - menções
  - relações
  - evidências
- Endpoints básicos expostos:
  - busca de entidade
  - detalhe de entidade
  - detalhe de página
- Extração heurística implementada para:
  - pessoas
  - instituições
  - lugares
- Relações estruturadas implementadas:
  - `spouse_of`
  - `child_of`
  - `parent_of`
  - `holds_role`
  - `member_of`
  - `resident_of`
  - `mentioned_with`

### 7. Resolução de identidade

- Introdução de uma camada leve de resolução de identidade.
- Separação prática entre:
  - menção textual
  - entidade consolidada
- Uso de contexto para desambiguar:
  - cargo/título
  - instituição
  - lugar
  - ano
  - coocorrência familiar
- Persistência de metadados de identidade:
  - `identity_key`
  - `identity_status`
  - `identity_hints`
  - `identity_confidence`
- Busca estrutural ajustada para considerar aliases e variantes históricas.

### 8. UI historiográfica

- Frontend evoluído de “chat puro” para interface mais historiográfica.
- Inclusão de ficha de pessoa com:
  - nome canônico
  - variantes
  - status de identidade
  - pistas de identidade
  - relações
  - menções
  - evidências documentais
- Melhoria recente da ficha com:
  - recorte temporal
  - periódicos onde a entidade aparece
  - agrupamento simples de relações
  - linha do tempo simples por ano
- Links para:
  - `ver página`
  - `imagem`

### 9. Arquitetura e refactoring

- Reorganização completa do projeto por domínio:
  - `src/cli`
  - `src/web`
  - `src/processing`
  - `src/scraping`
  - `src/structured`
  - `src/benchmarks`
- Extração de módulos internos para reduzir arquivos monolíticos.
- Isolamento de scripts experimentais em `scripts/experimental`.
- Limpeza do topo de `src/`.
- `.gitignore` ajustado para não subir artefatos locais.
- Refactoring de scraping, OCR, busca, API, CLI e persistência.

## Ideias e decisões arquiteturais

### Princípios adotados

- Não degradar OCR antigo sem ganho claro.
- Toda relação histórica precisa apontar para evidência.
- Toda resposta importante deve permitir verificação da página original.
- O sistema deve distinguir:
  - fato
  - inferência
  - hipótese
- O grafo não deve ser alimentado por texto livre do LLM.

### Direção do produto

O produto está sendo construído em camadas:

1. Documento e evidência
- OCR
- texto corrigido
- metadados editoriais
- chunking
- indexação

2. Estrutura
- entidades
- menções
- relações
- evidências
- identidade

3. Consulta verificável
- busca híbrida
- ficha de pessoa
- evidências clicáveis
- página original

4. Grafo
- exportação futura a partir da base estruturada

### Ideia central para o futuro grafo

Quando o grafo entrar, ele deve representar:

- `Person`
- `Publication`
- `Issue`
- `Page`
- `Institution`
- `Place`
- `Mention`

E as arestas devem carregar proveniência:

- `MENTIONED_IN`
- `SPOUSE_OF`
- `CHILD_OF`
- `PARENT_OF`
- `HOLDS_ROLE`
- `MEMBER_OF`
- `RESIDENT_OF`

Com propriedades como:

- `confidence`
- `status`
- `quote`
- `source_page_id`
- `source_chunk_id`

## O que ainda está pendente

### Operação nos acervos reais

Ainda falta executar em acervos prioritários:

- `enriquecer-metadados`
- `chunkar --force`
- `estruturar`
- `indexar`

Sem isso, parte das melhorias já implementadas existe no código, mas ainda não foi aplicada em toda a base real.

### Extração estruturada

Pendências principais:

- guardar mais de uma evidência por relação
- melhorar ainda mais a precisão de relações sensíveis
- validar comportamento no corpus real, não só em testes unitários

### Qualidade e confiabilidade

Ainda falta validação empírica no acervo com casos reais como:

- `João Affonso Botelho`
- `Antonio Benedicto d'Araujo Pernambuco`
- `Botelho` / `Botelhos`

Ou seja: os testes automatizados existem, mas a validação historiográfica sobre o corpus real ainda precisa ser feita.

### UI historiográfica

Ainda há espaço claro para evoluir:

- melhorar navegação entre entidade, evidência e página
- mostrar melhor a imagem da página
- deixar status das relações mais legíveis
- tornar a ficha de pessoa ainda mais forte como instrumento de pesquisa

### Revisão humana leve

Ainda não foi implementado:

- confirmar ou rejeitar relações familiares
- corrigir fusões erradas de identidade
- registrar revisão sem destruir a evidência original

Essa etapa é importante antes de consolidar o futuro grafo como base histórica confiável.

### Banco orientado a grafos

Ainda não foi implementado:

- `src/graph_store.py`
- schema inicial do grafo
- exportação idempotente SQLite -> grafo
- consulta baseada em grafo

Ou seja: o projeto está no “pré-grafo”, com uma base estruturada pronta para alimentar essa etapa.

## Estado atual resumido

Hoje o projeto já tem:

- pipeline operacional de captura/OCR/correção/chunking/indexação
- busca híbrida melhor para nomes históricos
- RAG mais orientado a evidência
- metadados editoriais enriquecíveis sem refazer OCR
- camada estruturada em SQLite
- resolução inicial de identidade
- ficha historiográfica no frontend
- benchmark de OCR e benchmark de correção
- arquitetura interna muito mais limpa

Hoje o projeto ainda não tem:

- revisão humana leve
- validação historiográfica ampla no acervo real
- múltiplas evidências por relação consolidadas
- exportação real para grafo
- consulta baseada em grafo

## Próximos passos recomendados

1. Rodar o enriquecimento de metadados nos acervos prioritários.
2. Reexecutar `chunkar`, `estruturar` e `indexar` nesses acervos.
3. Validar consultas históricas reais na UI.
4. Adicionar múltiplas evidências por relação.
5. Implementar revisão humana leve.
6. Só então partir para exportação para banco de grafos.
