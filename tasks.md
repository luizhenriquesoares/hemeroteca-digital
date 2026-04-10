# Tasks

## Objetivo

Transformar a base atual em uma plataforma historiográfica confiável, com:
- OCR preservado quando já for melhor
- metadados editoriais ricos
- extração estruturada de entidades e relações com evidência
- camada de consulta verificável
- exportação futura para banco orientado a grafos

## Princípios

- Nunca degradar o OCR existente sem ganho claro.
- Toda relação histórica precisa apontar para evidência documental.
- A interface deve sempre permitir verificar a página original do jornal.
- O grafo deve ser alimentado por dados estruturados com proveniência, não por respostas livres do LLM.
- Resolver primeiro qualidade e confiabilidade da base; só depois expandir visualização e grafo.

## Fase 1: Estabilização da Base

- [x] Consolidar entrada web em uma única app.
- [x] Corrigir dependências principais do projeto.
- [x] Melhorar busca híbrida para nomes históricos e variantes nominais.
- [x] Melhorar RAG para responder com evidências e fontes.
- [x] Preservar metadados editoriais no pipeline hi-res.
- [x] Criar camada estruturada inicial em SQLite.
- [x] Expor endpoints básicos de entidade e página.
- [x] Criar modo seguro para enriquecer apenas metadados sem refazer OCR.

## Fase 2: Metadados e Reindexação

- [ ] Rodar `enriquecer-metadados` nos acervos prioritários.
- [ ] Recriar chunks dos acervos enriquecidos.
- [ ] Reexecutar `estruturar` nos mesmos acervos.
- [ ] Reindexar os acervos enriquecidos.
- [ ] Validar manualmente se periódico, ano, edição e paginação aparecem corretamente na UI.

## Fase 3: Extração Estruturada

- [x] Melhorar extração de pessoas com menos falso positivo institucional.
- [x] Adicionar extração de lugares.
- [x] Adicionar extração de instituições.
- [x] Adicionar extração de cargos e funções com mais precisão.
- [x] Expandir relações:
  - `spouse_of`
  - `child_of`
  - `parent_of`
  - `holds_role`
  - `member_of`
  - `resident_of`
  - `mentioned_with`
- [ ] Guardar mais de uma evidência por relação.
- [x] Melhorar normalização histórica de nomes e aliases.

## Fase 4: Resolução de Identidade

- [x] Separar claramente `menção` de `entidade consolidada`.
- [x] Criar heurística de fusão por:
  - nome normalizado
  - cargo
  - período
  - local
  - coocorrência com parentes e instituições
- [x] Evitar fusão agressiva de homônimos.
- [x] Marcar entidades ambíguas para revisão futura.

## Fase 5: Qualidade e Confiabilidade

- [x] Melhorar benchmark de OCR com métricas historiográficas mais úteis.
- [x] Melhorar benchmark comparando:
  - OCR salvo
  - OCR adaptativo
  - texto corrigido
- [x] Criar score operacional para detectar páginas ruins candidatas a reprocessamento.
- [x] Ampliar testes de regressão para busca de pessoas e famílias.
- [ ] Validar consultas reais com casos como:
  - João Affonso Botelho
  - Antonio Benedicto d'Araujo Pernambuco
  - Botelho / Botelhos

## Fase 6: UI Historiográfica

- [ ] Melhorar ficha de pessoa com:
  - nomes variantes
  - menções por periódico
  - linha do tempo simples
  - relações agrupadas
  - evidências clicáveis
- [ ] Exibir melhor a página do jornal e a imagem correspondente.
- [ ] Adicionar navegação entre entidade, evidência e página.
- [ ] Mostrar claramente status da relação:
  - hipótese
  - provável
  - confirmado
  - rejeitado

## Fase 7: Revisão Humana Leve

- [ ] Criar status de revisão para relações sensíveis.
- [ ] Permitir confirmar ou rejeitar relações familiares.
- [ ] Permitir marcar fusões erradas de identidade.
- [ ] Registrar revisão sem destruir a evidência original.

## Fase 8: Exportação para Grafo

- [ ] Criar `src/graph_store.py`.
- [ ] Definir schema inicial do grafo:
  - `Person`
  - `Publication`
  - `Issue`
  - `Page`
  - `Institution`
  - `Place`
  - `Mention`
- [ ] Definir arestas com proveniência:
  - `MENTIONED_IN`
  - `SPOUSE_OF`
  - `CHILD_OF`
  - `PARENT_OF`
  - `HOLDS_ROLE`
  - `MEMBER_OF`
  - `RESIDENT_OF`
- [ ] Exportar SQLite -> grafo de forma idempotente.
- [ ] Guardar em cada aresta:
  - `confidence`
  - `status`
  - `quote`
  - `source_page_id`
  - `source_chunk_id`

## Fase 9: Consulta Baseada em Grafo

- [ ] Criar comando `exportar-grafo`.
- [ ] Criar consulta híbrida:
  - busca documental
  - busca estrutural
  - busca no grafo
- [ ] Responder perguntas sobre pessoas com:
  - perfil
  - relações
  - periódicos
  - evidências
  - link da página original

## Próximos Passos Recomendados

1. Rodar `enriquecer-metadados` nos acervos prioritários.
2. Reexecutar `chunkar`, `estruturar` e `indexar` nesses acervos.
3. Melhorar a extração de entidades e relações.
4. Consolidar a camada SQLite.
5. Só então implementar a exportação para banco de grafos.
