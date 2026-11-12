# Tasks — Hemeroteca Digital PE (roadmap 2026-09)

Atualizado em 2026-09-09. Substitui o roadmap de abril (Fases 1-9); o que dele foi
feito está listado no fim. Estratégia e decisões: `PLANO.md`. Estado: `PROJECT_STATUS.md`.

## Princípios

- Toda relação histórica aponta para evidência: trecho literal + página + recorte da imagem.
- O grafo é alimentado por dados estruturados com proveniência, nunca por texto livre do LLM.
- Fato, inferência e hipótese são distinguidos e revisáveis.
- Zero custo por padrão na consulta; o gasto com modelo fica na ingestão, em lote.
- Recorte primeiro: século XIX (1825-1889), depois o resto.

## Fase 0 — Saneamento (setembro)

- [x] Commitar o trabalho pendente de abril (`7f7de98`).
- [x] Segredo de upload sai do código (`HEMEROTECA_UPLOAD_SECRET`; endpoints desligados sem a variável).
- [ ] Rebaixar ou remover as arestas `mentioned_with` do grafo exportado (hairball).
- [ ] CI rodando `pytest tests/` (GitHub Actions).
- [ ] Aposentar o hack de upload para o Railway; hospedagem definida na Fase 7.

## Fase 1 — Benchmark visão-primeiro (a decisão)

- [x] Pacote `src/vision`: schema (`PageExtraction`), tiles, leitor Claude com saída estruturada, benchmark.
- [x] Comando `benchmark-visao` (`--page`, `--golden`, `--config`, `--grid`, `--dry-run`, `--extrapolar`).
- [x] Backend `claude-code` (`claude -p` headless, `--json-schema`) para rodar pela assinatura sem créditos de API.
- [ ] Créditos de API no Console para o backend `api` (Batch a 50% só existe na API).
- [x] Manifesto de 20 páginas douradas em `src/vision/golden.json`.
- [x] Recapturar as 19 páginas douradas sem imagem hi-res local (`ocr-hires --keep-images`). Feito em 2026-09-18 via `ocr-hires --golden --keep-images`; as 20 estão em `data/images/`.
- [x] 1 página real por configuração (Claude Code): resultados em `PLANO.md`; Opus full effort low é candidato; seleção final depende de gabarito e páginas densas.
- [x] Recortes experimentais por layout e altura estimada de caracteres (`--tiling layout`), com visão geral e prévia offline.
- [ ] Validar tamanho dos caracteres e layout nas páginas densas; ainda sem correção de inclinação ou releitura seletiva.
- [x] Avaliador offline CER/WER por trechos; referência provisória de 5 trechos da capa, sem revisão humana.
- [x] Corrigir seleção do Chrome no capturador. O código já derivava a versão de `installed_chrome_major()`; a captura por ID foi implementada em 2026-09-18 e a BN respondeu com o Chrome 153 instalado.
- [ ] Homologar referências em 3–5 páginas densas, incluindo nomes e relações.
- [ ] Rodar as 20 páginas nas 6 configurações; revisar 5 à mão (colunas, pessoas, relações, ortografia).
- [ ] Configuração híbrida: OCR barato dedicado + Claude só na extração sobre texto (`--config ocr+opus-text`).
- [ ] Aposentar o modo `structure` se as páginas densas confirmarem que `full` é melhor e mais barato.
- [ ] Decidir a configuração de produção (qualidade por dólar) e registrar em `PLANO.md`.
- [x] Detecção conservadora de cabeçalhos/colunas por espaços e linhas; caixas e escalas registradas no benchmark.

## Fase 2 — Pipeline visão-primeiro em produção

- [ ] Comando `ler-visao --bib X` usando Batch API (50%) e cache do system prompt.
- [ ] Persistir `PageExtraction`: tabelas `segments`, `segment_people`, `segment_relations` com `page_id` e tiles.
- [ ] Processar por EDIÇÃO (páginas da mesma data numa chamada) para itens que atravessam páginas.
- [ ] Recorte (crop) do segmento a partir dos tiles como proveniência visual.
- [ ] Reindexar a busca com chunk = segmento (não janela de 1000 caracteres).
- [ ] Aposentar `ocr`, `corrigir`, `chunkar` e `estruturar` para acervos lidos por visão.

## Fase 3 — Identidade e qualidade

- [ ] Blocking de candidatos (nome normalizado + período + lugar) e juiz LLM para fusão.
- [ ] Rebaixar entidades sem segmento tipado; fila de revisão só com relação biográfica.
- [ ] Múltiplas evidências por relação na persistência.
- [ ] Amostragem de QA humana (20 páginas por mês) com erros registrados por tipo de segmento.

## Fase 4 — Historiador de IA

- [ ] Agente com ferramentas: busca estruturada (SQL), busca semântica, abrir folha e recorte, comparar entidades.
- [ ] Citações com localização (API de citações) e link para a folha na BN.
- [ ] Dossiê exportável (Markdown/PDF) com todas as provas.
- [ ] Substituir o RAG atual pelo agente na UI.

## Fase 5 — Verticais e dados abertos

- [ ] Dataset "Anúncios de fuga de escravizados, Pernambuco 1825-1888" (CSV e JSON, CC-BY, DOI).
- [ ] Dataset "Movimento do porto do Recife" (embarcações, procedências, passageiros).
- [ ] Dataset "Nomeações e cargos da província".
- [ ] API pública de leitura e página `/dados`.
- [ ] Contato com Fundaj, UFPE (LAHPE) e APEJE; pedir acesso em lote à BN.

## Fase 6 — Captura

- [ ] Testar download direto da imagem do DocReader com cookies de sessão resolvida, sem dirigir o Chrome por página.
- [ ] Se falhar: Safari com AppleScript (provado noutro projeto) ou Chrome visível em IP residencial.
- [ ] Recapturar `029033_01` (1.534 falhas e cauda 12.482-13.723), depois `029033_02` a `07` (1840-1899).
- [ ] Guardar as imagens hi-res após a leitura (disco ou S3 barato); hoje o pipeline apaga.

## Fase 7 — Infra e frontend

- [ ] Servidor próprio de 8-16 GB (Hetzner ou Lightsail grande); Postgres com pgvector ou SQLite com sqlite-vec; sem Railway.
- [ ] Quebrar `frontend/index.html` (5k linhas) em módulos; página `/dados`.
- [ ] Açores: retomar o cruzamento genealógico depois da Fase 3.

## Feito em abril de 2026 (referência)

Pipeline Tesseract, correção por LLM, chunks e ChromaDB; camada estruturada em SQLite com
identidade e revisão humana (endpoints); grafo (`graph_store`, `exportar-grafo`); descoberta na
home (personagem do dia, trilhas, sobrenomes, grafo interativo); landing `/sobre`; biografias
automáticas; módulo Açores com uma paróquia transcrita por gpt-4o.

## Mesa de pesquisa — setembro de 2026

- [x] Nova entrada responsiva com busca, grafo por camadas e painel de fontes; interface anterior em `/avancado`.
- [x] Leitor local com imagem, zoom, transcrição e localização de palavras.
- [x] Busca textual FTS5 por palavras e período, sem chamadas a modelos.
- [x] Corrigir colisões de nós literais e agrupar ligações repetidas preservando IDs de extração.
- [x] Consultar evidência da relação exata, incluindo objetos literais e última revisão.
- [ ] Completar imagens e metadados ausentes e revisar extrações sem evidência; esta interface não valida automaticamente o corpus.

## Arquivo vivo — experiência de investigação

- [x] Entrada editorial com a capa original, movimento e caminhos temáticos para começar uma pesquisa.
- [x] Cinema de documentos reais, com transporte, ritmo, cenas selecionáveis e pausa para ler.
- [x] Caderno local com trechos, notas persistentes, exportação Markdown e desfazer retirada.
- [x] Confronto de duas fontes com transcrições completas lado a lado.
- [x] Retomar entidades visitadas pelo caminho da investigação.
- [x] Responsividade e redução de movimento; fac-símile distinguido da apresentação de OCR.

## Laboratório de descobertas — seis instrumentos

- [x] Detetive de identidades com grafias, anos, contextos, evidências e ressalvas; sem fusão automática.
- [x] Pergunta vira investigação salva, com fontes candidatas, hipótese editável, lacunas e exportação.
- [x] Atlas temporal de fontes com posições modernas anotadas pelo pesquisador e exportação GeoJSON.
- [x] Caminhos de até duas relações entre entidades, com evidências e coocorrências separadas.
- [x] Próximas pistas a partir da última fonte salva, com motivo e documentos para conferir.
- [x] Editor de documentário com roteiro persistente, cenas reordenáveis, vídeo 720p e áudio anexado opcional.
- [ ] Ampliar cobertura de imagens, revisão de entidades e georreferenciamento histórico validado; as ferramentas atuais não substituem essa curadoria.
