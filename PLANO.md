# Plano — Hemeroteca Digital PE (setembro de 2026)

## Tese

O projeto hoje é uma busca sobre OCR ruidoso. A versão que vale a pena construir é uma
**base estruturada e verificável de acontecimentos do Pernambuco oitocentista**, com um
historiador de IA que responde citando o recorte da página, e com os dados publicados
abertos. A fundação atual não sustenta isso: o Tesseract a ~60% de acerto contamina
entidades, relações e grafo, e só um acervo foi capturado em escala.

## O que muda em relação a abril

| Abril | Agora |
|---|---|
| Tesseract → correção LLM → chunk → regex | Uma leitura da imagem devolve segmentos tipados + estrutura (`src/vision`) |
| Relação = coocorrência (`mentioned_with`, 1,2M arestas) | Relação só explícita na fonte, com trecho literal e recorte |
| Página inteira como unidade | Segmento editorial (edital, fuga, porto, sentença…) como unidade |
| RAG "resposta com fontes" | Agente historiador com ferramentas e citações localizadas |
| Acervo inteiro (1,1M páginas, 1825-1990) | Século XIX primeiro (~150k páginas) |
| Railway + upload por partes | Servidor próprio, dados abertos, API pública |

Os planos de abril continuam válidos como anexos: `PLANO_DESCOBERTA.md` (UI de descoberta,
em boa parte feito), `PLANO_GENEALOGIA.md` (Açores, retomar na Fase 3) e
`ESTIMATIVA_CUSTOS.md` (a premissa de US$ 0,01/página mediu 780 tokens de saída, que não é
uma página transcrita; o benchmark abaixo remede).

## O que mudou no ecossistema desde abril

- Claude Opus 5 e Sonnet 5 com janela de 1M tokens: uma edição inteira numa chamada, itens
  que atravessam colunas e páginas viram um só segmento.
- Saídas estruturadas com schema garantido: o JSON sempre valida, sem parser de regex.
- Batch API a 50%: a ingestão inteira roda em lote.
- API de citações com localização por página e trecho: proveniência para o agente.
- Isso não existia no benchmark de abril em que o Claude perdeu para o GPT-4o; o teste
  precisa ser refeito.

## Fase 1: o benchmark decide

`python main.py benchmark-visao --golden --dry-run` conta os tokens de entrada de graça.
`python main.py benchmark-visao --golden` roda as 6 configurações nas 20 páginas douradas.

| Configuração | O que mede |
|---|---|
| `opus-full`, `sonnet-full`, `haiku-full` | Transcrição integral por segmento + estrutura; custo dominado pela saída |
| `opus-structure`, `sonnet-structure`, `haiku-structure` | Só estrutura e âncora curta; o texto integral viria de um OCR barato |
| (a criar) `ocr+opus-text` | OCR dedicado barato → Claude extrai sobre texto (entrada barata, sem tiles) |

Métricas: tokens reais, US$ por página avulso e em Batch, extrapolação para 150k páginas,
proporção de palavras válidas, cobertura do vocabulário da referência local, número de
segmentos, pessoas e relações, tempo. A leitura humana de 5 páginas fecha a decisão.

Estimativa prévia (20k tokens de entrada em tiles, 8k de saída, Batch): Opus 5 ≈ US$ 0,15/pág
(US$ 22k em 150k páginas), Sonnet 5 ≈ US$ 0,06 (US$ 9k), Sonnet só estrutura ≈ US$ 0,035
(US$ 5k), híbrido OCR barato + Opus sobre texto ≈ US$ 0,04 (US$ 6k).

## Resultados parciais do benchmark (2026-09-09, 1 página)

Página `029033_01_00001` (capa de 7/11/1825, tipos grandes, ~2.100 caracteres), rodada pelo
Claude Code headless (assinatura). Custo convertido para preço de API: entrada = recortes pela
fórmula de pixels + prompt; saída = tokens reais (raciocínio incluso).

| Configuração | Grade | Entrada | Saída | US$/pág | Batch | Tempo | Leitura |
|---|---|---|---|---|---|---|---|
| opus-full (effort low) | 4x3 | 19,9k | 3,3k | 0,18 | 0,09 | 45 s | Melhor: ortografia da época, segmentos e pessoa certos |
| opus-full (effort low) | 2x2 | 7,6k | 2,4k | 0,10 | 0,05 | 36 s | Igual em texto; errou um sobrenome (Ferreira/Parreira) |
| opus-full (effort low) | 3x2 | 10,7k | 3,7k | 0,14 | 0,07 | 48 s | Igual em texto; nome com [?] |
| sonnet-full (effort low) | 4x3 | 19,9k | 6,1k | 0,10 | 0,05 | 86 s | Transcrição cheia de reconstruções [entre colchetes] nas bordas dos recortes |
| sonnet-structure (medium) | 4x3 | 19,9k | 13,3k | 0,17 | 0,09 | 165 s | Pensa demais (10,6k tokens); pior e mais caro que o full |
| haiku-full | 4x3 | 19,9k | 6,8k | 0,05 | 0,03 | 74 s | Moderniza a grafia, classifica venda de brigue como venda de escravizado; descartado |

O que já dá para concluir:

- Opus 5 com `effort low` é a leitura de referência: quase não raciocina (56 tokens) e preserva a
  grafia. Sonnet 5 custa metade e perde qualidade nas bordas dos recortes. Haiku está fora.
- O modo `structure` não compensa: gasta mais raciocínio que a transcrição e perde texto.
  Produção deve usar `full` e derivar a estrutura do mesmo JSON.
- A entrada (recortes) é mais da metade do custo do Opus. A grade precisa se adaptar ao tamanho
  do tipo: 2x2 basta nesta capa, páginas densas de 4 colunas devem exigir 4x3 ou mais.
- Extrapolação para 150k páginas com Opus full em Batch: US$ 7,5k (2x2) a 13,5k (4x3). Páginas
  densas terão saída de 5 a 10k tokens, então o número final deve ficar entre isso e o dobro.
- O que decide é a rodada nas 19 páginas douradas densas, que ainda precisam de captura hi-res.

Lições de execução: o `--json-schema` do Claude Code não aceita `$defs` (o modelo devolvia os
nomes das definições como chaves), o Sonnet sem `--effort` gastou 60k tokens de raciocínio numa
página, e o custo informado pelo Claude Code inclui ~33k tokens de harness em cache, por isso a
conversão para preço de API.

## Arquitetura alvo

```
imagem hi-res ──► tiles ──► Claude (schema PageExtraction, Batch) ──► segments / people / relations
                                                                        │
      OCR barato (texto integral p/ busca) ─────────────────────────────┤
                                                                        ▼
                         SQLite/Postgres + índice vetorial por segmento + recortes de imagem
                                                                        │
                    agente historiador (SQL, vetor, abrir folha, comparar) ──► dossiê citado
                                                                        │
                       datasets abertos (fugas, porto, nomeações) + API pública + /dados
```

## Sequência

1. Fase 0 e Fase 1 (setembro): saneamento, benchmark, decisão.
2. Fase 2 (outubro): ingestão visão-primeiro do `029033_01` completo; busca por segmento.
3. Fase 3 e 4: identidade com juiz LLM; agente historiador substitui o RAG.
4. Fase 5: primeiro dataset aberto (fugas de escravizados) e parcerias.
5. Fases 6 e 7 correm em paralelo conforme a captura e a hospedagem exigirem.

Detalhe por item em `tasks.md`.
