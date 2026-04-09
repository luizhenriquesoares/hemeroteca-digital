"""Heurísticas de qualidade para curadoria da camada estruturada."""

from __future__ import annotations

from src.structured.entities import INSTITUTION_MARKERS, PLACE_STOPWORDS, normalize_name

VESSEL_MARKERS = {
    "sumaca",
    "sumaca",
    "barca",
    "barco",
    "bergantim",
    "corveta",
    "escuna",
    "fragata",
    "galeota",
    "navio",
    "vapor",
}

PLACE_MARKERS = {
    "bairro",
    "cidade",
    "freguesia",
    "ilha",
    "largo",
    "povoacao",
    "povoação",
    "porto",
    "praia",
    "provincia",
    "província",
    "recife",
    "rio",
    "rua",
    "villa",
    "vila",
}

FALSE_POSITIVE_PHRASES = {
    "deos guarde": "fórmula editorial ou expressão fixa, não pessoa histórica",
    "deus guarde": "fórmula editorial ou expressão fixa, não pessoa histórica",
    "guarde deos": "fórmula editorial ou expressão fixa, não pessoa histórica",
    "guarde deus": "fórmula editorial ou expressão fixa, não pessoa histórica",
    "diario pernambuco": "título editorial extraído como se fosse pessoa",
    "diario de pernambuco": "título editorial extraído como se fosse pessoa",
    "rio grande sul": "topônimo extraído como se fosse pessoa",
    "rio grande": "topônimo extraído como se fosse pessoa",
    "villa conde": "topônimo extraído como se fosse pessoa",
    "vila conde": "topônimo extraído como se fosse pessoa",
    "porto alegre": "topônimo extraído como se fosse pessoa",
    "santa luzia": "topônimo ou embarcação extraído como pessoa",
    "sumaca santa luzia": "embarcação extraída como pessoa",
    "praca uniao": "topônimo ou praça pública extraído como pessoa",
    "praça uniao": "topônimo ou praça pública extraído como pessoa",
    "le citoyen": "título editorial ou publicação estrangeira extraído como pessoa",
    "forte mattos": "fortificação extraída como pessoa",
    "forte mato": "fortificação extraída como pessoa",
    "proclamacao assemblea geral brazil": "título político ou documental extraído como pessoa",
    "proclamacao assemblea geral brasil": "título político ou documental extraído como pessoa",
}

EDITORIAL_STOPWORDS = {
    "sessao", "sessão", "summario", "sumario", "indice", "artigo",
    "edicao", "edição", "numero", "folha", "pagina", "texto",
    "formula", "fórmula",
}

GENERIC_SINGLE_TOKENS = {
    "boa", "comp", "art", "dito", "dita", "idem", "anno",
    "nos", "sua", "seus", "elle", "ella",
}

DISCOVERY_FALSE_POSITIVE_PHRASES = {
    "diario": "rótulo editorial genérico aparecendo como entidade histórica",
    "diario de pernambuco": "título editorial genérico aparecendo no radar como entidade do corpus",
    "tipografia": "oficina editorial genérica aparecendo como lugar",
    "tipografia do diario": "marca editorial aparecendo como lugar",
    "sessao": "marcador editorial ou de ata, não entidade histórica",
    "sessão": "marcador editorial ou de ata, não entidade histórica",
    "boa": "fragmento genérico sem valor histórico isolado",
    "comp": "abreviação genérica sem desambiguação",
    "art": "marcador editorial de artigo",
    "acervo": "rótulo técnico do sistema, não entidade histórica",
    "praca": "marcador espacial genérico sem valor toponímico suficiente",
    "praça": "marcador espacial genérico sem valor toponímico suficiente",
    "conselho": "instituição genérica aparecendo como lugar",
    "forte mattos": "fortificação aparecendo como pessoa ou lugar em destaque indevido",
    "forte mato": "fortificação aparecendo como pessoa ou lugar em destaque indevido",
}

DISCOVERY_GENERIC_SINGLE_TOKENS = {
    "boa", "comp", "art", "diario", "sessao", "sessão", "tipografia",
    "dito", "dita", "idem", "acervo", "praca", "praça", "conselho",
}

PERSON_CONTEXT_ONLY_MARKERS = {
    "administrador", "alferes", "assemblea", "assembleia", "brasil",
    "brigadeiro", "camara", "capitao", "capitão", "citoyen",
    "commandante", "comandante", "conselheiro", "coronel", "desembargador",
    "director", "diretor", "forte", "general", "geral", "governo",
    "inspector", "juiz", "junta", "major", "militar", "padre", "praca",
    "praça", "prefeito", "presidente", "proclamacao", "proclamação",
    "secretario", "secretário", "tenente", "trem", "uniao", "união",
    "vigario", "vigário", "brazil",
}


def assess_entity_noise(
    *,
    entity_type: str,
    canonical_name: str,
    attributes: dict | None = None,
) -> dict:
    """Avalia se a entidade parece ser ruído estrutural."""

    normalized = normalize_name(canonical_name)
    tokens = [token for token in normalized.split() if token]
    token_set = set(tokens)
    attrs = attributes or {}
    reasons: list[str] = []
    score = 0.0

    if entity_type != "person":
        return {"score": 0.0, "reasons": [], "is_probable_noise": False}

    exact_reason = FALSE_POSITIVE_PHRASES.get(normalized)
    if exact_reason:
        reasons.append(exact_reason)
        score = max(score, 0.96)

    if token_set & VESSEL_MARKERS:
        reasons.append("vocabulário de embarcação aparecendo como pessoa")
        score = max(score, 0.83)

    if token_set & PLACE_MARKERS:
        reasons.append("vocabulário geográfico ou urbano aparecendo como pessoa")
        score = max(score, 0.76)

    # Normalizar marcadores institucionais para cobrir variantes ortográficas
    institution_normalized = {normalize_name(m) for m in INSTITUTION_MARKERS}
    if token_set & institution_normalized:
        reasons.append("vocabulário institucional aparecendo como pessoa")
        score = max(score, 0.74)

    if token_set & PLACE_STOPWORDS:
        reasons.append("estrutura toponímica forte para uma entidade marcada como pessoa")
        score = max(score, 0.7)

    if tokens[:1] and tokens[0] in {"villa", "vila", "rio", "rua", "bairro", "sumaca", "vapor"}:
        reasons.append("nome começa com marcador típico de lugar, logradouro ou embarcação")
        score = max(score, 0.78)

    if token_set & EDITORIAL_STOPWORDS:
        reasons.append("vocabulário editorial ou genérico aparecendo como pessoa")
        score = max(score, 0.76)

    if len(tokens) == 1 and tokens[0] in GENERIC_SINGLE_TOKENS:
        reasons.append("token único genérico, não identifica pessoa histórica")
        score = max(score, 0.85)

    hints = attrs.get("identity_hints") or []
    if attrs.get("identity_status") in {"ambiguous", "contextual"} and hints and all(
        str(item).startswith("year:") for item in hints
    ):
        reasons.append("identidade sustentada apenas por pista temporal")
        score = max(score, 0.58)

    if len(tokens) == 2 and all(len(token) <= 4 for token in tokens):
        reasons.append("nome muito curto e genérico para uma pessoa histórica individualizada")
        score = max(score, 0.52)

    unique_reasons = []
    seen = set()
    for item in reasons:
        if item not in seen:
            seen.add(item)
            unique_reasons.append(item)

    score = round(min(score, 0.99), 2)
    return {
        "score": score,
        "reasons": unique_reasons,
        "is_probable_noise": score >= 0.45,
    }


def assess_discovery_noise(
    *,
    entity_type: str,
    canonical_name: str,
    attributes: dict | None = None,
) -> dict:
    """Avalia ruído para listas públicas de descoberta, mais conservador que a curadoria."""

    normalized = normalize_name(canonical_name)
    tokens = [token for token in normalized.split() if token]
    token_set = set(tokens)
    attrs = attributes or {}
    reasons: list[str] = []
    score = 0.0

    if entity_type == "person":
        base = assess_entity_noise(entity_type=entity_type, canonical_name=canonical_name, attributes=attrs)
        reasons.extend(base["reasons"])
        score = max(score, base["score"])
        if tokens and all(token in PERSON_CONTEXT_ONLY_MARKERS for token in token_set):
            reasons.append("sequência de cargos, instituições ou termos políticos sem nome próprio individualizado")
            score = max(score, 0.93)
        return {
            "score": round(min(score, 0.99), 2),
            "reasons": list(dict.fromkeys(reasons)),
            "is_probable_noise": score >= 0.55,
        }

    exact_reason = DISCOVERY_FALSE_POSITIVE_PHRASES.get(normalized)
    if exact_reason:
        reasons.append(exact_reason)
        score = max(score, 0.96)

    if len(tokens) == 1 and tokens[0] in DISCOVERY_GENERIC_SINGLE_TOKENS:
        reasons.append("token único genérico ou editorial sem valor investigativo isolado")
        score = max(score, 0.9)

    institution_normalized = {normalize_name(m) for m in INSTITUTION_MARKERS}

    if entity_type == "place":
        if token_set & institution_normalized:
            reasons.append("vocabulário institucional aparecendo como lugar")
            score = max(score, 0.8)
        if token_set & EDITORIAL_STOPWORDS:
            reasons.append("vocabulário editorial aparecendo como lugar")
            score = max(score, 0.88)
        if len(tokens) == 1 and len(tokens[0]) <= 4:
            reasons.append("topônimo muito curto ou fragmentário para orientar pesquisa")
            score = max(score, 0.72)
    elif entity_type == "institution":
        if normalized in {"comp", "companhia do", "companhia da"}:
            reasons.append("instituição truncada ou abreviada em excesso")
            score = max(score, 0.9)
        if len(tokens) == 1 and tokens[0] in {"comp", "art", "dito", "dita"}:
            reasons.append("instituição genérica demais para aparecer em destaque")
            score = max(score, 0.86)

    return {
        "score": round(min(score, 0.99), 2),
        "reasons": list(dict.fromkeys(reasons)),
        "is_probable_noise": score >= 0.6,
    }
