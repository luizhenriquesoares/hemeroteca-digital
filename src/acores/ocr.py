"""OCR de manuscritos paroquiais via GPT-4o Vision."""

from __future__ import annotations

import base64
import io
import json
import logging
from pathlib import Path

import openai
from PIL import Image

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """Você é um paleógrafo especialista em manuscritos portugueses do século XVIII-XIX.
Transcreva integralmente os registros paroquiais, preservando a ortografia original.
Use [?] apenas para palavras verdadeiramente ilegíveis. Nunca recuse a tarefa."""

TRANSCRIPTION_PROMPT = """Esta é uma página de um livro de {record_type} da freguesia de {parish}, {island}, Açores.

TRANSCREVA O TEXTO COMPLETO, linha por linha, com ortografia original.
Preserve abreviações (fr.a, d., etc.).

Após a transcrição, faça uma EXTRAÇÃO ESTRUTURADA em JSON para cada registro:
```json
[
  {{
    "record_type": "{record_type}",
    "event_date": "data do evento",
    "birth_date": "data de nascimento (se diferente)",
    "person_name": "nome do batizado/noivo/falecido",
    "father_name": "nome completo do pai",
    "mother_name": "nome completo da mãe",
    "paternal_grandfather": "avô paterno",
    "paternal_grandmother": "avó paterna",
    "maternal_grandfather": "avô materno",
    "maternal_grandmother": "avó materna",
    "godparents": ["padrinho 1", "madrinha 1"],
    "spouse_name": "cônjuge (se casamento)",
    "priest": "nome do pároco/vigário",
    "parish": "freguesia",
    "place": "lugar específico",
    "notes": "observações adicionais (raça, profissão, etc.)"
  }}
]
```

Transcreva agora:"""


def transcribe_page(
    image_path: Path,
    *,
    record_type: str = "batismo",
    parish: str = "",
    island: str = "São Miguel",
    model: str = "gpt-4o",
) -> dict:
    """Transcreve uma página de registro paroquial.

    Returns:
        {"raw_text": str, "records": list[dict], "tokens": int, "cost": float}
    """
    img = Image.open(image_path)
    w, h = img.size

    results = []
    full_raw = ""

    # Processar cada página (esquerda e direita) se imagem de spread
    sides = []
    if w > h * 1.3:  # imagem com duas páginas
        sides = [
            ("left", img.crop((0, 0, w // 2 + 40, h))),
            ("right", img.crop((w // 2 - 40, 0, w, h))),
        ]
    else:
        sides = [("single", img)]

    client = openai.OpenAI()
    total_tokens = 0

    for side_name, side_img in sides:
        # Ampliar 2x para melhor leitura
        upscaled = side_img.resize(
            (side_img.width * 2, side_img.height * 2), Image.LANCZOS
        )
        buf = io.BytesIO()
        upscaled.save(buf, format="JPEG", quality=95)
        img_b64 = base64.b64encode(buf.getvalue()).decode()

        prompt = TRANSCRIPTION_PROMPT.format(
            record_type=record_type, parish=parish, island=island,
        )

        response = client.chat.completions.create(
            model=model,
            max_tokens=4000,
            temperature=0.05,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/jpeg;base64,{img_b64}",
                                "detail": "high",
                            },
                        },
                        {"type": "text", "text": prompt},
                    ],
                },
            ],
        )

        text = response.choices[0].message.content
        total_tokens += response.usage.prompt_tokens + response.usage.completion_tokens
        full_raw += f"\n--- {side_name} ---\n{text}\n"

        # Extrair JSON estruturado do response
        records = _extract_json_records(text)
        results.extend(records)

    cost = (total_tokens / 1_000_000) * 7.50  # média input+output
    return {
        "raw_text": full_raw.strip(),
        "records": results,
        "tokens": total_tokens,
        "cost": round(cost, 4),
    }


def transcribe_collection(
    collection_dir: Path,
    output_dir: Path,
    *,
    metadata: dict | None = None,
    model: str = "gpt-4o",
    force: bool = False,
) -> dict:
    """Transcreve todas as páginas de uma coleção.

    Returns:
        {"pages_processed": int, "records_extracted": int, "total_cost": float}
    """
    meta = metadata or {}
    record_type = meta.get("record_type", "batismo")
    parish = meta.get("parish", "")
    island = meta.get("island", "São Miguel")

    output_dir.mkdir(parents=True, exist_ok=True)
    images = sorted(collection_dir.glob("*.jpg"))

    pages_processed = 0
    total_records = 0
    total_cost = 0.0
    all_records = []

    for img_path in images:
        page_id = img_path.stem
        out_file = output_dir / f"{page_id}.json"

        if out_file.exists() and not force:
            # Carregar registros existentes
            existing = json.loads(out_file.read_text(encoding="utf-8"))
            all_records.extend(existing.get("records", []))
            total_records += len(existing.get("records", []))
            continue

        try:
            result = transcribe_page(
                img_path,
                record_type=record_type,
                parish=parish,
                island=island,
                model=model,
            )
            # Salvar resultado individual
            out_data = {
                "page_id": page_id,
                "image": img_path.name,
                "raw_text": result["raw_text"],
                "records": result["records"],
                "tokens": result["tokens"],
                "cost": result["cost"],
            }
            out_file.write_text(
                json.dumps(out_data, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            all_records.extend(result["records"])
            pages_processed += 1
            total_records += len(result["records"])
            total_cost += result["cost"]
            logger.info(
                "Página %s: %d registros (custo $%.4f)",
                page_id, len(result["records"]), result["cost"],
            )
        except Exception as exc:
            logger.error("Erro na página %s: %s", page_id, exc)

    # Salvar consolidado
    consolidated = output_dir / "all_records.json"
    consolidated.write_text(
        json.dumps(all_records, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    return {
        "pages_processed": pages_processed,
        "records_extracted": total_records,
        "total_cost": round(total_cost, 4),
    }


def _extract_json_records(text: str) -> list[dict]:
    """Extrai registros JSON do texto do GPT-4o."""
    import re

    # Procurar bloco JSON no texto
    json_match = re.search(r"```json\s*(\[.*?\])\s*```", text, re.DOTALL)
    if json_match:
        try:
            return json.loads(json_match.group(1))
        except json.JSONDecodeError:
            pass

    # Tentar encontrar array JSON sem backticks
    json_match = re.search(r"(\[\s*\{.*?\}\s*\])", text, re.DOTALL)
    if json_match:
        try:
            return json.loads(json_match.group(1))
        except json.JSONDecodeError:
            pass

    # Fallback: extrair manualmente dos campos
    records = []
    sections = re.split(r"(?:Registro|Record)\s*\d+", text, flags=re.IGNORECASE)
    for section in sections[1:]:
        record = {}
        for field, pattern in [
            ("person_name", r"(?:Nome do batizado|Nome):\s*(.+)"),
            ("father_name", r"(?:Pai|Nome do pai):\s*(.+)"),
            ("mother_name", r"(?:Mãe|Nome da mãe):\s*(.+)"),
            ("event_date", r"(?:Data|Data do batismo):\s*(.+)"),
            ("priest", r"(?:Pároco|Vigário):\s*(.+)"),
            ("parish", r"(?:Freguesia|Parish):\s*(.+)"),
            ("place", r"(?:Lugar|Local|Observações):\s*(.+)"),
        ]:
            match = re.search(pattern, section, re.IGNORECASE)
            if match:
                record[field] = match.group(1).strip().rstrip("*").strip()
        if record.get("person_name"):
            records.append(record)

    return records
