"""Schema estruturado de uma página de jornal lida por modelo de visão.

Usado como `output_format` (saídas estruturadas) na chamada ao Claude e como
contrato de persistência. Mantido deliberadamente simples: tipos primitivos,
listas, enums e opcionais, sem constraints que o schema estrito não suporta.
"""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field


class SegmentType(str, Enum):
    EDITAL = "edital"
    ATO_GOVERNO = "ato_governo"
    NOMEACAO = "nomeacao"
    SENTENCA_JUDICIAL = "sentenca_judicial"
    FUGA_ESCRAVIZADO = "fuga_escravizado"
    VENDA_ESCRAVIZADO = "venda_escravizado"
    MOVIMENTO_PORTO = "movimento_porto"
    OBITO = "obito"
    LEILAO = "leilao"
    ANUNCIO = "anuncio"
    CORRESPONDENCIA = "correspondencia"
    NOTICIA = "noticia"
    OUTRO = "outro"


class RelationPredicate(str, Enum):
    SPOUSE_OF = "spouse_of"
    CHILD_OF = "child_of"
    PARENT_OF = "parent_of"
    WIDOW_OF = "widow_of"
    HOLDS_ROLE = "holds_role"
    APPOINTED_TO = "appointed_to"
    MEMBER_OF = "member_of"
    RESIDENT_OF = "resident_of"
    SLAVE_OF = "slave_of"
    ACCUSED_OF = "accused_of"
    VICTIM_OF = "victim_of"
    SENTENCED_TO = "sentenced_to"
    ABSOLVED = "absolved"
    DECEASED = "deceased"
    TRAVELED_ON = "traveled_on"
    SIGNED_BY = "signed_by"
    CREDITOR_OF = "creditor_of"
    DEBTOR_OF = "debtor_of"
    OWNER_OF = "owner_of"
    OTHER = "other"


class PersonMention(BaseModel):
    name: str = Field(description="Nome completo como impresso, ortografia original")
    role: str | None = Field(
        default=None,
        description="Cargo, título ou condição (Capitão, viúva, escravo, réu, negociante)",
    )
    context: str | None = Field(
        default=None,
        description="Papel neste segmento (réu, vítima, dono, fugitivo, passageiro, signatário)",
    )


class Relation(BaseModel):
    subject: str
    predicate: RelationPredicate
    object: str = Field(description="Outra pessoa, instituição, lugar, embarcação ou literal")
    quote: str = Field(description="Trecho literal curto da fonte que sustenta a relação")


class Segment(BaseModel):
    type: SegmentType
    title: str | None = Field(default=None, description="Cabeçalho ou primeira linha identificadora")
    text: str = Field(
        description=(
            "Transcrição do segmento: integral no modo 'full'; "
            "apenas as primeiras linhas (até ~300 caracteres) no modo 'structure'"
        )
    )
    date_text: str | None = Field(default=None, description="Data como aparece no segmento")
    people: list[PersonMention] = Field(default_factory=list)
    institutions: list[str] = Field(default_factory=list)
    places: list[str] = Field(default_factory=list)
    ships: list[str] = Field(default_factory=list)
    relations: list[Relation] = Field(default_factory=list)
    tiles: list[int] = Field(
        default_factory=list,
        description="Índices dos recortes (tiles) em que o segmento aparece",
    )
    confidence: float = Field(default=0.7, description="0 a 1: legibilidade e certeza da leitura")


class PageExtraction(BaseModel):
    newspaper: str | None = Field(default=None, description="Título do periódico no cabeçalho")
    issue_date: str | None = Field(default=None, description="Data da edição como impressa")
    issue_number: str | None = Field(default=None)
    page_number: str | None = Field(default=None)
    columns: int | None = Field(default=None, description="Número de colunas do layout")
    segments: list[Segment] = Field(default_factory=list)
    illegible_notes: str | None = Field(default=None, description="Áreas ilegíveis, manchas, cortes")

    def full_text(self) -> str:
        return "\n\n".join(segment.text for segment in self.segments if segment.text)
