"""Contexto TLS com a cadeia completa dos servidores da Hemeroteca Digital.

O servidor da BN envia **apenas o certificado folha** (`*.bn.gov.br`), sem o
intermediário `Certum DV TLS G2 R39 CA`. O macOS valida mesmo assim, porque tem
esse intermediário no chaveiro do sistema; o Python com o bundle do `certifi`
não, e a conexão falha com:

    [SSL: CERTIFICATE_VERIFY_FAILED] unable to get local issuer certificate

O contorno que existia no projeto era desligar a verificação
(`ssl.CERT_NONE`), o que deixa a conexão aberta a interceptação — inclusive no
download das imagens, que é onde o cookie de sessão da BN viaja.

Este módulo monta um bundle = `certifi` + o intermediário, baixado da URL de
`CA Issuers` do próprio certificado (`certumdvtlsg2r39ca.repository.certum.pl`),
e o guarda em `data/cache/ca_bundle.pem`. O download acontece uma vez; depois o
bundle é reusado.

Se o intermediário não puder ser obtido, a verificação **continua ligada** com o
`certifi` puro e o erro é registrado: falhar alto é melhor que rebaixar a
segurança em silêncio.

Sobrescreva o bundle com a variável `HEMEROTECA_CA_BUNDLE` se a cadeia mudar.
"""

from __future__ import annotations

import base64
import logging
import os
import re
import ssl
import urllib.request
from pathlib import Path

from src.config import CACHE_DIR

logger = logging.getLogger(__name__)

CA_BUNDLE_PATH = CACHE_DIR / "ca_bundle.pem"
_AIA_URL = "http://certumdvtlsg2r39ca.repository.certum.pl/certumdvtlsg2r39ca.cer"
_intermediario_cache: str | None = None


def _pem_do_intermediario() -> str | None:
    """Baixa o intermediário que falta e devolve em PEM. None se não conseguir."""
    global _intermediario_cache
    if _intermediario_cache is not None:
        return _intermediario_cache
    try:
        requisicao = urllib.request.Request(_AIA_URL, headers={"User-Agent": "hemeroteca-pe/0.1"})
        dados = urllib.request.urlopen(requisicao, timeout=30).read()
    except Exception as exc:  # noqa: BLE001
        logger.warning("Não foi possível baixar o intermediário TLS: %s", exc)
        return None

    if b"-----BEGIN CERTIFICATE-----" in dados:
        _intermediario_cache = dados.decode("ascii", "replace")
    else:
        corpo = "\n".join(base64.encodebytes(dados).decode("ascii").split())
        _intermediario_cache = (
            "-----BEGIN CERTIFICATE-----\n" + corpo + "\n-----END CERTIFICATE-----\n"
        )
    return _intermediario_cache


def ca_bundle() -> Path | None:
    """Caminho do bundle com a cadeia completa, criando-o se necessário.

    Devolve None se o bundle não puder ser montado — o chamador deve então cair
    no `certifi` puro, mantendo a verificação ligada.
    """
    variavel = os.environ.get("HEMEROTECA_CA_BUNDLE")
    if variavel:
        caminho = Path(variavel)
        return caminho if caminho.exists() else None

    if CA_BUNDLE_PATH.exists() and CA_BUNDLE_PATH.stat().st_size > 100_000:
        return CA_BUNDLE_PATH

    intermediario = _pem_do_intermediario()
    if not intermediario:
        return None

    try:
        import certifi

        base = Path(certifi.where()).read_text(encoding="utf-8")
    except Exception as exc:  # noqa: BLE001
        logger.warning("certifi indisponível (%s); usando só o intermediário", exc)
        base = ""

    CA_BUNDLE_PATH.parent.mkdir(parents=True, exist_ok=True)
    CA_BUNDLE_PATH.write_text(base + "\n" + intermediario, encoding="utf-8")
    logger.info("Bundle TLS montado em %s", CA_BUNDLE_PATH)
    return CA_BUNDLE_PATH


def ssl_context() -> ssl.SSLContext:
    """Contexto TLS com verificação de certificado **ligada**.

    Usa o bundle com a cadeia completa quando disponível; senão, o padrão do
    sistema com o `certifi`. Nunca desliga a verificação.
    """
    bundle = ca_bundle()
    if bundle:
        return ssl.create_default_context(cafile=str(bundle))
    return ssl.create_default_context()
