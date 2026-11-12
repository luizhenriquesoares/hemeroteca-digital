#!/usr/bin/env bash
# Roda NA VPS como root (o enviar.sh do Mac chama este script). Idempotente.
#   DOMINIO=painel.foxymilhas.com.br bash instalar.sh
set -euo pipefail
DOMINIO="${DOMINIO:?defina DOMINIO}"
APP=/opt/hemeroteca/app
CADDY_CONF=/opt/foxymilhas-landing/conf/Caddyfile
CADDY_CT=foxymilhas-landing

echo "== 1/6 pacotes do sistema"
export DEBIAN_FRONTEND=noninteractive
PACOTES="python3-venv python3-tesserocr tesseract-ocr tesseract-ocr-eng tesseract-ocr-por curl"
# só mexe no apt se faltar algo; se outro apt (unattended-upgrades) segura o lock, espera até 10 min
if ! dpkg -s $PACOTES >/dev/null 2>&1; then
  for i in $(seq 1 120); do
    fuser /var/lib/dpkg/lock-frontend /var/lib/dpkg/lock /var/lib/apt/lists/lock >/dev/null 2>&1 || break
    [ "$i" = 1 ] && echo "   outro apt esta rodando, esperando o lock..."
    sleep 5
  done
  apt-get -o DPkg::Lock::Timeout=600 update -qq
  apt-get -o DPkg::Lock::Timeout=600 install -y -qq $PACOTES >/dev/null
fi

echo "== 2/6 usuario hemeroteca e venv"
id hemeroteca >/dev/null 2>&1 || useradd --system --home /opt/hemeroteca --shell /bin/bash hemeroteca
mkdir -p /opt/hemeroteca /etc/hemeroteca
chown -R hemeroteca:hemeroteca /opt/hemeroteca
if [ ! -x /opt/hemeroteca/venv/bin/python ]; then
  sudo -u hemeroteca python3 -m venv --system-site-packages /opt/hemeroteca/venv
fi
sudo -u hemeroteca /opt/hemeroteca/venv/bin/pip install -q --upgrade pip
sudo -u hemeroteca /opt/hemeroteca/venv/bin/pip install -q -r "$APP/deploy/painel/requirements-painel.txt"
# o OCR do CAPTCHA procura eng.traineddata em data/tessdata (src/config.py: TESSDATA_DIR)
SYS_TESSDATA=$(dirname "$(ls /usr/share/tesseract-ocr/*/tessdata/eng.traineddata | sort -V | tail -1)")
sudo -u hemeroteca mkdir -p "$APP/data/tessdata"
for L in eng por; do
  [ -s "$APP/data/tessdata/$L.traineddata" ] || sudo -u hemeroteca cp "$SYS_TESSDATA/$L.traineddata" "$APP/data/tessdata/"
done

echo "== 3/6 Claude Code (leitura pela assinatura)"
# o painel.service procura o claude em /opt/hemeroteca/.local/bin
if [ ! -x /opt/hemeroteca/.local/bin/claude ]; then
  sudo -u hemeroteca -i bash -lc 'curl -fsSL https://claude.ai/install.sh | bash'
fi
[ -x /opt/hemeroteca/.local/bin/claude ] || { echo "ERRO: claude nao instalado em /opt/hemeroteca/.local/bin"; exit 1; }

echo "== 4/6 senha do painel"
if [ ! -f /etc/hemeroteca/painel.env ]; then
  SENHA=$(python3 -c 'import secrets; print(secrets.token_urlsafe(18))')
  printf 'PAINEL_SENHA=%s\n' "$SENHA" > /etc/hemeroteca/painel.env
  NOVA_SENHA="$SENHA"
fi
chmod 600 /etc/hemeroteca/painel.env; chown root:root /etc/hemeroteca/painel.env

echo "== 5/6 servico systemd"
install -m 644 "$APP/deploy/painel/painel.service" /etc/systemd/system/hemeroteca-painel.service
systemctl daemon-reload
systemctl enable --now hemeroteca-painel >/dev/null
systemctl restart hemeroteca-painel
for i in $(seq 1 30); do
  curl -s -o /dev/null http://172.16.0.1:8765/login && break; sleep 1
done
curl -sf -o /dev/null http://172.16.0.1:8765/login || { journalctl -u hemeroteca-painel -n 40 --no-pager; echo "ERRO: painel nao subiu"; exit 1; }
echo "   painel respondendo em 172.16.0.1:8765"

echo "== 6/6 Caddy do foxymilhas-landing"
if command -v ufw >/dev/null && ufw status | grep -q "Status: active"; then
  ufw allow in on docker0 to 172.16.0.1 port 8765 proto tcp >/dev/null
fi
if ! docker exec "$CADDY_CT" wget -q -O /dev/null http://172.16.0.1:8765/login; then
  echo "ERRO: o container do Caddy nao alcanca o painel em 172.16.0.1:8765 (firewall?). Nada mudou no foxymilhas."; exit 1
fi
if grep -q "^$DOMINIO {" "$CADDY_CONF"; then
  echo "   bloco de $DOMINIO ja existe, nada a mudar"
else
  BACKUP="$CADDY_CONF.antes-painel-$(date +%Y%m%d-%H%M%S)"
  cp "$CADDY_CONF" "$BACKUP"
  # o bloco entra ANTES do 'http://' generico
  python3 - "$CADDY_CONF" "$DOMINIO" <<'PY'
import sys
path, dom = sys.argv[1], sys.argv[2]
text = open(path).read()
block = f"""{dom} {{
\treverse_proxy 172.16.0.1:8765
\theader {{
\t\tStrict-Transport-Security "max-age=31536000"
\t\tX-Content-Type-Options "nosniff"
\t\tX-Frame-Options "DENY"
\t\tReferrer-Policy "same-origin"
\t\t-Server
\t}}
}}

"""
marker = "# Acesso pelo IP"
i = text.find(marker)
if i < 0:
    i = text.find("http:// {")
text = text[:i] + block + text[i:] if i >= 0 else text.rstrip() + "\n\n" + block
open(path, "w").write(text)
PY
  if docker exec "$CADDY_CT" caddy validate --config /etc/caddy/Caddyfile --adapter caddyfile >/dev/null 2>&1; then
    docker exec "$CADDY_CT" caddy reload --config /etc/caddy/Caddyfile --adapter caddyfile
    echo "   Caddy recarregado (backup em $BACKUP)"
  else
    cp "$BACKUP" "$CADDY_CONF"
    docker exec "$CADDY_CT" caddy validate --config /etc/caddy/Caddyfile --adapter caddyfile || true
    echo "ERRO: configuracao do Caddy invalida, restaurei o backup. Nada mudou no foxymilhas."; exit 1
  fi
fi

echo
echo "PRONTO: https://$DOMINIO"
if [ -n "${NOVA_SENHA:-}" ]; then
  echo "SENHA DO PAINEL (guarde agora, ela fica em /etc/hemeroteca/painel.env): $NOVA_SENHA"
fi
echo "Falta: sudo -u hemeroteca -i claude   (faça o login com sua conta e saia com /exit)"
