#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

install -d -m 0755 /etc/rancher/k3s
if ! cmp -s "$SCRIPT_DIR/k3s-config.yaml" /etc/rancher/k3s/config.yaml; then
  install -m 0644 "$SCRIPT_DIR/k3s-config.yaml" /etc/rancher/k3s/config.yaml
  systemctl restart k3s
  sleep 10
fi

apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y nginx certbot

install -d -m 0755 /var/www/certbot /etc/nginx/sites-available /etc/nginx/sites-enabled

if [ ! -f /etc/letsencrypt/live/api.fableim.lol/fullchain.pem ]; then
  cp "$SCRIPT_DIR/fable-api-http.conf" /etc/nginx/sites-available/fable-api.conf
  ln -sfn /etc/nginx/sites-available/fable-api.conf /etc/nginx/sites-enabled/fable-api.conf
  rm -f /etc/nginx/sites-enabled/default
  nginx -t
  systemctl enable --now nginx
  systemctl reload nginx
  certbot certonly --webroot -w /var/www/certbot -d api.fableim.lol \
    --non-interactive --agree-tos --register-unsafely-without-email
fi

cp "$SCRIPT_DIR/fable-api.conf" /etc/nginx/sites-available/fable-api.conf
ln -sfn /etc/nginx/sites-available/fable-api.conf /etc/nginx/sites-enabled/fable-api.conf
rm -f /etc/nginx/sites-enabled/default
nginx -t
systemctl enable --now nginx
systemctl reload nginx
systemctl enable --now certbot.timer
