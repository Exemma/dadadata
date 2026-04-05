"""
Проверка баланса DaData через локальный SOCKS5 (Tor Browser по умолчанию — 9150).

Установка:
  pip install dadata "httpx[socks]"

Запуск (cmd):
  set DADATA_TOKEN=ваш_api_ключ
  set DADATA_SECRET=ваш_secret
  python check_dadata_balance.py

PowerShell:
  $env:DADATA_TOKEN="ваш_api_ключ"
  $env:DADATA_SECRET="ваш_secret"
  python check_dadata_balance.py

Другой порт/хост прокси: переменная DADATA_SOCKS_PROXY, например socks5://127.0.0.1:9050
"""

from __future__ import annotations

import os
import sys

from dadata_proxy import apply_socks_proxy_from_env

apply_socks_proxy_from_env()

from dadata import Dadata  # noqa: E402


def main() -> None:
    token = os.environ.get("DADATA_TOKEN")
    if not token:
        print("Задайте переменную окружения DADATA_TOKEN.", file=sys.stderr)
        sys.exit(1)

    secret = os.environ.get("DADATA_SECRET") or None

    try:
        with Dadata(token, secret, timeout=30) as dadata:
            balance = dadata.get_balance()
    except Exception as e:
        print(f"Ошибка запроса: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Баланс: {balance}")


if __name__ == "__main__":
    main()
