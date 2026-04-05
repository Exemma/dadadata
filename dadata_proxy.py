"""
Общая настройка SOCKS для httpx/dadata.

Вызывайте apply_socks_proxy_from_env() до import dadata, чтобы клиент подхватил прокси.
"""

from __future__ import annotations

import os

DEFAULT_SOCKS = "socks5://127.0.0.1:9150"
ENV_KEY = "DADATA_SOCKS_PROXY"


def apply_socks_proxy_from_env() -> str:
    proxy = os.environ.get(ENV_KEY, DEFAULT_SOCKS)
    os.environ["ALL_PROXY"] = proxy
    os.environ["HTTPS_PROXY"] = proxy
    return proxy
