"""
Стандартизация адресов через DaData Cleaner API (GUI на Tkinter).

Установка: pip install -r requirements.txt

Ключи задаются в окне (API-ключ и Secret — со звёздочками); при старте
поля заполняются из DADATA_TOKEN, DADATA_SECRET, YANDEX_GEOCODER_API_KEY,
YANDEX_DEVELOPER_AUTH_KEY (X-Auth-Key API кабинета разработчика), если заданы.
SOCKS5 настраивается кнопкой «Настроить прокси» (хост, порт, логин, пароль) и сохраняется в профиле;
при заданной DADATA_SOCKS_PROXY стартовые поля диалога можно подтянуть из URL.
API-ключ, Secret, ключи Яндекса и параметры прокси при выходе/по «ОК» в диалоге сохраняются в профиле
(не в папку программы), чтобы при передаче копии приложения чужие данные не ехали вместе с файлами.
Галка «Через SOCKS5» по умолчанию выключена; DADATA_USE_PROXY=1 включает использование прокси при старте.
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote, unquote, urlparse

import httpx

from dadata_proxy import apply_socks_proxy_from_env

apply_socks_proxy_from_env()

from dadata import Dadata  # noqa: E402

import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext, ttk

from openpyxl import Workbook

REQUEST_TIMEOUT = 30


def _local_credentials_file() -> Path:
    """Файл только в профиле пользователя — не рядом с кодом/портативной копией."""
    if sys.platform == "win32":
        base = os.environ.get("LOCALAPPDATA")
        if not base:
            base = str(Path.home() / "AppData" / "Local")
        return Path(base) / "DaDataAddressCleaner" / "api_credentials.json"
    xdg = os.environ.get("XDG_CONFIG_HOME")
    if xdg:
        return Path(xdg) / "dadadata-address-cleaner" / "api_credentials.json"
    return Path.home() / ".config" / "dadadata-address-cleaner" / "api_credentials.json"


def _read_credentials_raw() -> Dict[str, Any]:
    path = _local_credentials_file()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError, TypeError, UnicodeError):
        return {}


def _load_saved_api_credentials() -> Tuple[str, str, str, str]:
    data = _read_credentials_raw()
    token_v = data.get("token")
    secret_v = data.get("secret")
    yandex_v = data.get("yandex_api_key")
    dev_v = data.get("yandex_developer_auth_key")
    token = str(token_v).strip() if token_v is not None else ""
    secret = str(secret_v).strip() if secret_v is not None else ""
    yandex = str(yandex_v).strip() if yandex_v is not None else ""
    dev = str(dev_v).strip() if dev_v is not None else ""
    return token, secret, yandex, dev


def _parse_socks_url(url: str) -> Tuple[str, str, str, str]:
    """Из socks5://[user:pass@]host:port в (host, port, user, password)."""
    u = url.strip()
    if not u or "://" not in u:
        return "127.0.0.1", "9150", "", ""
    parsed = urlparse(u)
    host = (parsed.hostname or "").strip() or "127.0.0.1"
    port = str(parsed.port) if parsed.port is not None else "9150"
    user = unquote(parsed.username) if parsed.username else ""
    password = unquote(parsed.password) if parsed.password else ""
    return host, port, user, password


def _build_socks5_url(host: str, port: str, user: str, password: str) -> str:
    h = host.strip() or "127.0.0.1"
    p = (port.strip() or "9150").lstrip(":") or "9150"
    u, pw = user.strip(), password.strip()
    if u or pw:
        return f"socks5://{quote(u, safe='')}:{quote(pw, safe='')}@{h}:{p}"
    return f"socks5://{h}:{p}"


def _load_saved_socks_proxy() -> Tuple[str, str, str, str]:
    data = _read_credentials_raw()
    sp = data.get("socks_proxy")
    if isinstance(sp, dict):
        host = str(sp.get("host") or "").strip() or "127.0.0.1"
        port = str(sp.get("port") or "").strip() or "9150"
        user = str(sp.get("user") or "").strip()
        password = str(sp.get("password") or "").strip()
        return host, port, user, password
    if "DADATA_SOCKS_PROXY" in os.environ:
        return _parse_socks_url(str(os.environ.get("DADATA_SOCKS_PROXY", "")))
    return "127.0.0.1", "9150", "", ""


def _save_socks_proxy_to_file(host: str, port: str, user: str, password: str) -> None:
    path = _local_credentials_file()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        existing = _read_credentials_raw()
        existing["socks_proxy"] = {
            "host": host.strip() or "127.0.0.1",
            "port": (port.strip() or "9150").lstrip(":") or "9150",
            "user": user.strip(),
            "password": password.strip(),
        }
        path.write_text(json.dumps(existing, ensure_ascii=False, indent=2), encoding="utf-8")
        try:
            path.chmod(0o600)
        except OSError:
            pass
    except OSError:
        pass


def _save_api_credentials(
    token: str, secret: str, yandex_api_key: str, yandex_developer_auth_key: str
) -> None:
    path = _local_credentials_file()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        existing = _read_credentials_raw()
        existing["token"] = token.strip()
        existing["secret"] = secret.strip()
        existing["yandex_api_key"] = yandex_api_key.strip()
        existing["yandex_developer_auth_key"] = yandex_developer_auth_key.strip()
        path.write_text(json.dumps(existing, ensure_ascii=False, indent=2), encoding="utf-8")
        try:
            path.chmod(0o600)
        except OSError:
            pass
    except OSError:
        pass


def _env_or_saved(env_key: str, saved: str) -> str:
    """Если переменная задана в окружении (в т.ч. пустая строка) — берём её, иначе сохранённое."""
    if env_key in os.environ:
        return str(os.environ.get(env_key, ""))
    return saved


# Палитра (светлая тема)
COL_BG = "#eef1f5"
COL_CARD = "#ffffff"
COL_BORDER = "#d8dee9"
COL_TEXT = "#1e293b"
COL_MUTED = "#64748b"
COL_ACCENT = "#0d9488"
COL_ACCENT_HOVER = "#0f766e"
COL_ACCENT_DIM = "#ccfbf1"
COL_BTN_SECONDARY = "#e2e8f0"
COL_BTN_SECONDARY_HOVER = "#cbd5e1"


def _apply_theme(root: tk.Tk) -> ttk.Style:
    root.configure(bg=COL_BG)
    style = ttk.Style(root)
    try:
        style.theme_use("clam")
    except tk.TclError:
        pass

    style.configure("App.TFrame", background=COL_BG)
    style.configure("Card.TLabelframe", background=COL_CARD, relief="solid", borderwidth=1)
    style.configure(
        "Card.TLabelframe.Label",
        background=COL_CARD,
        foreground=COL_TEXT,
        font=("Segoe UI", 10, "bold"),
    )
    style.configure("Muted.TLabel", background=COL_BG, foreground=COL_MUTED, font=("Segoe UI", 9))
    style.configure("Status.TLabel", background=COL_CARD, foreground=COL_MUTED, font=("Segoe UI", 9))

    style.configure(
        "Primary.TButton",
        font=("Segoe UI", 10, "bold"),
        padding=(22, 11),
        background=COL_ACCENT,
        foreground="#ffffff",
        borderwidth=0,
        focuscolor=COL_ACCENT,
    )
    style.map(
        "Primary.TButton",
        background=[("active", COL_ACCENT_HOVER), ("disabled", "#94a3b8")],
        foreground=[("disabled", "#f1f5f9")],
    )

    style.configure(
        "Secondary.TButton",
        font=("Segoe UI", 9),
        padding=(16, 9),
        background=COL_BTN_SECONDARY,
        foreground=COL_TEXT,
        borderwidth=0,
        focuscolor=COL_BTN_SECONDARY,
    )
    style.map(
        "Secondary.TButton",
        background=[("active", COL_BTN_SECONDARY_HOVER)],
    )

    style.configure("TSeparator", background=COL_BORDER)
    style.configure(
        "Card.TEntry",
        fieldbackground="#ffffff",
        foreground=COL_TEXT,
        insertcolor=COL_ACCENT,
    )
    style.configure(
        "Card.TCheckbutton",
        background=COL_CARD,
        foreground=COL_TEXT,
        font=("Segoe UI", 9),
        focuscolor=COL_CARD,
        padding=(2, 4),
    )
    style.map(
        "Card.TCheckbutton",
        background=[("active", COL_CARD), ("!disabled", COL_CARD), ("disabled", COL_CARD)],
        foreground=[("disabled", COL_MUTED)],
        indicatorcolor=[
            ("selected pressed", COL_ACCENT_HOVER),
            ("selected", COL_ACCENT),
            ("pressed", COL_BTN_SECONDARY_HOVER),
            ("!selected", "#ffffff"),
        ],
    )
    return style


def clear_proxy_env() -> None:
    """Убирает прокси из окружения, чтобы httpx ходил напрямую."""
    for key in ("ALL_PROXY", "HTTPS_PROXY", "HTTP_PROXY"):
        os.environ.pop(key, None)


def apply_proxy_from_value(raw: str) -> str:
    """Обновляет ALL_PROXY/HTTPS_PROXY для httpx. Возвращает итоговый URL."""
    p = raw.strip()
    if not p:
        p = "socks5://127.0.0.1:9150"
    if "://" not in p:
        p = f"socks5://{p}"
    os.environ["ALL_PROXY"] = p
    os.environ["HTTPS_PROXY"] = p
    return p


def _env_flag_enabled(name: str, default: bool = True) -> bool:
    raw = os.environ.get(name)
    if raw is None or not str(raw).strip():
        return default
    return str(raw).strip().lower() not in ("0", "false", "no", "off", "")


# Подписи к ключам services/remaining в ответе profile/daily (DaData API)
STATS_SERVICE_LABELS: Dict[str, str] = {
    "clean": "Нормализация (clean)",
    "suggestions": "Подсказки (suggestions)",
    "company": "Организации (company)",
    "company_similar": "Похожие компании (company_similar)",
    "merging": "Объединение дублей (merging)",
}


def _stats_service_title(key: str) -> str:
    return STATS_SERVICE_LABELS.get(key, key)


def _format_stats_scalar(v: Any) -> str:
    if v is None:
        return "—"
    if isinstance(v, float) and v == int(v):
        return str(int(v))
    return str(v)


def format_daily_stats_human(data: Any) -> str:
    """Преобразует ответ get_daily_stats в читаемый текст."""
    if not isinstance(data, dict):
        return _format_stats_scalar(data)

    lines: List[str] = []

    date_v = data.get("date")
    if date_v is not None:
        lines.append(f"Дата: {date_v}")
        lines.append("")

    services = data.get("services")
    remaining = data.get("remaining")

    if isinstance(services, dict) and isinstance(remaining, dict):
        all_keys = sorted(set(services.keys()) | set(remaining.keys()))
        if all_keys:
            lines.append("Лимиты по услугам")
            lines.append("─" * 44)
            for key in all_keys:
                title = _stats_service_title(key)
                u = services.get(key)
                r = remaining.get(key)
                lines.append(title)
                lines.append(f"  использовано за день: {_format_stats_scalar(u)}")
                lines.append(f"  остаток лимита:      {_format_stats_scalar(r)}")
                lines.append("")
    elif isinstance(services, dict):
        lines.append("Использовано за день")
        lines.append("─" * 44)
        for key in sorted(services.keys()):
            lines.append(f"{_stats_service_title(key)}: {_format_stats_scalar(services[key])}")
        lines.append("")
    elif isinstance(remaining, dict):
        lines.append("Остаток лимита")
        lines.append("─" * 44)
        for key in sorted(remaining.keys()):
            lines.append(f"{_stats_service_title(key)}: {_format_stats_scalar(remaining[key])}")
        lines.append("")

    rest_shown = False
    for key, value in data.items():
        if key in ("date", "services", "remaining"):
            continue
        if not rest_shown:
            lines.append("Дополнительные поля")
            lines.append("─" * 44)
            rest_shown = True
        if isinstance(value, (dict, list)):
            lines.append(f"{key}:")
            lines.append(json.dumps(value, ensure_ascii=False, indent=2, default=str))
        else:
            lines.append(f"{key}: {_format_stats_scalar(value)}")

    return "\n".join(lines).rstrip() or "(пустой ответ)"


def _pick(d: Dict[str, Any], *keys: str) -> str:
    for k in keys:
        v = d.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return "—"


def format_short_line(data: Optional[Dict[str, Any]]) -> str:
    if not data:
        return "—"
    result = data.get("result")
    if result is None or not str(result).strip():
        return "—"
    return str(result).strip()


def format_clean_block(source: str, data: Optional[Dict[str, Any]]) -> str:
    if not data:
        return (
            f"Исходник:\n{source}\n\n"
            "Пустой ответ от API.\n"
        )
    result = data.get("result")
    result_line = str(result).strip() if result is not None else "—"
    lines = [
        f"Исходник:\n{source}\n",
        f"Результат:\n{result_line}\n",
        "Поля:",
        f"  Индекс: {_pick(data, 'postal_code')}",
        f"  Регион: {_pick(data, 'region_with_type', 'region')}",
        f"  Город: {_pick(data, 'city_with_type', 'city')}",
        f"  Улица: {_pick(data, 'street_with_type', 'street')}",
        f"  Дом: {_pick(data, 'house')}",
        f"  Квартира: {_pick(data, 'flat')}",
        "",
    ]
    return "\n".join(lines)


def format_error_block(source: str, err: BaseException) -> str:
    return (
        f"Исходник:\n{source}\n\n"
        f"Ошибка:\n{err!s}\n"
    )


# Столбцы Excel для DaData (после «исходный» и «стандартизированный»).
DADATA_EXCEL_EXTRA: List[Tuple[str, Tuple[str, ...]]] = [
    ("Индекс", ("postal_code",)),
    ("Страна", ("country",)),
    ("Регион", ("region_with_type", "region")),
    ("Район", ("area_with_type", "area")),
    ("Город", ("city_with_type", "city")),
    ("Населённый пункт", ("settlement_with_type", "settlement")),
    ("Улица", ("street_with_type", "street")),
    ("Дом", ("house",)),
    ("Корпус / строение", ("block", "building")),
    ("Квартира", ("flat",)),
]


def dadata_excel_headers() -> List[str]:
    return ["Исходный адрес", "Стандартизированный"] + [t[0] for t in DADATA_EXCEL_EXTRA]


def _dadata_excel_row(source: str, data: Optional[Dict[str, Any]], error: Optional[BaseException]) -> List[str]:
    if error is not None:
        return [source, f"Ошибка: {error!s}"] + [""] * len(DADATA_EXCEL_EXTRA)
    if not data:
        return [source, "—"] + ["—"] * len(DADATA_EXCEL_EXTRA)
    std = format_short_line(data)
    extras = [_pick(data, *keys) for _label, keys in DADATA_EXCEL_EXTRA]
    return [source, std] + extras


YANDEX_EXCEL_HEADERS = ["Исходный адрес", "Адрес (Яндекс)", "Координаты"]


def _write_xlsx(path: str, sheet_title: str, headers: List[str], rows: List[List[str]]) -> None:
    wb = Workbook()
    ws = wb.active
    ws.title = sheet_title[:31] if sheet_title else "Лист1"
    ws.append(headers)
    n = len(headers)
    for r in rows:
        padded = list(r[:n]) + [""] * max(0, n - len(r))
        ws.append(padded)
    wb.save(path)


# Актуальный endpoint (см. https://yandex.ru/dev/geocode/doc/ru/quickstart.html).
# Путь /1.x/ — устаревший; ключи из нового кабинета с ним часто дают 403.
YANDEX_GEOCODE_URL = "https://geocode-maps.yandex.ru/v1/"
YANDEX_GEOCODE_PAUSE_SEC = 0.35

YANDEX_403_HINT = (
    "403 у геокодера Яндекса часто из‑за неверного адреса API: для ключей из кабинета "
    "нужен путь …/v1/, а не …/1.x/ (старый URL не принимает новые ключи).\n"
    "Также проверьте: продукт ключа «JavaScript API и HTTP Геокодер», ограничения ключа, баланс; "
    "активация ключа до ~15 минут после создания."
)


def _format_yandex_request_error(err: BaseException) -> str:
    if isinstance(err, httpx.HTTPStatusError):
        lines = [str(err)]
        try:
            body = (err.response.text or "").strip()
            if body:
                lines.append("Тело ответа:")
                lines.append(body[:1500] + ("…" if len(body) > 1500 else ""))
        except OSError:
            pass
        if err.response.status_code == 403:
            lines.append("")
            lines.append(YANDEX_403_HINT)
        return "\n".join(lines)
    return str(err)


def _http_error_with_body(err: BaseException) -> str:
    if isinstance(err, httpx.HTTPStatusError):
        lines = [str(err)]
        try:
            body = (err.response.text or "").strip()
            if body:
                lines.append("Тело ответа:")
                lines.append(body[:2000] + ("…" if len(body) > 2000 else ""))
        except OSError:
            pass
        return "\n".join(lines)
    return str(err)


YANDEX_DEVELOPER_API_BASE = "https://api-developer.tech.yandex.net"

YANDEX_DEV_SKIP_KEYS = frozenset({"self", "href", "url", "uri", "link"})

YANDEX_DEV_FIELD_LABELS: Dict[str, str] = {
    "limit": "Лимит",
    "value": "Израсходовано",
    "dailyLimit": "Суточный лимит",
    "dailyValue": "Использовано за сутки",
    "used": "Израсходовано",
    "remaining": "Остаток",
    "resetAt": "Сброс лимита",
    "resetTime": "Время сброса",
    "period": "Период",
    "type": "Тип",
    "name": "Название",
    "title": "Название",
    "description": "Описание",
    "id": "ID",
    "serviceId": "ID сервиса",
    "projectId": "ID проекта",
    "slug": "Код",
    "quota": "Квота",
    "count": "Количество",
    "total": "Всего",
    "free": "Бесплатная часть",
    "paid": "Платная часть",
    "enabled": "Включено",
    "status": "Статус",
    "tariff": "Тариф",
    "serviceName": "Сервис",
    "displayName": "Отображаемое имя",
}


def _yandex_dev_pretty_number(v: Any) -> str:
    if v is None:
        return "—"
    if isinstance(v, bool):
        return "да" if v else "нет"
    if isinstance(v, int):
        s = f"{abs(v):,}".replace(",", " ")
        return f"−{s}" if v < 0 else s
    if isinstance(v, float):
        if v == int(v) and abs(v) < 1e12:
            return _yandex_dev_pretty_number(int(v))
        return str(v).replace(".", ",")
    t = str(v).strip()
    return t if t else "—"


def _yandex_dev_field_label(key: str) -> str:
    if key in YANDEX_DEV_FIELD_LABELS:
        return YANDEX_DEV_FIELD_LABELS[key]
    if "_" in key:
        return key.replace("_", " ").strip().capitalize()
    out: List[str] = []
    for i, c in enumerate(key):
        if c.isupper() and i > 0 and key[i - 1].islower():
            out.append(" ")
        out.append(c.lower())
    s = "".join(out)
    return s[:1].upper() + s[1:] if s else key


def _yandex_dev_fallback_summary(payload: Any, heading: str, line_indent: str = "  ") -> List[str]:
    """Краткое текстовое описание ответа без вывода полного JSON."""
    lines = [heading, ""]
    d2 = line_indent + "  "
    if payload is None:
        lines.append(f"{line_indent}(нет данных)")
        return lines
    if isinstance(payload, dict):
        lines.append(f"{line_indent}Структура ответа:")
        keys = sorted(payload.keys())
        for k in keys[:40]:
            v = payload[k]
            lab = _yandex_dev_field_label(k)
            if isinstance(v, list):
                lines.append(f"{line_indent}• {lab}: список ({len(v)} шт.)")
            elif isinstance(v, dict):
                lines.append(f"{line_indent}• {lab}: объект ({len(v)} полей)")
            else:
                lines.append(f"{line_indent}• {lab}: {_yandex_dev_pretty_number(v)}")
        if len(keys) > 40:
            lines.append(f"{line_indent}… и ещё {len(keys) - 40} полей")
        return lines
    if isinstance(payload, list):
        lines.append(f"{line_indent}Список из {len(payload)} элементов.")
        if payload and isinstance(payload[0], dict):
            sample = payload[0]
            lines.append(f"{line_indent}У первого элемента:")
            for k in sorted(sample.keys())[:24]:
                lines.append(f"{d2}— {_yandex_dev_field_label(k)}")
            if len(sample) > 24:
                lines.append(f"{d2}… всего полей: {len(sample)}")
        return lines
    lines.append(f"{line_indent}{_yandex_dev_pretty_number(payload)}")
    return lines


def _yandex_dev_render_limits(lim_raw: Any, base_indent: str = "    ", depth: int = 0) -> List[str]:
    """Человекочитаемый вывод лимитов (без JSON)."""
    lines: List[str] = []
    lw = 22
    max_depth = 8

    def kv(label: str, val: str) -> None:
        lab = label.rstrip(": ") + ":"
        lines.append(f"{base_indent}{lab.ljust(lw + 2)}{val}")

    if depth > max_depth:
        lines.append(f"{base_indent}…")
        return lines

    if lim_raw is None:
        lines.append(f"{base_indent}Нет данных.")
        return lines

    if isinstance(lim_raw, list):
        if not lim_raw:
            lines.append(f"{base_indent}Ограничения не заданы (пустой список).")
            return lines
        for i, item in enumerate(lim_raw, 1):
            lines.append(f"{base_indent}── Запись {i} " + "─" * max(0, 28 - len(str(i))))
            lines.extend(_yandex_dev_render_limits(item, base_indent + "   ", depth + 1))
        return lines

    if isinstance(lim_raw, dict):
        if len(lim_raw) == 1:
            sole_k, sole_v = next(iter(lim_raw.items()))
            if (
                isinstance(sole_v, list)
                and sole_k.lower() in ("limits", "items", "data", "result", "quotas", "services")
            ):
                return _yandex_dev_render_limits(sole_v, base_indent, depth)

        has_lv = "limit" in lim_raw or "value" in lim_raw
        if has_lv:
            lim = lim_raw.get("limit")
            val = lim_raw.get("value")
            kv("Лимит", _yandex_dev_pretty_number(lim))
            kv("Израсходовано", _yandex_dev_pretty_number(val))
            if isinstance(lim, (int, float)) and isinstance(val, (int, float)):
                try:
                    rest = float(lim) - float(val)
                    if rest == int(rest):
                        rest_n: Any = int(rest)
                    else:
                        rest_n = rest
                    kv("Остаток", _yandex_dev_pretty_number(rest_n))
                except (TypeError, ValueError):
                    pass

        for k in sorted(lim_raw.keys()):
            if k in ("limit", "value"):
                continue
            if k in YANDEX_DEV_SKIP_KEYS:
                continue
            v = lim_raw[k]
            lab = _yandex_dev_field_label(k)
            if isinstance(v, dict):
                lines.append(f"{base_indent}{lab}")
                lines.extend(_yandex_dev_render_limits(v, base_indent + "   ", depth + 1))
            elif isinstance(v, list):
                lines.append(f"{base_indent}{lab} ({len(v)} шт.)")
                lines.extend(_yandex_dev_render_limits(v, base_indent + "   ", depth + 1))
            else:
                kv(lab, _yandex_dev_pretty_number(v))
        return lines

    lines.append(f"{base_indent}{_yandex_dev_pretty_number(lim_raw)}")
    return lines


def _yandex_dev_get_json(client: httpx.Client, path: str) -> Any:
    """GET path относительно API кабинета; при 404 дублирует запрос с префиксом /v1."""
    url = f"{YANDEX_DEVELOPER_API_BASE}{path}"
    r = client.get(url)
    if r.status_code == 404 and not path.startswith("/v1"):
        r = client.get(f"{YANDEX_DEVELOPER_API_BASE}/v1{path}")
    r.raise_for_status()
    return r.json()


def _yandex_dev_json_list(payload: Any, *keys: str) -> List[Any]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for k in keys:
            v = payload.get(k)
            if isinstance(v, list):
                return v
    return []


def _yandex_dev_entity_id(obj: Any) -> str:
    if not isinstance(obj, dict):
        return ""
    for k in ("id", "projectId", "serviceId", "uuid"):
        v = obj.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    for inner_key in ("project", "service"):
        inner = obj.get(inner_key)
        if isinstance(inner, dict):
            for k in ("id", "projectId", "serviceId", "uuid"):
                v = inner.get(k)
                if v is not None and str(v).strip():
                    return str(v).strip()
    return ""


def _yandex_dev_entity_title(obj: Any) -> str:
    if not isinstance(obj, dict):
        return ""
    for k in ("name", "title", "serviceName", "displayName", "slug"):
        v = obj.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return ""


def fetch_yandex_developer_limits_report(auth_key: str) -> str:
    """
    Сводка лимитов через API кабинета разработчика (заголовок X-Auth-Key).
    См. документацию сервиса «API кабинета» в developer.tech.yandex.ru.
    """
    headers = {"X-Auth-Key": auth_key.strip()}
    lines: List[str] = []
    lines.append("Лимиты API кабинета Яндекса")
    lines.append(f"Сформировано: {datetime.now().strftime('%d.%m.%Y %H:%M')}")
    lines.append("")
    lines.append("═" * 56)
    lines.append("")

    with httpx.Client(timeout=REQUEST_TIMEOUT, headers=headers) as client:
        try:
            projects_raw = _yandex_dev_get_json(client, "/projects")
        except httpx.HTTPStatusError as e:
            raise RuntimeError(_http_error_with_body(e)) from e

        projects = _yandex_dev_json_list(projects_raw, "projects", "items", "data", "result")
        if not projects:
            lines.extend(
                _yandex_dev_fallback_summary(
                    projects_raw,
                    "Не удалось выделить список проектов в ответе /projects.",
                )
            )
            return "\n".join(lines)

        for proj in projects:
            pid = _yandex_dev_entity_id(proj)
            pname = _yandex_dev_entity_title(proj) or (pid if pid else "Проект")
            lines.append(f"ПРОЕКТ  {pname}")
            if pid:
                lines.append(f"         ID:  {pid}")
            else:
                lines.extend(
                    _yandex_dev_fallback_summary(
                        proj,
                        "         Нет ID проекта — состав объекта:",
                        line_indent="         ",
                    )
                )
            lines.append("  " + "·" * 52)

            if not pid:
                lines.append("")
                continue

            try:
                svcs_raw = _yandex_dev_get_json(client, f"/projects/{pid}/services")
            except httpx.HTTPStatusError as e:
                lines.append("  Не удалось загрузить сервисы:")
                for part in _http_error_with_body(e).splitlines():
                    lines.append(f"    {part}")
                lines.append("")
                continue

            services = _yandex_dev_json_list(svcs_raw, "services", "items", "data", "result")
            if not services:
                lines.extend(
                    _yandex_dev_fallback_summary(
                        svcs_raw,
                        "  Сервисы: ответ не в ожидаемом виде.",
                    )
                )
                lines.append("")
                continue

            for svc in services:
                sid = _yandex_dev_entity_id(svc)
                sname = _yandex_dev_entity_title(svc) or (sid if sid else "Сервис")
                lines.append("")
                lines.append(f"    Сервис:  {sname}")
                if sid:
                    lines.append(f"             ID:  {sid}")
                else:
                    lines.extend(
                        _yandex_dev_fallback_summary(
                            svc,
                            "             Нет ID сервиса — состав объекта:",
                            line_indent="             ",
                        )
                    )
                    lines.append("")
                    continue

                lines.append("    " + "─" * 48)
                try:
                    lim_raw = _yandex_dev_get_json(client, f"/projects/{pid}/services/{sid}/limits")
                except httpx.HTTPStatusError as e:
                    lines.append("    Лимиты недоступны:")
                    for part in _http_error_with_body(e).splitlines():
                        lines.append(f"      {part}")
                    continue
                lines.extend(_yandex_dev_render_limits(lim_raw, "    "))
            lines.append("")
    return "\n".join(lines).rstrip()


def _parse_yandex_geocode_json(payload: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Возвращает (широта, долгота, текст адреса) или (None, None, None)."""
    try:
        coll = payload.get("response", {}).get("GeoObjectCollection", {})
        members = coll.get("featureMember")
        if not isinstance(members, list) or not members:
            return None, None, None
        geo = members[0].get("GeoObject")
        if not isinstance(geo, dict):
            return None, None, None
        pos_raw = geo.get("Point", {}).get("pos")
        if not pos_raw or not str(pos_raw).strip():
            return None, None, None
        parts = str(pos_raw).strip().split()
        if len(parts) < 2:
            return None, None, None
        lon_s, lat_s = parts[0], parts[1]
        meta = geo.get("metaDataProperty", {}).get("GeocoderMetaData", {})
        text = meta.get("text") if isinstance(meta, dict) else None
        addr = str(text).strip() if text is not None else ""
        return lat_s, lon_s, addr or None
    except (TypeError, AttributeError, KeyError, ValueError):
        return None, None, None


def process_yandex_geocode(lines: List[str], api_key: str) -> Tuple[str, str, str, List[List[str]]]:
    """Геокодирование через HTTP Geocoder API Яндекса (JSON).

    Возвращает (координаты построчно, адреса построчно, полный вывод, строки для Excel).
    """
    coord_parts: List[str] = []
    addr_parts: List[str] = []
    full_parts: List[str] = []
    excel_rows: List[List[str]] = []
    with httpx.Client(timeout=REQUEST_TIMEOUT) as client:
        for idx, raw in enumerate(lines):
            source = raw.strip()
            if not source:
                continue
            if idx > 0:
                time.sleep(YANDEX_GEOCODE_PAUSE_SEC)
            try:
                r = client.get(
                    YANDEX_GEOCODE_URL,
                    params={
                        "apikey": api_key,
                        "geocode": source,
                        "format": "json",
                        "lang": "ru_RU",
                        "results": 1,
                    },
                )
                r.raise_for_status()
                data = r.json()
                if not isinstance(data, dict):
                    raise ValueError("Ответ не JSON-объект")
                lat, lon, yandex_text = _parse_yandex_geocode_json(data)
                if lat is None or lon is None:
                    coord_parts.append("не найдено")
                    addr_parts.append("—")
                    excel_rows.append([source, "—", "не найдено"])
                    full_parts.append(
                        f"Исходник:\n{source}\n\n"
                        "Координаты не найдены (пустой ответ геокодера).\n"
                    )
                else:
                    coord_parts.append(f"{lat}, {lon}")
                    addr_cell = (yandex_text.strip() if yandex_text else "") or "—"
                    addr_parts.append(addr_cell)
                    excel_rows.append([source, addr_cell, f"{lat}, {lon}"])
                    addr_block = yandex_text or "—"
                    full_parts.append(
                        f"Исходник:\n{source}\n\n"
                        f"Широта: {lat}\n"
                        f"Долгота: {lon}\n"
                        f"Адрес (Яндекс): {addr_block}\n"
                    )
            except Exception as e:
                msg = _format_yandex_request_error(e)
                err_line = f"Ошибка: {msg.splitlines()[0]}"
                coord_parts.append(err_line)
                addr_parts.append("—")
                excel_rows.append([source, "—", err_line])
                full_parts.append(f"Исходник:\n{source}\n\nОшибка:\n{msg}\n")
            full_parts.append("---\n")
    if full_parts and full_parts[-1] == "---\n":
        full_parts.pop()
    coords_text = "\n".join(coord_parts)
    addresses_text = "\n".join(addr_parts)
    full_text = "\n".join(full_parts).rstrip() + "\n"
    return coords_text, addresses_text, full_text, excel_rows


def process_addresses(lines: List[str], token: str, secret: Optional[str]) -> Tuple[str, str, List[List[str]]]:
    short_parts: List[str] = []
    full_parts: List[str] = []
    excel_rows: List[List[str]] = []
    with Dadata(token, secret, timeout=REQUEST_TIMEOUT) as dadata:
        for raw in lines:
            source = raw.strip()
            if not source:
                continue
            try:
                data = dadata.clean(name="address", source=source)
                short_parts.append(format_short_line(data))
                full_parts.append(format_clean_block(source, data))
                excel_rows.append(_dadata_excel_row(source, data, None))
            except Exception as e:
                short_parts.append(f"Ошибка: {e!s}")
                full_parts.append(format_error_block(source, e))
                excel_rows.append(_dadata_excel_row(source, None, e))
            full_parts.append("---\n")
    if full_parts and full_parts[-1] == "---\n":
        full_parts.pop()
    short_text = "\n".join(short_parts)
    full_text = "\n".join(full_parts).rstrip() + "\n"
    return short_text, full_text, excel_rows


class AddressCleanerApp:
    def __init__(self) -> None:
        self.root = tk.Tk()
        self.root.title("DaData — стандартизация адресов")
        self.root.minsize(760, 540)
        self._worker: Optional[threading.Thread] = None
        self._full_expanded = False
        self._last_dadata_excel_rows: Optional[List[List[str]]] = None
        self._last_yandex_excel_rows: Optional[List[List[str]]] = None

        _apply_theme(self.root)

        outer = ttk.Frame(self.root, style="App.TFrame", padding=(16, 14))
        outer.pack(fill=tk.BOTH, expand=True)

        header = ttk.Frame(outer, style="App.TFrame")
        header.pack(fill=tk.X, pady=(0, 12))
        ttk.Label(
            header,
            text="Стандартизация адресов",
            font=("Segoe UI", 14, "bold"),
            foreground=COL_TEXT,
            background=COL_BG,
        ).pack(anchor=tk.W)
        ttk.Label(
            header,
            text="Вставьте «грязные» адреса слева — справа появится нормализованная строка.",
            style="Muted.TLabel",
        ).pack(anchor=tk.W, pady=(4, 0))

        cred = ttk.LabelFrame(outer, text="  Ключи и сеть  ", style="Card.TLabelframe", padding=(12, 10))
        cred.pack(fill=tk.X, pady=(0, 12))
        cred.columnconfigure(1, weight=1)

        saved_token, saved_secret, saved_yandex, saved_yandex_dev = _load_saved_api_credentials()
        self._token_var = tk.StringVar(value=_env_or_saved("DADATA_TOKEN", saved_token))
        self._secret_var = tk.StringVar(value=_env_or_saved("DADATA_SECRET", saved_secret))
        self._yandex_key_var = tk.StringVar(
            value=_env_or_saved("YANDEX_GEOCODER_API_KEY", saved_yandex)
        )
        self._yandex_dev_auth_var = tk.StringVar(
            value=_env_or_saved("YANDEX_DEVELOPER_AUTH_KEY", saved_yandex_dev)
        )
        sh, sp, su, spw = _load_saved_socks_proxy()
        self._socks_host = sh
        self._socks_port = sp
        self._socks_user = su
        self._socks_password = spw
        self._use_proxy_var = tk.BooleanVar(value=_env_flag_enabled("DADATA_USE_PROXY", False))
        if not self._use_proxy_var.get():
            clear_proxy_env()
        self._cred_fields_expanded = True

        cred_toggle_row = tk.Frame(cred, bg=COL_CARD)
        cred_toggle_row.grid(row=0, column=0, columnspan=2, sticky=tk.EW)
        self._cred_toggle_btn = ttk.Button(
            cred_toggle_row,
            text="▼ Свернуть поля ключей",
            command=self._toggle_cred_fields,
            style="Secondary.TButton",
        )
        self._cred_toggle_btn.pack(anchor=tk.W, pady=(0, 6))

        self._cred_body = tk.Frame(cred, bg=COL_CARD)
        self._cred_body.grid(row=1, column=0, columnspan=2, sticky=tk.EW)
        self._cred_body.columnconfigure(1, weight=1)

        ttk.Label(self._cred_body, text="API-ключ", background=COL_CARD, foreground=COL_TEXT).grid(
            row=0, column=0, sticky=tk.W, padx=(0, 10), pady=(0, 6)
        )
        self.entry_token = ttk.Entry(
            self._cred_body, textvariable=self._token_var, show="*", style="Card.TEntry"
        )
        self.entry_token.grid(row=0, column=1, sticky=tk.EW, pady=(0, 6))

        ttk.Label(self._cred_body, text="Secret", background=COL_CARD, foreground=COL_TEXT).grid(
            row=1, column=0, sticky=tk.W, padx=(0, 10), pady=(0, 6)
        )
        self.entry_secret = ttk.Entry(
            self._cred_body, textvariable=self._secret_var, show="*", style="Card.TEntry"
        )
        self.entry_secret.grid(row=1, column=1, sticky=tk.EW, pady=(0, 6))

        ttk.Label(self._cred_body, text="Ключ Яндекс (геокодер)", background=COL_CARD, foreground=COL_TEXT).grid(
            row=2, column=0, sticky=tk.W, padx=(0, 10), pady=(0, 6)
        )
        self.entry_yandex = ttk.Entry(
            self._cred_body, textvariable=self._yandex_key_var, show="*", style="Card.TEntry"
        )
        self.entry_yandex.grid(row=2, column=1, sticky=tk.EW, pady=(0, 6))

        ttk.Label(
            self._cred_body, text="Ключ API кабинета (X-Auth-Key)", background=COL_CARD, foreground=COL_TEXT
        ).grid(row=3, column=0, sticky=tk.W, padx=(0, 10), pady=(0, 6))
        self.entry_yandex_dev = ttk.Entry(
            self._cred_body, textvariable=self._yandex_dev_auth_var, show="*", style="Card.TEntry"
        )
        self.entry_yandex_dev.grid(row=3, column=1, sticky=tk.EW, pady=(0, 6))

        cred_btns = tk.Frame(cred, bg=COL_CARD)
        cred_btns.grid(row=2, column=0, columnspan=2, sticky=tk.W)
        self.balance_btn = ttk.Button(
            cred_btns,
            text="Показать баланс",
            command=self._on_balance,
            style="Secondary.TButton",
        )
        self.balance_btn.pack(side=tk.LEFT, padx=(0, 8))
        self.stats_btn = ttk.Button(
            cred_btns,
            text="Показать статистику",
            command=self._on_stats,
            style="Secondary.TButton",
        )
        self.stats_btn.pack(side=tk.LEFT)
        self.yandex_dev_btn = ttk.Button(
            cred_btns,
            text="Лимиты Яндекс (кабинет)",
            command=self._on_yandex_developer_limits,
            style="Secondary.TButton",
        )
        self.yandex_dev_btn.pack(side=tk.LEFT, padx=(8, 0))
        sep_proxy = tk.Frame(cred_btns, width=1, bg=COL_BORDER, highlightthickness=0)
        sep_proxy.pack(side=tk.LEFT, fill=tk.Y, padx=(18, 12), pady=5)
        self._proxy_settings_btn = ttk.Button(
            cred_btns,
            text="Настроить прокси",
            command=self._on_configure_proxy,
            style="Secondary.TButton",
        )
        self._proxy_settings_btn.pack(side=tk.LEFT, padx=(0, 10))
        self._proxy_chk = ttk.Checkbutton(
            cred_btns,
            text="Через SOCKS5",
            variable=self._use_proxy_var,
            command=self._on_use_proxy_toggle,
            style="Card.TCheckbutton",
        )
        self._proxy_chk.pack(side=tk.LEFT)

        main = ttk.Frame(outer, style="App.TFrame")
        main.pack(fill=tk.BOTH, expand=True)
        main.columnconfigure(0, weight=1)
        main.columnconfigure(1, weight=1)
        main.rowconfigure(0, weight=1)

        in_card = ttk.LabelFrame(main, text="  Ввод  ", style="Card.TLabelframe", padding=(12, 10))
        in_card.grid(row=0, column=0, sticky=tk.NSEW, padx=(0, 8))

        self.input_text = scrolledtext.ScrolledText(
            in_card,
            height=14,
            wrap=tk.WORD,
            font=("Segoe UI", 10),
            bg="#fafbfc",
            fg=COL_TEXT,
            insertbackground=COL_ACCENT,
            selectbackground=COL_ACCENT_DIM,
            relief=tk.FLAT,
            highlightthickness=1,
            highlightbackground=COL_BORDER,
            highlightcolor=COL_ACCENT,
            padx=10,
            pady=10,
        )
        self.input_text.pack(fill=tk.BOTH, expand=True)

        out_card = ttk.LabelFrame(main, text="  Результат  ", style="Card.TLabelframe", padding=(12, 10))
        out_card.grid(row=0, column=1, sticky=tk.NSEW, padx=(8, 0))

        self._out_result_body = ttk.Frame(out_card, style="App.TFrame")
        self._out_result_body.pack(fill=tk.BOTH, expand=True)
        self._result_yandex_split_visible = False

        st_kw = dict(
            wrap=tk.WORD,
            state=tk.DISABLED,
            font=("Segoe UI", 10),
            bg="#f8fafc",
            fg=COL_TEXT,
            relief=tk.FLAT,
            highlightthickness=1,
            highlightbackground=COL_BORDER,
            padx=10,
            pady=10,
        )
        self.short_text = scrolledtext.ScrolledText(self._out_result_body, height=14, **st_kw)
        self.short_text.pack(fill=tk.BOTH, expand=True)

        self._yandex_result_pane = ttk.Panedwindow(self._out_result_body, orient=tk.VERTICAL)
        coords_wrap = ttk.Frame(self._yandex_result_pane, style="App.TFrame")
        ttk.Label(coords_wrap, text="Координаты", style="Muted.TLabel").pack(anchor=tk.W, pady=(0, 4))
        self.yandex_coords_text = scrolledtext.ScrolledText(coords_wrap, height=7, **st_kw)
        self.yandex_coords_text.pack(fill=tk.BOTH, expand=True)
        addr_wrap = ttk.Frame(self._yandex_result_pane, style="App.TFrame")
        ttk.Label(addr_wrap, text="Адреса (Яндекс)", style="Muted.TLabel").pack(anchor=tk.W, pady=(0, 4))
        self.yandex_addr_text = scrolledtext.ScrolledText(addr_wrap, height=7, **st_kw)
        self.yandex_addr_text.pack(fill=tk.BOTH, expand=True)
        self._yandex_result_pane.add(coords_wrap)
        self._yandex_result_pane.add(addr_wrap)

        btn_row = ttk.Frame(outer, style="App.TFrame")
        btn_row.pack(fill=tk.X, pady=(14, 8))
        self.run_btn = ttk.Button(
            btn_row,
            text="  Стандартизировать  ",
            command=self._on_run,
            style="Primary.TButton",
        )
        self.run_btn.pack(side=tk.LEFT)
        self.yandex_btn = ttk.Button(
            btn_row,
            text="Получить координаты",
            command=self._on_yandex_geocode,
            style="Secondary.TButton",
        )
        self.yandex_btn.pack(side=tk.LEFT, padx=(12, 0))
        self.export_dadata_btn = ttk.Button(
            btn_row,
            text="В Excel (DaData)",
            command=self._export_excel_dadata,
            style="Secondary.TButton",
            state=tk.DISABLED,
        )
        self.export_dadata_btn.pack(side=tk.LEFT, padx=(12, 0))
        self.export_yandex_btn = ttk.Button(
            btn_row,
            text="В Excel (Яндекс)",
            command=self._export_excel_yandex,
            style="Secondary.TButton",
            state=tk.DISABLED,
        )
        self.export_yandex_btn.pack(side=tk.LEFT, padx=(8, 0))
        ttk.Label(btn_row, text="Ctrl+Enter — стандартизация", style="Muted.TLabel").pack(side=tk.LEFT, padx=(14, 0))

        ttk.Separator(outer, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=(0, 10))

        self.full_outer = ttk.Frame(outer, style="App.TFrame")
        self.full_outer.pack(fill=tk.BOTH, expand=False)

        self.toggle_btn = ttk.Button(
            self.full_outer,
            text="Показать полный вывод",
            command=self._toggle_full,
            style="Secondary.TButton",
        )
        self.toggle_btn.pack(anchor=tk.W)

        self.full_text = scrolledtext.ScrolledText(
            self.full_outer,
            height=11,
            wrap=tk.WORD,
            state=tk.DISABLED,
            font=("Consolas", 9),
            bg="#f1f5f9",
            fg=COL_TEXT,
            relief=tk.FLAT,
            highlightthickness=1,
            highlightbackground=COL_BORDER,
            padx=10,
            pady=10,
        )

        status_wrap = ttk.Frame(outer, style="App.TFrame")
        status_wrap.pack(fill=tk.X, pady=(12, 0))
        status_inner = tk.Frame(status_wrap, bg=COL_CARD, highlightbackground=COL_BORDER, highlightthickness=1)
        status_inner.pack(fill=tk.X)
        self.status = tk.StringVar(value="Готово.")
        ttk.Label(status_inner, textvariable=self.status, style="Status.TLabel", padding=(12, 8)).pack(
            fill=tk.X, anchor=tk.W
        )

        self.root.bind("<Control-Return>", lambda e: self._on_run())
        self.root.protocol("WM_DELETE_WINDOW", self._on_close_request)

    def _persist_api_credentials(self) -> None:
        _save_api_credentials(
            self._token_var.get(),
            self._secret_var.get(),
            self._yandex_key_var.get(),
            self._yandex_dev_auth_var.get(),
        )

    def _on_close_request(self) -> None:
        self._persist_api_credentials()
        self.root.destroy()

    def _get_token_secret(self) -> Tuple[str, Optional[str]]:
        token = self._token_var.get().strip()
        secret = self._secret_var.get().strip()
        return token, (secret if secret else None)

    def _current_socks_proxy_url(self) -> str:
        return _build_socks5_url(self._socks_host, self._socks_port, self._socks_user, self._socks_password)

    def _sync_proxy_for_request(self) -> None:
        if self._use_proxy_var.get():
            apply_proxy_from_value(self._current_socks_proxy_url())
        else:
            clear_proxy_env()

    def _on_use_proxy_toggle(self) -> None:
        if self._use_proxy_var.get():
            apply_proxy_from_value(self._current_socks_proxy_url())
        else:
            clear_proxy_env()

    def _on_configure_proxy(self) -> None:
        win = tk.Toplevel(self.root)
        win.title("Прокси SOCKS5")
        win.minsize(400, 260)
        win.resizable(True, False)
        win.configure(bg=COL_BG)
        win.transient(self.root)
        win.grab_set()

        outer = ttk.Frame(win, style="App.TFrame", padding=14)
        outer.pack(fill=tk.BOTH, expand=True)
        outer.columnconfigure(1, weight=1)

        host_v = tk.StringVar(value=self._socks_host)
        port_v = tk.StringVar(value=self._socks_port)
        user_v = tk.StringVar(value=self._socks_user)
        pass_v = tk.StringVar(value=self._socks_password)

        row = 0
        ttk.Label(outer, text="IP или хост", background=COL_BG, foreground=COL_TEXT).grid(
            row=row, column=0, sticky=tk.W, padx=(0, 10), pady=(0, 8)
        )
        ttk.Entry(outer, textvariable=host_v, width=36, style="Card.TEntry").grid(
            row=row, column=1, sticky=tk.EW, pady=(0, 8)
        )
        row += 1
        ttk.Label(outer, text="Порт", background=COL_BG, foreground=COL_TEXT).grid(
            row=row, column=0, sticky=tk.W, padx=(0, 10), pady=(0, 8)
        )
        ttk.Entry(outer, textvariable=port_v, width=36, style="Card.TEntry").grid(
            row=row, column=1, sticky=tk.EW, pady=(0, 8)
        )
        row += 1
        ttk.Label(outer, text="Логин", background=COL_BG, foreground=COL_TEXT).grid(
            row=row, column=0, sticky=tk.W, padx=(0, 10), pady=(0, 8)
        )
        ttk.Entry(outer, textvariable=user_v, width=36, style="Card.TEntry").grid(
            row=row, column=1, sticky=tk.EW, pady=(0, 8)
        )
        row += 1
        ttk.Label(outer, text="Пароль", background=COL_BG, foreground=COL_TEXT).grid(
            row=row, column=0, sticky=tk.W, padx=(0, 10), pady=(0, 8)
        )
        ttk.Entry(outer, textvariable=pass_v, width=36, show="*", style="Card.TEntry").grid(
            row=row, column=1, sticky=tk.EW, pady=(0, 8)
        )
        row += 1
        ttk.Label(
            outer,
            text="Протокол: SOCKS5. Пустой логин и пароль — без авторизации.",
            style="Muted.TLabel",
        ).grid(row=row, column=0, columnspan=2, sticky=tk.W, pady=(4, 12))
        row += 1

        btn_row = ttk.Frame(outer, style="App.TFrame")
        btn_row.grid(row=row, column=0, columnspan=2, sticky=tk.EW)

        def on_ok() -> None:
            port_s = port_v.get().strip().lstrip(":") or "9150"
            try:
                pi = int(port_s, 10)
            except ValueError:
                messagebox.showerror("Прокси", "Порт должен быть целым числом.", parent=win)
                return
            if not 1 <= pi <= 65535:
                messagebox.showerror("Прокси", "Порт должен быть от 1 до 65535.", parent=win)
                return
            host_s = host_v.get().strip() or "127.0.0.1"
            user_s = user_v.get().strip()
            pass_s = pass_v.get().strip()
            self._socks_host = host_s
            self._socks_port = str(pi)
            self._socks_user = user_s
            self._socks_password = pass_s
            _save_socks_proxy_to_file(host_s, str(pi), user_s, pass_s)
            if self._use_proxy_var.get():
                apply_proxy_from_value(self._current_socks_proxy_url())
            else:
                clear_proxy_env()
            win.destroy()

        def on_cancel() -> None:
            win.destroy()

        ttk.Button(btn_row, text="ОК", command=on_ok, style="Primary.TButton").pack(side=tk.RIGHT, padx=(8, 0))
        ttk.Button(btn_row, text="Отмена", command=on_cancel, style="Secondary.TButton").pack(side=tk.RIGHT)
        win.bind("<Return>", lambda e: on_ok())
        win.bind("<Escape>", lambda e: on_cancel())

    def _toggle_cred_fields(self) -> None:
        self._cred_fields_expanded = not self._cred_fields_expanded
        if self._cred_fields_expanded:
            self._cred_body.grid(row=1, column=0, columnspan=2, sticky=tk.EW)
            self._cred_toggle_btn.configure(text="▼ Свернуть поля ключей")
        else:
            self._cred_body.grid_remove()
            self._cred_toggle_btn.configure(text="▶ Показать поля ключей")

    def _set_loading(self, loading: bool) -> None:
        state = tk.DISABLED if loading else tk.NORMAL
        self.run_btn.configure(state=state)
        self.yandex_btn.configure(state=state)
        self.balance_btn.configure(state=state)
        self.stats_btn.configure(state=state)
        self.yandex_dev_btn.configure(state=state)
        self._proxy_settings_btn.configure(state=state)
        self.export_dadata_btn.configure(
            state=tk.DISABLED
            if loading
            else (tk.NORMAL if self._last_dadata_excel_rows else tk.DISABLED)
        )
        self.export_yandex_btn.configure(
            state=tk.DISABLED
            if loading
            else (tk.NORMAL if self._last_yandex_excel_rows else tk.DISABLED)
        )

    def _show_stats_dialog(self, data: Dict[str, Any]) -> None:
        win = tk.Toplevel(self.root)
        win.title("Статистика DaData")
        win.minsize(520, 420)
        win.configure(bg=COL_BG)

        outer = ttk.Frame(win, style="App.TFrame", padding=12)
        outer.pack(fill=tk.BOTH, expand=True)

        ttk.Label(
            outer,
            text="Суточная статистика использования API",
            font=("Segoe UI", 11, "bold"),
            background=COL_BG,
            foreground=COL_TEXT,
        ).pack(anchor=tk.W, pady=(0, 8))

        txt = scrolledtext.ScrolledText(
            outer,
            wrap=tk.WORD,
            font=("Segoe UI", 10),
            bg=COL_CARD,
            fg=COL_TEXT,
            relief=tk.FLAT,
            highlightthickness=1,
            highlightbackground=COL_BORDER,
            padx=12,
            pady=12,
            width=72,
            height=22,
        )
        txt.pack(fill=tk.BOTH, expand=True)
        txt.insert(tk.END, format_daily_stats_human(data))
        txt.configure(state=tk.DISABLED)

        btn_row = ttk.Frame(outer, style="App.TFrame")
        btn_row.pack(fill=tk.X, pady=(12, 0))

        def copy_json() -> None:
            payload = json.dumps(data, ensure_ascii=False, indent=2, default=str)
            win.clipboard_clear()
            win.clipboard_append(payload)
            win.update()

        ttk.Button(btn_row, text="Копировать JSON", command=copy_json, style="Secondary.TButton").pack(
            side=tk.LEFT
        )
        ttk.Button(btn_row, text="Закрыть", command=win.destroy, style="Secondary.TButton").pack(side=tk.RIGHT)

    def _on_balance(self) -> None:
        if self._worker is not None and self._worker.is_alive():
            return
        token, secret = self._get_token_secret()
        if not token:
            messagebox.showerror("DaData", "Укажите API-ключ.")
            return
        self._sync_proxy_for_request()
        self._set_loading(True)
        self.status.set("Запрос баланса…")

        def work() -> None:
            err: Optional[BaseException] = None
            bal: Optional[float] = None
            try:
                with Dadata(token, secret, timeout=REQUEST_TIMEOUT) as dadata:
                    bal = dadata.get_balance()
            except BaseException as e:
                err = e

            def finish() -> None:
                self._set_loading(False)
                self._worker = None
                self.status.set("Готово.")
                if err is not None:
                    messagebox.showerror("DaData", str(err))
                    return
                messagebox.showinfo("Баланс DaData", f"Баланс: {bal}")

            self.root.after(0, finish)

        self._worker = threading.Thread(target=work, daemon=True)
        self._worker.start()

    def _on_stats(self) -> None:
        if self._worker is not None and self._worker.is_alive():
            return
        token, secret = self._get_token_secret()
        if not token:
            messagebox.showerror("DaData", "Укажите API-ключ.")
            return
        self._sync_proxy_for_request()
        self._set_loading(True)
        self.status.set("Запрос статистики…")

        def work() -> None:
            err: Optional[BaseException] = None
            stats: Optional[Dict[str, Any]] = None
            try:
                with Dadata(token, secret, timeout=REQUEST_TIMEOUT) as dadata:
                    stats = dadata.get_daily_stats()
            except BaseException as e:
                err = e

            def finish() -> None:
                self._set_loading(False)
                self._worker = None
                self.status.set("Готово.")
                if err is not None:
                    messagebox.showerror("DaData", str(err))
                    return
                if stats is not None:
                    self._show_stats_dialog(stats)

            self.root.after(0, finish)

        self._worker = threading.Thread(target=work, daemon=True)
        self._worker.start()

    def _show_yandex_developer_dialog(self, text: str) -> None:
        win = tk.Toplevel(self.root)
        win.title("Лимиты — API кабинета Яндекса")
        win.minsize(560, 440)
        win.configure(bg=COL_BG)

        outer = ttk.Frame(win, style="App.TFrame", padding=12)
        outer.pack(fill=tk.BOTH, expand=True)

        ttk.Label(
            outer,
            text="Проекты, сервисы и суточные лимиты (геокодер, JS API и др.)",
            font=("Segoe UI", 11, "bold"),
            background=COL_BG,
            foreground=COL_TEXT,
        ).pack(anchor=tk.W, pady=(0, 8))

        txt = scrolledtext.ScrolledText(
            outer,
            wrap=tk.WORD,
            font=("Consolas", 10),
            bg=COL_CARD,
            fg=COL_TEXT,
            relief=tk.FLAT,
            highlightthickness=1,
            highlightbackground=COL_BORDER,
            padx=12,
            pady=12,
            width=80,
            height=24,
        )
        txt.pack(fill=tk.BOTH, expand=True)
        txt.insert(tk.END, text)
        txt.configure(state=tk.DISABLED)

        btn_row = ttk.Frame(outer, style="App.TFrame")
        btn_row.pack(fill=tk.X, pady=(12, 0))

        def copy_all() -> None:
            win.clipboard_clear()
            win.clipboard_append(text)
            win.update()

        ttk.Button(btn_row, text="Копировать", command=copy_all, style="Secondary.TButton").pack(side=tk.LEFT)
        ttk.Button(btn_row, text="Закрыть", command=win.destroy, style="Secondary.TButton").pack(side=tk.RIGHT)

    def _on_yandex_developer_limits(self) -> None:
        if self._worker is not None and self._worker.is_alive():
            return
        auth = self._yandex_dev_auth_var.get().strip()
        if not auth:
            messagebox.showerror(
                "API кабинета Яндекса",
                "Укажите ключ API кабинета разработчика (X-Auth-Key).\n"
                "Его выдают в сервисе «API кабинета» на developer.tech.yandex.ru — это не ключ геокодера.",
            )
            return
        self._sync_proxy_for_request()
        self._set_loading(True)
        self.status.set("Запрос лимитов API кабинета Яндекса…")

        def work() -> None:
            err: Optional[BaseException] = None
            report = ""
            try:
                report = fetch_yandex_developer_limits_report(auth)
            except BaseException as e:
                err = e

            def finish() -> None:
                self._set_loading(False)
                self._worker = None
                self.status.set("Готово.")
                if err is not None:
                    detail = _http_error_with_body(err) if isinstance(err, httpx.HTTPStatusError) else str(err)
                    messagebox.showerror("API кабинета Яндекса", detail)
                    return
                self._show_yandex_developer_dialog(report)

            self.root.after(0, finish)

        self._worker = threading.Thread(target=work, daemon=True)
        self._worker.start()

    def _on_yandex_geocode(self) -> None:
        if self._worker is not None and self._worker.is_alive():
            return

        yandex_key = self._yandex_key_var.get().strip()
        if not yandex_key:
            messagebox.showerror("Яндекс геокодер", "Укажите ключ API Яндекса (геокодер).")
            return

        raw = self.input_text.get("1.0", tk.END)
        lines = [ln for ln in raw.splitlines() if ln.strip()]
        if not lines:
            self.status.set("Нет непустых строк.")
            return

        self._sync_proxy_for_request()
        self._set_loading(True)
        self.status.set(f"Геокодирование Яндекс… ({len(lines)} адр.)")

        def work() -> None:
            coords_out = ""
            addr_out = ""
            full_out = ""
            err: Optional[BaseException] = None
            tb_str = ""
            try:
                coords_out, addr_out, full_out, yandex_x_rows = process_yandex_geocode(lines, yandex_key)
            except BaseException as e:
                err = e
                tb_str = traceback.format_exc()

            def finish() -> None:
                self._set_loading(False)
                self._worker = None
                if err is not None:
                    self._last_yandex_excel_rows = None
                    self._update_excel_export_buttons()
                    self._set_both("Ошибка запроса", tb_str)
                    self.status.set("Ошибка.")
                    if not self._full_expanded:
                        self._toggle_full()
                    messagebox.showerror("Яндекс геокодер", str(err))
                    return
                self._last_yandex_excel_rows = yandex_x_rows
                self._update_excel_export_buttons()
                self._show_result_yandex_split()
                self._set_yandex_result_panes(coords_out, addr_out)
                self._set_full(full_out)
                self.status.set(f"Готово. Координаты: {len(lines)} адр.")

            self.root.after(0, finish)

        self._worker = threading.Thread(target=work, daemon=True)
        self._worker.start()

    def _toggle_full(self) -> None:
        self._full_expanded = not self._full_expanded
        if self._full_expanded:
            self.full_text.pack(fill=tk.BOTH, expand=True, pady=(10, 0))
            self.toggle_btn.configure(text="Скрыть полный вывод")
        else:
            self.full_text.pack_forget()
            self.toggle_btn.configure(text="Показать полный вывод")

    def _set_short(self, text: str) -> None:
        self.short_text.configure(state=tk.NORMAL)
        self.short_text.delete("1.0", tk.END)
        self.short_text.insert(tk.END, text)
        self.short_text.configure(state=tk.DISABLED)

    def _set_full(self, text: str) -> None:
        self.full_text.configure(state=tk.NORMAL)
        self.full_text.delete("1.0", tk.END)
        self.full_text.insert(tk.END, text)
        self.full_text.configure(state=tk.DISABLED)

    def _show_result_single(self) -> None:
        if self._result_yandex_split_visible:
            self._yandex_result_pane.pack_forget()
            self._result_yandex_split_visible = False
        self.short_text.pack(fill=tk.BOTH, expand=True)

    def _show_result_yandex_split(self) -> None:
        if self._result_yandex_split_visible:
            return
        self.short_text.pack_forget()
        self._yandex_result_pane.pack(fill=tk.BOTH, expand=True)
        self._result_yandex_split_visible = True

    def _set_yandex_result_panes(self, coords: str, addresses: str) -> None:
        for widget, text in (
            (self.yandex_coords_text, coords),
            (self.yandex_addr_text, addresses),
        ):
            widget.configure(state=tk.NORMAL)
            widget.delete("1.0", tk.END)
            widget.insert(tk.END, text)
            widget.configure(state=tk.DISABLED)

    def _set_both(self, short: str, full: str) -> None:
        self._show_result_single()
        self._set_short(short)
        self._set_full(full)

    def _on_run(self) -> None:
        if self._worker is not None and self._worker.is_alive():
            return

        token, secret = self._get_token_secret()
        if not token:
            messagebox.showerror("DaData", "Укажите API-ключ.")
            return

        raw = self.input_text.get("1.0", tk.END)
        lines = [ln for ln in raw.splitlines() if ln.strip()]
        if not lines:
            self.status.set("Нет непустых строк.")
            return

        self._sync_proxy_for_request()
        self._set_loading(True)
        self.status.set(f"Обработка… ({len(lines)} адр.)")

        def work() -> None:
            short_out = ""
            full_out = ""
            err: Optional[BaseException] = None
            tb_str = ""
            try:
                short_out, full_out, dadata_x_rows = process_addresses(lines, token, secret)
            except BaseException as e:
                err = e
                tb_str = traceback.format_exc()

            def finish() -> None:
                self._set_loading(False)
                self._worker = None
                if err is not None:
                    self._last_dadata_excel_rows = None
                    self._update_excel_export_buttons()
                    self._set_both("Ошибка запроса", tb_str)
                    self.status.set("Ошибка.")
                    if not self._full_expanded:
                        self._toggle_full()
                    messagebox.showerror("DaData", str(err))
                    return
                self._last_dadata_excel_rows = dadata_x_rows
                self._update_excel_export_buttons()
                self._set_both(short_out, full_out)
                self.status.set(f"Готово. Обработано строк: {len(lines)}.")

            self.root.after(0, finish)

        self._worker = threading.Thread(target=work, daemon=True)
        self._worker.start()

    def _update_excel_export_buttons(self) -> None:
        self.export_dadata_btn.configure(
            state=tk.NORMAL if self._last_dadata_excel_rows else tk.DISABLED
        )
        self.export_yandex_btn.configure(
            state=tk.NORMAL if self._last_yandex_excel_rows else tk.DISABLED
        )

    def _export_excel_dadata(self) -> None:
        rows = self._last_dadata_excel_rows
        if not rows:
            return
        path = filedialog.asksaveasfilename(
            parent=self.root,
            title="Сохранить результат DaData",
            defaultextension=".xlsx",
            filetypes=[("Книга Excel", "*.xlsx"), ("Все файлы", "*.*")],
        )
        if not path:
            return
        if not str(path).lower().endswith(".xlsx"):
            path = f"{path}.xlsx"
        try:
            _write_xlsx(path, "DaData", dadata_excel_headers(), rows)
        except Exception as e:
            messagebox.showerror("Excel", f"Не удалось сохранить файл:\n{e}", parent=self.root)
            return
        self.status.set(f"Сохранено: {path}")

    def _export_excel_yandex(self) -> None:
        rows = self._last_yandex_excel_rows
        if not rows:
            return
        path = filedialog.asksaveasfilename(
            parent=self.root,
            title="Сохранить результат Яндекса",
            defaultextension=".xlsx",
            filetypes=[("Книга Excel", "*.xlsx"), ("Все файлы", "*.*")],
        )
        if not path:
            return
        if not str(path).lower().endswith(".xlsx"):
            path = f"{path}.xlsx"
        try:
            _write_xlsx(path, "Яндекс", list(YANDEX_EXCEL_HEADERS), rows)
        except Exception as e:
            messagebox.showerror("Excel", f"Не удалось сохранить файл:\n{e}", parent=self.root)
            return
        self.status.set(f"Сохранено: {path}")

    def run(self) -> None:
        self.root.mainloop()


def main() -> None:
    AddressCleanerApp().run()


if __name__ == "__main__":
    main()
