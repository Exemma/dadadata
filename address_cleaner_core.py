"""
Общая бизнес-логика: ключи, прокси, DaData, Яндекс геокодер, Excel.
Используется address_cleaner_gui и address_cleaner_gui_ctk.
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote, unquote, urlparse

import httpx

from dadata_proxy import apply_socks_proxy_from_env

apply_socks_proxy_from_env()

from dadata import Dadata  # noqa: E402
from openpyxl import Workbook

REQUEST_TIMEOUT = 30

_CONNECTION_LOG_MAX = 800
_connection_log_deque: deque[str] = deque(maxlen=_CONNECTION_LOG_MAX)
_connection_log_lock = threading.Lock()


def connection_log_clear() -> None:
    with _connection_log_lock:
        _connection_log_deque.clear()


def connection_log_text() -> str:
    with _connection_log_lock:
        return "\n".join(_connection_log_deque)


def _connection_log(service: str, message: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] [{service}] {message}"
    with _connection_log_lock:
        _connection_log_deque.append(line)


def _connection_log_proxy_hint() -> None:
    p = (os.environ.get("HTTPS_PROXY") or os.environ.get("ALL_PROXY") or "").strip()
    if p:
        _connection_log("Сеть", "Используется прокси (HTTPS_PROXY/ALL_PROXY)")
    else:
        _connection_log("Сеть", "Прямое соединение (прокси не задан)")


def dadata_get_balance(token: str, secret: Optional[str]) -> Any:
    _connection_log_proxy_hint()
    _connection_log("DaData", "Запрос баланса (get_balance)…")
    t0 = time.perf_counter()
    try:
        with Dadata(token, secret, timeout=REQUEST_TIMEOUT) as dadata:
            bal = dadata.get_balance()
        ms = (time.perf_counter() - t0) * 1000
        _connection_log("DaData", f"Баланс: {bal} ({ms:.0f} мс)")
        return bal
    except BaseException as e:
        ms = (time.perf_counter() - t0) * 1000
        _connection_log("DaData", f"Ошибка баланса ({ms:.0f} мс): {e!s}")
        raise


def dadata_get_daily_stats(token: str, secret: Optional[str]) -> Dict[str, Any]:
    _connection_log_proxy_hint()
    _connection_log("DaData", "Запрос суточной статистики (get_daily_stats)…")
    t0 = time.perf_counter()
    try:
        with Dadata(token, secret, timeout=REQUEST_TIMEOUT) as dadata:
            stats = dadata.get_daily_stats()
        ms = (time.perf_counter() - t0) * 1000
        _connection_log("DaData", f"Статистика получена ({ms:.0f} мс)")
        return stats
    except BaseException as e:
        ms = (time.perf_counter() - t0) * 1000
        _connection_log("DaData", f"Ошибка статистики ({ms:.0f} мс): {e!s}")
        raise


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
    _connection_log("Яндекс кабинет", f"GET {path}")
    t0 = time.perf_counter()
    try:
        r = client.get(url)
    except BaseException as e:
        _connection_log("Яндекс кабинет", f"Сбой соединения GET {path}: {e!s}")
        raise
    if r.status_code == 404 and not path.startswith("/v1"):
        alt = f"/v1{path}"
        _connection_log("Яндекс кабинет", f"HTTP 404 → повтор {alt}")
        try:
            r = client.get(f"{YANDEX_DEVELOPER_API_BASE}{alt}")
        except BaseException as e:
            _connection_log("Яндекс кабинет", f"Сбой соединения GET {alt}: {e!s}")
            raise
    ms = (time.perf_counter() - t0) * 1000
    _connection_log("Яндекс кабинет", f"HTTP {r.status_code}, {ms:.0f} мс")
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
    _connection_log_proxy_hint()
    _connection_log("Яндекс кабинет", "Формирование отчёта лимитов (несколько запросов к API)…")
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
    _connection_log_proxy_hint()
    sources = [raw.strip() for raw in lines if raw.strip()]
    total = len(sources)
    _connection_log("Яндекс геокодер", f"Старт: {total} запрос(ов) к geocode-maps.yandex.ru")
    coord_parts: List[str] = []
    addr_parts: List[str] = []
    full_parts: List[str] = []
    excel_rows: List[List[str]] = []
    with httpx.Client(timeout=REQUEST_TIMEOUT) as client:
        for idx, source in enumerate(sources):
            if idx > 0:
                time.sleep(YANDEX_GEOCODE_PAUSE_SEC)
            preview = source if len(source) <= 72 else source[:69] + "…"
            _connection_log("Яндекс геокодер", f"GET {idx + 1}/{total}: «{preview}»")
            t0 = time.perf_counter()
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
                ms = (time.perf_counter() - t0) * 1000
                _connection_log("Яндекс геокодер", f"Ответ HTTP {r.status_code}, {ms:.0f} мс")
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
                ms = (time.perf_counter() - t0) * 1000
                _connection_log("Яндекс геокодер", f"Ошибка ({ms:.0f} мс): {e!s}")
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
    _connection_log_proxy_hint()
    pending = sum(1 for raw in lines if raw.strip())
    _connection_log("DaData", f"Старт clean/address: {pending} адрес(ов)")
    short_parts: List[str] = []
    full_parts: List[str] = []
    excel_rows: List[List[str]] = []
    n = 0
    with Dadata(token, secret, timeout=REQUEST_TIMEOUT) as dadata:
        for raw in lines:
            source = raw.strip()
            if not source:
                continue
            n += 1
            _connection_log("DaData", f"clean address {n}/{pending}…")
            t0 = time.perf_counter()
            try:
                data = dadata.clean(name="address", source=source)
                ms = (time.perf_counter() - t0) * 1000
                _connection_log("DaData", f"OK {n}/{pending} ({ms:.0f} мс)")
                short_parts.append(format_short_line(data))
                full_parts.append(format_clean_block(source, data))
                excel_rows.append(_dadata_excel_row(source, data, None))
            except Exception as e:
                ms = (time.perf_counter() - t0) * 1000
                _connection_log("DaData", f"Ошибка {n}/{pending} ({ms:.0f} мс): {e!s}")
                short_parts.append(f"Ошибка: {e!s}")
                full_parts.append(format_error_block(source, e))
                excel_rows.append(_dadata_excel_row(source, None, e))
            full_parts.append("---\n")
    if full_parts and full_parts[-1] == "---\n":
        full_parts.pop()
    short_text = "\n".join(short_parts)
    full_text = "\n".join(full_parts).rstrip() + "\n"
    return short_text, full_text, excel_rows

