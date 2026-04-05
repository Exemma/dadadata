"""
Стандартизация адресов через DaData Cleaner API (GUI на Tkinter).

Общая логика: address_cleaner_core.py. Альтернативный интерфейс: address_cleaner_gui_ctk.py (CustomTkinter).

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
import threading
import traceback
from typing import Any, Dict, List, Optional, Tuple

import httpx
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext, ttk
from dadata import Dadata

from address_cleaner_core import (
    REQUEST_TIMEOUT,
    YANDEX_EXCEL_HEADERS,
    apply_proxy_from_value,
    clear_proxy_env,
    dadata_excel_headers,
    fetch_yandex_developer_limits_report,
    format_daily_stats_human,
    process_addresses,
    process_yandex_geocode,
    _build_socks5_url,
    _env_flag_enabled,
    _env_or_saved,
    _http_error_with_body,
    _load_saved_api_credentials,
    _load_saved_socks_proxy,
    _save_api_credentials,
    _save_socks_proxy_to_file,
    _write_xlsx,
)


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
