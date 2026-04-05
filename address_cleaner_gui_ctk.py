"""
Стандартизация адресов через DaData Cleaner API (GUI на CustomTkinter).

Те же возможности, что у address_cleaner_gui.py; логика в address_cleaner_core.

Запуск: python address_cleaner_gui_ctk.py
"""

from __future__ import annotations

import json
import threading
import traceback
import tkinter as tk
from tkinter import filedialog, messagebox
from typing import Any, Dict, List, Optional, Tuple

import customtkinter as ctk
import httpx
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

_TI = "1.0"


def _tb_set(tb: ctk.CTkTextbox, text: str, *, readonly: bool = True) -> None:
    tb.configure(state="normal")
    tb.delete(_TI, "end")
    tb.insert(_TI, text)
    tb.configure(state="disabled" if readonly else "normal")


class AddressCleanerApp:
    def __init__(self) -> None:
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("dark-blue")

        self.root = ctk.CTk()
        self.root.title("DaData — стандартизация адресов")
        self.root.geometry("960x680")
        self.root.minsize(560, 420)

        self._worker: Optional[threading.Thread] = None
        self._full_expanded = False
        self._last_dadata_excel_rows: Optional[List[List[str]]] = None
        self._last_yandex_excel_rows: Optional[List[List[str]]] = None

        sec = {"fg_color": "gray40", "hover_color": "gray35", "border_width": 0}
        self._sec_btn = sec
        font_std = ctk.CTkFont(size=13)
        font_mono = ctk.CTkFont(family="Consolas", size=12)

        self.root.grid_columnconfigure(0, weight=1)
        self.root.grid_rowconfigure(0, weight=1)

        outer = ctk.CTkFrame(self.root, fg_color="transparent")
        outer.grid(row=0, column=0, sticky="nsew", padx=16, pady=14)
        outer.grid_columnconfigure(0, weight=1)

        grid_row = 0
        hdr = ctk.CTkFrame(outer, fg_color="transparent")
        hdr.grid(row=grid_row, column=0, sticky="ew")
        grid_row += 1
        ctk.CTkLabel(
            hdr,
            text="Стандартизация адресов",
            font=ctk.CTkFont(size=20, weight="bold"),
        ).pack(anchor="w")
        ctk.CTkLabel(
            hdr,
            text="Вставьте «грязные» адреса слева — справа появится нормализованная строка.",
            text_color="gray65",
            font=font_std,
        ).pack(anchor="w", pady=(4, 0))

        cred = ctk.CTkFrame(outer, corner_radius=10)
        cred.grid(row=grid_row, column=0, sticky="ew", pady=(12, 12))
        grid_row += 1
        cred.grid_columnconfigure(1, weight=1)
        cred.grid_rowconfigure(1, weight=0)

        saved_token, saved_secret, saved_yandex, saved_yandex_dev = _load_saved_api_credentials()
        self._token_var = tk.StringVar(value=_env_or_saved("DADATA_TOKEN", saved_token))
        self._secret_var = tk.StringVar(value=_env_or_saved("DADATA_SECRET", saved_secret))
        self._yandex_key_var = tk.StringVar(value=_env_or_saved("YANDEX_GEOCODER_API_KEY", saved_yandex))
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

        self._cred_toggle_btn = ctk.CTkButton(
            cred,
            text="▼ Свернуть поля ключей",
            command=self._toggle_cred_fields,
            width=220,
            **self._sec_btn,
        )
        self._cred_toggle_btn.grid(row=0, column=0, columnspan=2, sticky="w", padx=12, pady=(12, 6))

        self._cred_body = ctk.CTkFrame(cred, fg_color="transparent")
        self._cred_body.grid(row=1, column=0, columnspan=2, sticky="ew", padx=12)
        self._cred_body.grid_columnconfigure(1, weight=1)

        def _cred_row(r: int, label: str, show_star: bool) -> ctk.CTkEntry:
            ctk.CTkLabel(self._cred_body, text=label, font=font_std).grid(
                row=r, column=0, sticky="w", padx=(0, 10), pady=(0, 8)
            )
            kw: dict[str, Any] = {"font": font_std}
            if show_star:
                kw["show"] = "*"
            ent = ctk.CTkEntry(self._cred_body, **kw)
            ent.grid(row=r, column=1, sticky="ew", pady=(0, 8))
            return ent

        self.entry_token = _cred_row(0, "API-ключ", True)
        self.entry_token.configure(textvariable=self._token_var)
        self.entry_secret = _cred_row(1, "Secret", True)
        self.entry_secret.configure(textvariable=self._secret_var)
        self.entry_yandex = _cred_row(2, "Ключ Яндекс (геокодер)", True)
        self.entry_yandex.configure(textvariable=self._yandex_key_var)
        self.entry_yandex_dev = _cred_row(3, "Ключ API кабинета (X-Auth-Key)", True)
        self.entry_yandex_dev.configure(textvariable=self._yandex_dev_auth_var)

        cred_btns = ctk.CTkFrame(cred, fg_color="transparent")
        cred_btns.grid(row=2, column=0, columnspan=2, sticky="ew", padx=12, pady=(4, 12))

        cred_row_api = ctk.CTkFrame(cred_btns, fg_color="transparent")
        cred_row_api.pack(anchor="w", fill="x")
        self.balance_btn = ctk.CTkButton(
            cred_row_api, text="Показать баланс", command=self._on_balance, width=138, **self._sec_btn
        )
        self.balance_btn.pack(side="left", padx=(0, 6))
        self.stats_btn = ctk.CTkButton(
            cred_row_api, text="Показать статистику", command=self._on_stats, width=158, **self._sec_btn
        )
        self.stats_btn.pack(side="left", padx=(0, 6))
        self.yandex_dev_btn = ctk.CTkButton(
            cred_row_api,
            text="Лимиты Яндекс (кабинет)",
            command=self._on_yandex_developer_limits,
            width=188,
            **self._sec_btn,
        )
        self.yandex_dev_btn.pack(side="left")

        cred_row_proxy = ctk.CTkFrame(cred_btns, fg_color="transparent")
        cred_row_proxy.pack(anchor="w", fill="x", pady=(8, 0))
        self._proxy_settings_btn = ctk.CTkButton(
            cred_row_proxy, text="Настроить прокси", command=self._on_configure_proxy, width=150, **self._sec_btn
        )
        self._proxy_settings_btn.pack(side="left", padx=(0, 10))
        self._proxy_chk = ctk.CTkCheckBox(
            cred_row_proxy,
            text="Через SOCKS5",
            variable=self._use_proxy_var,
            command=self._on_use_proxy_toggle,
            font=font_std,
        )
        self._proxy_chk.pack(side="left")

        main = ctk.CTkFrame(outer, fg_color="transparent")
        main.grid(row=grid_row, column=0, sticky="nsew", pady=(0, 4))
        outer.grid_rowconfigure(grid_row, weight=1)
        grid_row += 1
        main.grid_columnconfigure(0, weight=1)
        main.grid_columnconfigure(1, weight=1)
        main.grid_rowconfigure(0, weight=1)

        in_card = ctk.CTkFrame(main, corner_radius=10)
        in_card.grid(row=0, column=0, sticky="nsew", padx=(0, 8))
        ctk.CTkLabel(in_card, text="Ввод", font=ctk.CTkFont(size=14, weight="bold")).pack(
            anchor="w", padx=12, pady=(10, 4)
        )
        self.input_text = ctk.CTkTextbox(in_card, font=font_std, wrap="word", height=160)
        self.input_text.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        out_card = ctk.CTkFrame(main, corner_radius=10)
        out_card.grid(row=0, column=1, sticky="nsew", padx=(8, 0))
        ctk.CTkLabel(out_card, text="Результат", font=ctk.CTkFont(size=14, weight="bold")).pack(
            anchor="w", padx=12, pady=(10, 4)
        )

        self._out_result_body = ctk.CTkFrame(out_card, fg_color="transparent")
        self._out_result_body.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        self._result_yandex_split_visible = False

        self.short_text = ctk.CTkTextbox(self._out_result_body, font=font_std, wrap="word", height=160)
        self.short_text.configure(state="disabled")
        self.short_text.pack(fill="both", expand=True)

        self._yandex_result_pane = ctk.CTkFrame(self._out_result_body, fg_color="transparent")
        coords_wrap = ctk.CTkFrame(self._yandex_result_pane, fg_color="transparent")
        coords_wrap.pack(fill="both", expand=True, pady=(0, 6))
        ctk.CTkLabel(coords_wrap, text="Координаты", text_color="gray65", font=font_std).pack(anchor="w", pady=(0, 4))
        self.yandex_coords_text = ctk.CTkTextbox(coords_wrap, font=font_std, wrap="word", height=110)
        self.yandex_coords_text.configure(state="disabled")
        self.yandex_coords_text.pack(fill="both", expand=True)
        addr_wrap = ctk.CTkFrame(self._yandex_result_pane, fg_color="transparent")
        addr_wrap.pack(fill="both", expand=True)
        ctk.CTkLabel(addr_wrap, text="Адреса (Яндекс)", text_color="gray65", font=font_std).pack(
            anchor="w", pady=(0, 4)
        )
        self.yandex_addr_text = ctk.CTkTextbox(addr_wrap, font=font_std, wrap="word", height=110)
        self.yandex_addr_text.configure(state="disabled")
        self.yandex_addr_text.pack(fill="both", expand=True)

        btn_row = ctk.CTkFrame(outer, fg_color="transparent")
        btn_row.grid(row=grid_row, column=0, sticky="ew", pady=(8, 8))
        grid_row += 1

        btn_row_primary = ctk.CTkFrame(btn_row, fg_color="transparent")
        btn_row_primary.pack(anchor="w", fill="x")
        self.run_btn = ctk.CTkButton(
            btn_row_primary,
            text="Стандартизировать",
            command=self._on_run,
            width=170,
            font=ctk.CTkFont(size=14, weight="bold"),
        )
        self.run_btn.pack(side="left")
        self.yandex_btn = ctk.CTkButton(
            btn_row_primary,
            text="Получить координаты",
            command=self._on_yandex_geocode,
            width=170,
            **self._sec_btn,
        )
        self.yandex_btn.pack(side="left", padx=(10, 0))

        btn_row_excel = ctk.CTkFrame(btn_row, fg_color="transparent")
        btn_row_excel.pack(anchor="w", fill="x", pady=(8, 0))
        self.export_dadata_btn = ctk.CTkButton(
            btn_row_excel,
            text="В Excel (DaData)",
            command=self._export_excel_dadata,
            width=130,
            state="disabled",
            **self._sec_btn,
        )
        self.export_dadata_btn.pack(side="left", padx=(0, 8))
        self.export_yandex_btn = ctk.CTkButton(
            btn_row_excel,
            text="В Excel (Яндекс)",
            command=self._export_excel_yandex,
            width=140,
            state="disabled",
            **self._sec_btn,
        )
        self.export_yandex_btn.pack(side="left", padx=(0, 12))
        ctk.CTkLabel(btn_row_excel, text="Ctrl+Enter — стандартизация", text_color="gray60", font=font_std).pack(
            side="left"
        )

        sep_line = ctk.CTkFrame(outer, height=2, fg_color="gray35")
        sep_line.grid(row=grid_row, column=0, sticky="ew", pady=(0, 10))
        grid_row += 1

        self.full_outer = ctk.CTkFrame(outer, fg_color="transparent")
        self.full_outer.grid(row=grid_row, column=0, sticky="ew")
        grid_row += 1
        self.toggle_btn = ctk.CTkButton(
            self.full_outer,
            text="Показать полный вывод",
            command=self._toggle_full,
            **self._sec_btn,
        )
        self.toggle_btn.pack(anchor="w")
        self.full_text = ctk.CTkTextbox(
            self.full_outer,
            font=font_mono,
            wrap="word",
            height=200,
        )
        self.full_text.configure(state="disabled")

        status_wrap = ctk.CTkFrame(outer, corner_radius=8, fg_color=("gray85", "gray20"))
        status_wrap.grid(row=grid_row, column=0, sticky="ew", pady=(12, 0))
        self.status = tk.StringVar(value="Готово.")
        ctk.CTkLabel(status_wrap, textvariable=self.status, font=font_std, anchor="w").pack(
            fill="x", padx=14, pady=10
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
        win = ctk.CTkToplevel(self.root)
        win.title("Прокси SOCKS5")
        win.geometry("440x300")
        win.minsize(400, 260)
        win.transient(self.root)
        win.grab_set()

        outer = ctk.CTkFrame(win, fg_color="transparent")
        outer.pack(fill="both", expand=True, padx=16, pady=16)
        outer.grid_columnconfigure(1, weight=1)

        host_v = tk.StringVar(value=self._socks_host)
        port_v = tk.StringVar(value=self._socks_port)
        user_v = tk.StringVar(value=self._socks_user)
        pass_v = tk.StringVar(value=self._socks_password)

        row = 0
        ctk.CTkLabel(outer, text="IP или хост").grid(row=row, column=0, sticky="w", padx=(0, 10), pady=(0, 8))
        ctk.CTkEntry(outer, textvariable=host_v, width=280).grid(row=row, column=1, sticky="ew", pady=(0, 8))
        row += 1
        ctk.CTkLabel(outer, text="Порт").grid(row=row, column=0, sticky="w", padx=(0, 10), pady=(0, 8))
        ctk.CTkEntry(outer, textvariable=port_v, width=280).grid(row=row, column=1, sticky="ew", pady=(0, 8))
        row += 1
        ctk.CTkLabel(outer, text="Логин").grid(row=row, column=0, sticky="w", padx=(0, 10), pady=(0, 8))
        ctk.CTkEntry(outer, textvariable=user_v, width=280).grid(row=row, column=1, sticky="ew", pady=(0, 8))
        row += 1
        ctk.CTkLabel(outer, text="Пароль").grid(row=row, column=0, sticky="w", padx=(0, 10), pady=(0, 8))
        ctk.CTkEntry(outer, textvariable=pass_v, show="*", width=280).grid(row=row, column=1, sticky="ew", pady=(0, 8))
        row += 1
        ctk.CTkLabel(
            outer,
            text="Протокол: SOCKS5. Пустой логин и пароль — без авторизации.",
            text_color="gray65",
        ).grid(row=row, column=0, columnspan=2, sticky="w", pady=(4, 12))
        row += 1

        btn_row = ctk.CTkFrame(outer, fg_color="transparent")
        btn_row.grid(row=row, column=0, columnspan=2, sticky="ew")

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

        ctk.CTkButton(btn_row, text="ОК", command=on_ok, width=100).pack(side="right", padx=(8, 0))
        ctk.CTkButton(btn_row, text="Отмена", command=on_cancel, width=100, **self._sec_btn).pack(side="right")
        win.bind("<Return>", lambda e: on_ok())
        win.bind("<Escape>", lambda e: on_cancel())

    def _toggle_cred_fields(self) -> None:
        self._cred_fields_expanded = not self._cred_fields_expanded
        if self._cred_fields_expanded:
            self._cred_body.grid(row=1, column=0, columnspan=2, sticky="ew", padx=12)
            self._cred_toggle_btn.configure(text="▼ Свернуть поля ключей")
        else:
            self._cred_body.grid_remove()
            self._cred_toggle_btn.configure(text="▶ Показать поля ключей")

    def _set_loading(self, loading: bool) -> None:
        st = "disabled" if loading else "normal"
        self.run_btn.configure(state=st)
        self.yandex_btn.configure(state=st)
        self.balance_btn.configure(state=st)
        self.stats_btn.configure(state=st)
        self.yandex_dev_btn.configure(state=st)
        self._proxy_settings_btn.configure(state=st)
        self.export_dadata_btn.configure(
            state="disabled"
            if loading
            else ("normal" if self._last_dadata_excel_rows else "disabled")
        )
        self.export_yandex_btn.configure(
            state="disabled"
            if loading
            else ("normal" if self._last_yandex_excel_rows else "disabled")
        )

    def _show_stats_dialog(self, data: Dict[str, Any]) -> None:
        win = ctk.CTkToplevel(self.root)
        win.title("Статистика DaData")
        win.geometry("560x480")
        win.minsize(520, 420)

        outer = ctk.CTkFrame(win, fg_color="transparent")
        outer.pack(fill="both", expand=True, padx=12, pady=12)

        ctk.CTkLabel(outer, text="Суточная статистика использования API", font=ctk.CTkFont(size=16, weight="bold")).pack(
            anchor="w", pady=(0, 8)
        )

        txt = ctk.CTkTextbox(outer, wrap="word", height=360)
        txt.pack(fill="both", expand=True)
        _tb_set(txt, format_daily_stats_human(data), readonly=True)

        btn_row = ctk.CTkFrame(outer, fg_color="transparent")
        btn_row.pack(fill="x", pady=(12, 0))

        def copy_json() -> None:
            payload = json.dumps(data, ensure_ascii=False, indent=2, default=str)
            win.clipboard_clear()
            win.clipboard_append(payload)
            win.update()

        ctk.CTkButton(btn_row, text="Копировать JSON", command=copy_json, **self._sec_btn).pack(side="left")
        ctk.CTkButton(btn_row, text="Закрыть", command=win.destroy, **self._sec_btn).pack(side="right")

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
        win = ctk.CTkToplevel(self.root)
        win.title("Лимиты — API кабинета Яндекса")
        win.geometry("640x520")
        win.minsize(560, 440)

        outer = ctk.CTkFrame(win, fg_color="transparent")
        outer.pack(fill="both", expand=True, padx=12, pady=12)

        ctk.CTkLabel(
            outer,
            text="Проекты, сервисы и суточные лимиты (геокодер, JS API и др.)",
            font=ctk.CTkFont(size=15, weight="bold"),
        ).pack(anchor="w", pady=(0, 8))

        txt = ctk.CTkTextbox(outer, font=ctk.CTkFont(family="Consolas", size=12), wrap="word", height=400)
        txt.pack(fill="both", expand=True)
        _tb_set(txt, text, readonly=True)

        btn_row = ctk.CTkFrame(outer, fg_color="transparent")
        btn_row.pack(fill="x", pady=(12, 0))

        def copy_all() -> None:
            win.clipboard_clear()
            win.clipboard_append(text)
            win.update()

        ctk.CTkButton(btn_row, text="Копировать", command=copy_all, **self._sec_btn).pack(side="left")
        ctk.CTkButton(btn_row, text="Закрыть", command=win.destroy, **self._sec_btn).pack(side="right")

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

        raw = self.input_text.get(_TI, "end")
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
            self.full_text.pack(fill="both", expand=True, pady=(10, 0))
            self.toggle_btn.configure(text="Скрыть полный вывод")
        else:
            self.full_text.pack_forget()
            self.toggle_btn.configure(text="Показать полный вывод")

    def _set_short(self, text: str) -> None:
        _tb_set(self.short_text, text, readonly=True)

    def _set_full(self, text: str) -> None:
        _tb_set(self.full_text, text, readonly=True)

    def _show_result_single(self) -> None:
        if self._result_yandex_split_visible:
            self._yandex_result_pane.pack_forget()
            self._result_yandex_split_visible = False
        self.short_text.pack(fill="both", expand=True)

    def _show_result_yandex_split(self) -> None:
        if self._result_yandex_split_visible:
            return
        self.short_text.pack_forget()
        self._yandex_result_pane.pack(fill="both", expand=True)
        self._result_yandex_split_visible = True

    def _set_yandex_result_panes(self, coords: str, addresses: str) -> None:
        for widget, text in (
            (self.yandex_coords_text, coords),
            (self.yandex_addr_text, addresses),
        ):
            _tb_set(widget, text, readonly=True)

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

        raw = self.input_text.get(_TI, "end")
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
        self.export_dadata_btn.configure(state="normal" if self._last_dadata_excel_rows else "disabled")
        self.export_yandex_btn.configure(state="normal" if self._last_yandex_excel_rows else "disabled")

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
