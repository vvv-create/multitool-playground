#!/usr/bin/env python3
"""multitool_bridge.py - мост между мессенджерами и MultiTool.

Что делает
----------
Слушает один или несколько мессенджеров, приводит сообщения к единому
формату и передаёт их в приёмник: HTTP-эндпоинт, JSONL-файл или stdout.

Поддерживаемые источники
------------------------
* telegram - официальный Bot API, long polling (getUpdates).
* vk       - официальный Long Poll API сообщества.
* webhook  - универсальный приёмник POST-запросов: сюда подключается
             любой мессенджер, у которого есть исходящий вебхук или
             собственный коннектор.

Важно про MultiTool
-------------------
У MultiTool нет опубликованного API автоматизации, поэтому адрес
приёмника задаётся вручную (--sink http://host:port/path). Для проверки
в комплекте есть демо-приёмник: --demo-receiver 8787.

Только стандартная библиотека Python 3.9+. Токены читаются из
переменных окружения и не попадают в логи.

Примеры
-------
    export TELEGRAM_BOT_TOKEN=123:abc

    # сообщения из Telegram -> локальный приёмник
    python multitool_bridge.py run --telegram \
        --sink http://127.0.0.1:8787/inbox --demo-receiver 8787

    # только приём вебхуков от любого мессенджера
    python multitool_bridge.py run --webhook 0.0.0.0:8080 \
        --sink out/messages.jsonl

    # самопроверка без сети и токенов
    python multitool_bridge.py self-test
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import queue
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Callable, Iterable, Optional

LOG = logging.getLogger("bridge")

Emit = Callable[["Message"], None]


# --------------------------------------------------------------------------
# Модель сообщения
# --------------------------------------------------------------------------

@dataclass
class Message:
    """Единый формат сообщения для всех источников."""

    source: str
    chat_id: str
    user_id: str
    user_name: str
    text: str
    ts: float = field(default_factory=lambda: round(time.time(), 3))

    def to_dict(self) -> dict:
        return asdict(self)


def message_from_dict(body: dict, default_source: str = "webhook") -> Optional[Message]:
    """Собирает Message из произвольного JSON вебхука."""
    text = body.get("text") or body.get("message") or body.get("body")
    if not isinstance(text, str) or not text.strip():
        return None
    return Message(
        source=str(body.get("source") or default_source),
        chat_id=str(body.get("chat_id") or body.get("chat") or body.get("peer_id") or ""),
        user_id=str(body.get("user_id") or body.get("from_id") or body.get("user") or ""),
        user_name=str(body.get("user_name") or body.get("author") or ""),
        text=text.strip(),
    )


# --------------------------------------------------------------------------
# Приёмники
# --------------------------------------------------------------------------

class Sink:
    """Куда отдавать сообщения."""

    name = "sink"

    def deliver(self, message: Message) -> None:  # pragma: no cover - интерфейс
        raise NotImplementedError

    def close(self) -> None:
        pass


class StdoutSink(Sink):
    name = "stdout"

    def deliver(self, message: Message) -> None:
        print(json.dumps(message.to_dict(), ensure_ascii=False), flush=True)


class JsonlSink(Sink):
    """Дописывает сообщения в файл, по одному JSON на строку."""

    name = "jsonl"

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        if self.path.parent and str(self.path.parent) not in ("", "."):
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def deliver(self, message: Message) -> None:
        line = json.dumps(message.to_dict(), ensure_ascii=False)
        with self._lock, self.path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")


class HttpSink(Sink):
    """POST в эндпоинт приёмника; при недоступности складывает в очередь повтора."""

    name = "http"

    def __init__(
        self,
        url: str,
        token: str = "",
        timeout: float = 10.0,
        spool: str | Path = "bridge-spool.jsonl",
    ) -> None:
        self.url = url
        self.token = token
        self.timeout = timeout
        self.spool = Path(spool)
        if str(self.spool.parent) not in ("", "."):
            self.spool.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._flusher: Optional[threading.Thread] = None
        self._stop = threading.Event()

    # -- отправка ---------------------------------------------------------
    def _post(self, payload: dict) -> None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers = {"Content-Type": "application/json; charset=utf-8"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        request = urllib.request.Request(self.url, data=data, headers=headers, method="POST")
        with urllib.request.urlopen(request, timeout=self.timeout) as response:
            response.read(1024)

    def deliver(self, message: Message) -> None:
        payload = {"type": "messenger_message", **message.to_dict()}
        try:
            self._post(payload)
        except (urllib.error.URLError, OSError, TimeoutError) as exc:
            LOG.warning("приёмник недоступен (%s); сообщение в очередь повтора", exc)
            self._spool_add(payload)
            self._ensure_flusher()
        else:
            self._flush_once()

    # -- очередь повтора --------------------------------------------------
    def _spool_add(self, payload: dict) -> None:
        line = json.dumps(payload, ensure_ascii=False)
        with self._lock, self.spool.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")

    def _flush_once(self) -> None:
        if not self.spool.exists():
            return
        with self._lock:
            try:
                lines = [ln for ln in self.spool.read_text(encoding="utf-8").splitlines() if ln.strip()]
            except OSError:
                return
            if not lines:
                return
            remaining: list[str] = []
            for line in lines:
                try:
                    self._post(json.loads(line))
                except (urllib.error.URLError, OSError, ValueError, TimeoutError):
                    remaining.append(line)
            try:
                if remaining:
                    self.spool.write_text("\n".join(remaining) + "\n", encoding="utf-8")
                else:
                    self.spool.unlink(missing_ok=True)
            except OSError:
                pass

    def _ensure_flusher(self) -> None:
        if self._flusher and self._flusher.is_alive():
            return
        self._stop.clear()

        def loop() -> None:
            while not self._stop.wait(15.0):
                self._flush_once()
                if not self.spool.exists():
                    return

        self._flusher = threading.Thread(target=loop, name="spool-flusher", daemon=True)
        self._flusher.start()

    def close(self) -> None:
        self._stop.set()


def build_sink(target: str) -> Sink:
    """Создаёт приёмник по строке из --sink."""
    lowered = target.lower()
    if lowered in ("stdout", "-"):
        return StdoutSink()
    if lowered.startswith(("http://", "https://")):
        return HttpSink(target)
    return JsonlSink(target)


# --------------------------------------------------------------------------
# Источники
# --------------------------------------------------------------------------

class Adapter:
    """Блокирующий цикл, который вызывает emit(message)."""

    name = "adapter"

    def run(self, emit: Emit) -> None:  # pragma: no cover - интерфейс
        raise NotImplementedError


class TelegramAdapter(Adapter):
    """Официальный Bot API, long polling. Нужен TELEGRAM_BOT_TOKEN."""

    name = "telegram"
    api = "https://api.telegram.org/bot{token}/{method}"

    def __init__(self, token: str, poll_timeout: int = 25) -> None:
        self.token = token
        self.poll_timeout = poll_timeout

    def _call(self, method: str, **params) -> dict:
        url = self.api.format(token=self.token, method=method)
        data = urllib.parse.urlencode(params).encode() if params else None
        with urllib.request.urlopen(url, data=data, timeout=self.poll_timeout + 15) as response:
            return json.loads(response.read().decode("utf-8"))

    def run(self, emit: Emit) -> None:
        LOG.info("telegram: long polling запущен")
        offset: Optional[int] = None
        while True:
            params: dict = {"timeout": self.poll_timeout, "allowed_updates": json.dumps(["message"])}
            if offset is not None:
                params["offset"] = offset
            try:
                response = self._call("getUpdates", **params)
            except urllib.error.HTTPError as exc:
                LOG.error("telegram: HTTP %s, пауза 10 с", exc.code)
                time.sleep(10)
                continue
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                LOG.warning("telegram: сеть недоступна (%s), пауза 5 с", exc)
                time.sleep(5)
                continue

            for update in response.get("result", []):
                offset = int(update["update_id"]) + 1
                raw = update.get("message") or {}
                text = raw.get("text")
                if not text:
                    continue
                sender = raw.get("from") or {}
                name = " ".join(
                    part for part in (sender.get("first_name"), sender.get("last_name")) if part
                ) or sender.get("username") or ""
                emit(
                    Message(
                        source=self.name,
                        chat_id=str((raw.get("chat") or {}).get("id", "")),
                        user_id=str(sender.get("id", "")),
                        user_name=name,
                        text=text,
                    )
                )


class VkAdapter(Adapter):
    """Long Poll API сообщества ВКонтакте. Нужен VK_GROUP_TOKEN и VK_GROUP_ID."""

    name = "vk"
    api = "https://api.vk.com/method/"
    version = "5.199"

    def __init__(self, token: str, group_id: str, version: str = version) -> None:
        self.token = token
        self.group_id = str(group_id)
        self.version = version

    def _call(self, method: str, **params) -> dict:
        params.update({"access_token": self.token, "v": self.version})
        data = urllib.parse.urlencode(params).encode()
        with urllib.request.urlopen(self.api + method, data=data, timeout=30) as response:
            document = json.loads(response.read().decode("utf-8"))
        if "error" in document:
            raise RuntimeError(document["error"].get("error_msg", "vk api error"))
        return document["response"]

    def run(self, emit: Emit) -> None:
        LOG.info("vk: long poll запущен")
        while True:
            try:
                server_info = self._call("groups.getLongPollServer", group_id=self.group_id)
            except (urllib.error.URLError, TimeoutError, OSError, RuntimeError) as exc:
                LOG.warning("vk: не удалось получить long poll сервер (%s), пауза 10 с", exc)
                time.sleep(10)
                continue

            server = server_info["server"]
            key = server_info["key"]
            ts = server_info["ts"]

            while True:
                url = f"{server}?act=a_check&key={key}&ts={ts}&wait=25"
                try:
                    with urllib.request.urlopen(url, timeout=40) as response:
                        document = json.loads(response.read().decode("utf-8"))
                except (urllib.error.URLError, TimeoutError, OSError) as exc:
                    LOG.warning("vk: обрыв long poll (%s), переподключение", exc)
                    break

                if document.get("failed"):
                    break  # пересоздаём ключ

                ts = document.get("ts", ts)
                for update in document.get("updates", []):
                    if update.get("type") != "message_new":
                        continue
                    raw = (update.get("object") or {}).get("message") or {}
                    text = raw.get("text")
                    if not text:
                        continue
                    emit(
                        Message(
                            source=self.name,
                            chat_id=str(raw.get("peer_id", "")),
                            user_id=str(raw.get("from_id", "")),
                            user_name="",
                            text=text,
                        )
                    )


class WebhookServer(Adapter):
    """Приём POST-запросов от любого мессенджера или коннектора."""

    name = "webhook"

    def __init__(self, host: str, port: int, path: str = "/") -> None:
        self.host = host
        self.port = port
        self.path = path or "/"

    def run(self, emit: Emit) -> None:
        path = self.path
        server_name = self.name

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, fmt: str, *args) -> None:  # тише в логах
                LOG.debug("%s - %s", self.address_string(), fmt % args)

            def _json(self, code: int, payload: dict) -> None:
                body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                self.send_response(code)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self) -> None:  # noqa: N802 - имя задано базовым классом
                if self.path.startswith("/health"):
                    self._json(200, {"ok": True, "service": "multitool-bridge"})
                else:
                    self._json(404, {"ok": False, "error": "not found"})

            def do_POST(self) -> None:  # noqa: N802
                if not self.path.startswith(path):
                    self._json(404, {"ok": False, "error": "not found"})
                    return
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length) if length else b""
                try:
                    body = json.loads(raw.decode("utf-8") or "{}")
                except (ValueError, UnicodeDecodeError):
                    self._json(400, {"ok": False, "error": "invalid json"})
                    return
                message = message_from_dict(body, default_source=server_name)
                if message is None:
                    self._json(422, {"ok": False, "error": "text is required"})
                    return
                emit(message)
                self._json(200, {"ok": True})

        server = ThreadingHTTPServer((self.host, self.port), Handler)
        LOG.info("webhook: слушаю http://%s:%s%s", self.host, self.port, path)
        try:
            server.serve_forever()
        finally:
            server.server_close()


# --------------------------------------------------------------------------
# Демонстрационный приёмник (проверка сквозного пути)
# --------------------------------------------------------------------------

class DemoReceiver:
    """Минимальный приёмник: печатает сообщения и пишет их в JSONL."""

    def __init__(self, host: str, port: int, path: str = "/inbox", dump: str = "received.jsonl") -> None:
        self.host = host
        self.port = port
        self.path = path
        self.dump = Path(dump)
        self._lock = threading.Lock()

    def serve(self) -> None:
        receiver = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, fmt: str, *args) -> None:
                LOG.debug("%s - %s", self.address_string(), fmt % args)

            def do_GET(self) -> None:  # noqa: N802
                body = b'{"ok":true,"service":"demo-receiver"}'
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_POST(self) -> None:  # noqa: N802
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length) if length else b""
                try:
                    payload = json.loads(raw.decode("utf-8") or "{}")
                except (ValueError, UnicodeDecodeError):
                    payload = {"raw": raw.decode("utf-8", "replace")}
                print("[receiver]", json.dumps(payload, ensure_ascii=False), flush=True)
                with receiver._lock, receiver.dump.open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
                body = b'{"ok":true}'
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        server = ThreadingHTTPServer((self.host, self.port), Handler)
        LOG.info("demo-receiver: слушаю http://%s:%s%s", self.host, self.port, self.path)
        try:
            server.serve_forever()
        finally:
            server.server_close()


# --------------------------------------------------------------------------
# Ядро: очередь, фильтры, воркер доставки
# --------------------------------------------------------------------------

class Bridge:
    """Принимает сообщения от источников и раздаёт их приёмникам."""

    def __init__(
        self,
        sinks: Iterable[Sink],
        allow_chats: Iterable[str] = (),
    ) -> None:
        self.sinks = list(sinks)
        self.allow_chats = {str(item) for item in allow_chats if str(item).strip()}
        self.queue: "queue.Queue[Message]" = queue.Queue()
        self._worker = threading.Thread(target=self._deliver_loop, name="delivery", daemon=True)
        self._worker.start()

    def emit(self, message: Message) -> None:
        if self.allow_chats and message.chat_id not in self.allow_chats:
            LOG.debug("пропуск чата %s (нет в allow_chats)", message.chat_id)
            return
        LOG.info("+ %s/%s: %s", message.source, message.user_name or message.user_id, message.text[:80])
        self.queue.put(message)

    def _deliver_loop(self) -> None:
        while True:
            message = self.queue.get()
            for sink in self.sinks:
                try:
                    sink.deliver(message)
                except Exception as exc:  # один сбойный приёмник не должен ронять мост
                    LOG.error("приёмник %s: %s", sink.name, exc)
            self.queue.task_done()

    def close(self) -> None:
        for sink in self.sinks:
            try:
                sink.close()
            except Exception:
                pass


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def _parse_hostport(value: str) -> tuple[str, int]:
    if ":" not in value:
        return value, 8080
    host, _, port = value.rpartition(":")
    return (host or "0.0.0.0"), int(port)


def _build_adapters(args) -> list[Adapter]:
    adapters: list[Adapter] = []

    if args.telegram:
        token = os.environ.get(args.telegram_token_env, "")
        if not token:
            raise SystemExit(f"нет токена: задайте переменную {args.telegram_token_env}")
        adapters.append(TelegramAdapter(token))

    if args.vk:
        token = os.environ.get(args.vk_token_env, "")
        group_id = os.environ.get(args.vk_group_id_env, "")
        if not token or not group_id:
            raise SystemExit(
                f"нет данных VK: нужны {args.vk_token_env} и {args.vk_group_id_env}"
            )
        adapters.append(VkAdapter(token, group_id))

    if args.webhook:
        host, port = _parse_hostport(args.webhook)
        adapters.append(WebhookServer(host, port, args.webhook_path))

    if not adapters:
        raise SystemExit("не выбран ни один источник: --telegram, --vk или --webhook")

    return adapters


def cmd_run(args) -> int:
    sinks: list[Sink] = []
    for target in args.sink or ["stdout"]:
        sinks.append(build_sink(target))
    bridge = Bridge(sinks, allow_chats=args.allow_chat)

    if args.demo_receiver is not None:
        host, port = _parse_hostport(str(args.demo_receiver))
        receiver = DemoReceiver(host, port, path=args.demo_receiver_path, dump=args.demo_receiver_dump)
        threading.Thread(target=receiver.serve, name="demo-receiver", daemon=True).start()
        time.sleep(0.3)

    adapters = _build_adapters(args)
    for adapter in adapters:
        threading.Thread(target=adapter.run, args=(bridge.emit,), name=adapter.name, daemon=True).start()

    LOG.info("мост запущен: источники=%s, приёмники=%s",
             ", ".join(a.name for a in adapters), ", ".join(s.name for s in sinks))
    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        LOG.info("остановка по Ctrl+C")
    finally:
        bridge.close()
    return 0


def cmd_self_test(args) -> int:
    """Проверяет путь webhook -> Bridge -> HttpSink -> приёмник без интернета."""
    received: list[dict] = []

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt: str, *args) -> None:
            pass

        def do_POST(self) -> None:  # noqa: N802
            length = int(self.headers.get("Content-Length") or 0)
            received.append(json.loads(self.rfile.read(length).decode("utf-8")))
            body = b'{"ok":true}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()

    sink = HttpSink(f"http://127.0.0.1:{port}/inbox", spool="self-test-spool.jsonl")
    bridge = Bridge([sink])
    bridge.emit(Message(source="self-test", chat_id="42", user_id="7", user_name="Тест", text="привет"))

    deadline = time.time() + 5.0
    while time.time() < deadline and not received:
        time.sleep(0.05)

    bridge.close()
    server.shutdown()
    Path("self-test-spool.jsonl").unlink(missing_ok=True)

    if not received:
        print("FAIL: приёмник не получил сообщение", file=sys.stderr)
        return 1

    payload = received[0]
    assert payload["type"] == "messenger_message", payload
    assert payload["text"] == "привет", payload
    print("OK: сообщение прошло путь webhook -> bridge -> sink")
    print(json.dumps(payload, ensure_ascii=False))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="multitool_bridge.py",
        description="Мост мессенджеров в MultiTool",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="подробный лог")

    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="запустить мост")
    run.add_argument("--telegram", action="store_true", help="слушать Telegram Bot API")
    run.add_argument("--telegram-token-env", default="TELEGRAM_BOT_TOKEN")
    run.add_argument("--vk", action="store_true", help="слушать VK Long Poll")
    run.add_argument("--vk-token-env", default="VK_GROUP_TOKEN")
    run.add_argument("--vk-group-id-env", default="VK_GROUP_ID")
    run.add_argument("--webhook", metavar="HOST:PORT", help="принимать POST от любого мессенджера")
    run.add_argument("--webhook-path", default="/", help="префикс пути для вебхука")
    run.add_argument("--sink", action="append", metavar="TARGET",
                     help="stdout | URL | путь к JSONL; можно несколько раз")
    run.add_argument("--allow-chat", action="append", default=[],
                     help="разрешённый chat_id (можно несколько раз)")
    run.add_argument("--demo-receiver", metavar="HOST:PORT", default=None,
                     help="поднять демо-приёмник на это адрес")
    run.add_argument("--demo-receiver-path", default="/inbox")
    run.add_argument("--demo-receiver-dump", default="received.jsonl")
    run.set_defaults(func=cmd_run)

    self_test = sub.add_parser("self-test", help="самопроверка без сети")
    self_test.set_defaults(func=cmd_self_test)

    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
