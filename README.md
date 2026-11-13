# multitool-playground

Пробный публичный репозиторий, созданный во время работы с MultiTool.

- `multitool_bridge.py` — мост: сообщения из мессенджеров → MultiTool
- `notes.md` — короткие заметки
- `demo.csv` — пример таблицы для экспериментов

## multitool_bridge.py

Слушает мессенджеры, приводит сообщения к единому формату и передаёт их в приёмник.

Источники:

- `--telegram` — официальный Telegram Bot API, long polling; токен из `TELEGRAM_BOT_TOKEN`;
- `--vk` — официальный Long Poll API сообщества ВКонтакте; нужны `VK_GROUP_TOKEN` и `VK_GROUP_ID`;
- `--webhook` — универсальный POST-приёмник: подключается любой мессенджер с исходящим вебхуком или своим коннектором.

Приёмники (`--sink`, можно несколько):

- `http://host:port/path` — POST с JSON `{"type": "messenger_message", "source", "chat_id", "user_id", "user_name", "text", "ts"}`;
- путь к файлу — JSONL, по одному сообщению на строку;
- `stdout`.

MultiTool пока не публикует API автоматизации, поэтому адрес приёмника задаётся вручную.
Если приёмник недоступен, сообщения складываются в `bridge-spool.jsonl` и доотправляются позже.

### Запуск

```bash
export TELEGRAM_BOT_TOKEN=123:abc

# Telegram -> локальный приёмник
python multitool_bridge.py run --telegram \
    --sink http://127.0.0.1:8787/inbox --demo-receiver 8787

# любой мессенджер через вебхук -> файл
python multitool_bridge.py run --webhook 0.0.0.0:8080 --sink out/messages.jsonl

# самопроверка без сети и токенов
python multitool_bridge.py self-test
```

Только стандартная библиотека Python 3.9+. Токены читаются из переменных окружения и не попадают в логи.

### Ограничения

- поддерживаются только официальные API ботов и сообществ; автоматизация личного аккаунта не входит в задачу;
- передаётся только текст, вложения пока не разбираются;
- поток односторонний: мессенджер → приёмник, ответы обратно не отправляются.
