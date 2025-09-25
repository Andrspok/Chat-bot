# Chat-bot v2 (эвристики)

Минимальный каркас Telegram-бота на `python-telegram-bot` v20, без LLM.
Хранит заявки локально (JSONL), классифицирует эвристиками и пишет логи.

## Быстрый старт (Windows, PowerShell)

```powershell
cd C:\Users\AAVolodin\Chat-bot

# 1) Python 3.11 установлен. Создаём виртуальное окружение
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1

# 2) Обновляем pip и ставим зависимости
python -m pip install --upgrade pip
pip install -r requirements.txt

# 3) Настраиваем токен и админов
copy .env.example .env
# Отредактируйте .env

# 4) Запуск
python -m src.bot
```

Логи пишутся в `logs/bot.log`. Заявки — в `data/tickets.jsonl`.
