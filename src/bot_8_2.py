# ============================================
# Chat-bot v2 — SINGLE FILE (bot.py)
# Version: v8.2 (2025-09-24)
# База: v8.1
# FIX: Экспорт Excel — сериализация сложных полей (dict/list/tuple) в листе "events".
# Команды экспорта: /export_excel, /export_csv
# ============================================

import os
import io
import csv
import json
import time
import uuid
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, UTC, timedelta  # timezone-aware UTC

from dotenv import load_dotenv
from loguru import logger
from rapidfuzz import fuzz
from html import escape as html_escape

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    InputFile,
)
from telegram.error import TelegramError
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

# ============================================
# ИНФРА / ЛОГИ / .ENV
# ============================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent  # ...\Chat-bot
DATA_DIR = PROJECT_ROOT / "data"
LOGS_DIR = PROJECT_ROOT / "logs"
DATA_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

def setup_logging(logs_dir: Path) -> None:
    logs_dir.mkdir(parents=True, exist_ok=True)
    logger.remove()
    logger.add(
        sink=lambda m: print(m, end=""),
        format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level}</level> | {message}",
        level="INFO",
        backtrace=False,
        diagnose=False,
    )
    logger.add(
        logs_dir / "bot.log",
        rotation="5 MB",
        retention="7 days",
        compression="zip",
        encoding="utf-8",
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level} | {message}",
        level="INFO",
        backtrace=False,
        diagnose=False,
    )

def load_env(project_root: Path) -> None:
    env_path = project_root / ".env"
    if env_path.exists():
        load_dotenv(env_path)
    else:
        load_dotenv()

# ============================================
# ПЕРСИСТ (JSONL) + ОПЕРАТИВНОЕ СОСТОЯНИЕ
# ============================================

TICKETS_FILE = DATA_DIR / "tickets.jsonl"
FEEDBACK_FILE = DATA_DIR / "feedback.jsonl"

def _append_jsonl(path: Path, record: Dict[str, Any]) -> None:
    record = {**record, "ts": record.get("ts") or datetime.now(UTC).isoformat(timespec="seconds")}
    with path.open("a", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False)
        f.write("\n")

def save_ticket_event(event: Dict[str, Any]) -> None:
    _append_jsonl(TICKETS_FILE, event)

def save_feedback(event: Dict[str, Any]) -> None:
    _append_jsonl(FEEDBACK_FILE, event)

# Оперативное хранилище заявок до БД
TICKETS: Dict[str, Dict[str, Any]] = {}  # ticket_id -> ticket
PENDING_REJECT_COMMENT_BY_USER: Dict[int, str] = {}  # executor_id -> ticket_id

def new_ticket_id() -> str:
    return uuid.uuid4().hex[:8].upper()

# ============================================
# ПОЛЕЗНЫЕ УТИЛИТЫ
# ============================================

def user_link_html(user_id: int, name: str | None) -> str:
    safe = html_escape(name or "пользователь", quote=False)
    return f'<a href="tg://user?id={user_id}">{safe}</a>'

def get_admins() -> set[int]:
    return {int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()}

GROUP_TO_ENV = {"СВС": "CHAT_ID_SVS", "СГЭ": "CHAT_ID_SGE", "ССТ": "CHAT_ID_SST"}

def get_group_chat_id(group: str) -> Optional[int]:
    env_key = GROUP_TO_ENV.get(group)
    if not env_key:
        logger.error(f"Unknown group '{group}' (no env key)")
        return None
    val = os.getenv(env_key, "").strip()
    if not val:
        logger.error(f"Env {env_key} not set (group={group})")
        return None
    try:
        return int(val)
    except ValueError:
        logger.error(f"Env {env_key} must be integer chat id, got '{val}'")
        return None

def get_audit_chat_id() -> Optional[int]:
    val = os.getenv("AUDIT_CHAT_ID", "").strip()
    if not val:
        return None
    try:
        return int(val)
    except ValueError:
        logger.error(f"AUDIT_CHAT_ID must be integer chat id, got '{val}'")
        return None

async def audit_log(bot, text: str) -> None:
    chat_id = get_audit_chat_id()
    if chat_id is None:
        return
    try:
        await bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML", disable_web_page_preview=True)
    except Exception:
        logger.exception("audit_log failed")

# ============================================
# ЭВРИСТИКИ УТО — из вашей таблицы
# ============================================

GROUP_CATEGORIES: Dict[str, List[Dict[str, Any]]] = {
    "СВС": [
        {"title": "Настройка температуры / обдува",
         "kw": ["обдув", "кондиционер", "тепло", "холодно", "температура в помещении", "душно", "температурный режим",
                "температур", "режим", "прохлад"]},
        {"title": "Ремонт вентиляции / кондиционера",
         "kw": ["вентиляция", "кондиционер", "холодно", "душно", "жарко", "температура", "режим",
                "вентиляц", "температур"]},
        {"title": "Протечки",
         "kw": ["протечка", "капает", "потолок", "вода", "туалет", "раковина", "умывальник",
                "протеч", "капл"]},
        {"title": "Автоматическая установка пожаротушения",
         "kw": ["автоматическая установка пожаротушения", "проверка оборудования", "установка оборудования",
                "пожарный кран", "аупт", "пожаротушен", "спринклер", "ороситель"]},
        {"title": "Засор",
         "kw": ["засор", "туалет", "раковина", "умывальник", "вода", "сантехник", "засорение",
                "канализац", "забилось", "не уходит вода", "слив", "трап"]},
        {"title": "Неприятный запах",
         "kw": ["запах", "туалет", "воняет", "вентиляция", "канализация", "амбре", "смрад"]},
        {"title": "Отопление",
         "kw": ["отопление", "тепло", "холодно", "радиатор", "батаре", "котельн"]},
    ],
    "СГЭ": [
        {"title": "Замена освещения",
         "kw": ["лампочка", "освещение", "свет", "перегорела", "заменить лампу", "замена света"]},
        {"title": "Неисправность / монтаж розетки",
         "kw": ["розетка", "электричество", "контакт", "искрит", "штепсель", "монтаж розетки"]},
        {"title": "Электрощит / питание",
         "kw": ["электрощит", "питание", "свет", "щиток", "автомат", "фаза", "выбивает"]},
        {"title": "Эвакуационное освещение",
         "kw": ["освещение", "эвакуация", "свет", "эвакуацион", "exit"]},
        {"title": "Провода",
         "kw": ["провода", "оголенные", "опасность", "поражение электрическим током", "кабель", "проводка", "оголен", "оголён"]},
        {"title": "Направление освещения",
         "kw": ["освещение", "перенаправить", "свет", "лампочка", "свет на этаже", "направлен", "угол света", "перенастроить свет"]},
    ],
    "ССТ": [
        {"title": "Выключить / включить музыку",
         "kw": ["музыка", "громкость", "играет не громко", "музыкальное сопровождение", "включить музыку", "выключить музыку", "звук"]},
        {"title": "Датчики дымовые (отключение, демонтаж/монтаж)",
         "kw": ["датчик", "дым", "датчик дыма", "дымовой", "пожарный датчик", "отключить датчик", "демонтаж"]},
        {"title": "Оповещатель речевой (отключение, демонтаж/монтаж)",
         "kw": ["речевой оповещатель", "оповещатель", "громкоговор", "сирена"]},
        {"title": "Настройка/ремонт систем автоматики",
         "kw": ["система автоматики", "настройка системы", "ремонт автоматики", "проверка автоматики", "автоматика", "контроллер", "система управлен"]},
    ],
}

def _count_hits(text: str, patterns: List[str]) -> int:
    t = (text or "").lower()
    return sum(1 for p in patterns if p.lower() in t)

def _score_category(text: str, title: str, kw: List[str]) -> int:
    score = _count_hits(text, kw)
    if fuzz.partial_ratio(title.lower(), (text or "").lower()) >= 85:
        score += 1
    return score

def classify(text: str) -> Dict[str, Any]:
    best_group, best_category, best_score = "Неопределено", "Другое", 0
    hits: Dict[str, int] = {}
    for group, cats in GROUP_CATEGORIES.items():
        for c in cats:
            title, kw = c["title"], c["kw"]
            s = _score_category(text, title, kw)
            hits[f"{group}:{title}"] = s
            if s > best_score:
                best_score, best_group, best_category = s, group, title
    return {"group": best_group, "category": best_category, "confidence": 0.0, "hits": hits}

# ============================================
# РЕНДЕР/КНОПКИ ДЛЯ ГРУППОВОГО СООБЩЕНИЯ
# ============================================

def ticket_group_text(t: Dict[str, Any]) -> str:
    submit_link = user_link_html(t["submitter_id"], t.get("submitter_name"))
    body = html_escape((t.get("text") or "").strip(), quote=False)
    parts = [
        f"🆕 Заявка #{t['id']} (группа: {t['classification']['group']} / категория: {t['classification']['category']})",
        f"Автор: {submit_link}",
        "",
        body,
        "",
        f"Статус: <b>{html_escape(t['status'].upper(), quote=False)}</b>",
    ]
    if t.get("executor_id"):
        parts.append(f"Исполнитель: {user_link_html(t['executor_id'], t.get('executor_name'))}")
    if t.get("reject_comment"):
        parts.append(f"Комментарий отказа: {html_escape(t['reject_comment'], quote=False)}")
    if t.get("closed"):
        parts.append("Закрыто исполнителем. ✅")
    return "\n".join(parts)

def kb_initial(ticket_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✅ Принять", callback_data=f"t:accept:{ticket_id}")],
            [InlineKeyboardButton("⛔ Отклонить", callback_data=f"t:reject:{ticket_id}")],
        ]
    )

def kb_after_accept(ticket_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✅ Завершить", callback_data=f"t:complete:{ticket_id}")],
            [InlineKeyboardButton("⛔ Отклонить", callback_data=f"t:reject:{ticket_id}")],
        ]
    )

# ============================================
# ОТПРАВКА В ЧАТ ГРУППЫ
# ============================================

async def send_to_group(context_bot, t: Dict[str, Any]) -> Optional[Message]:
    group = t["classification"]["group"]
    chat_id = get_group_chat_id(group)
    logger.info(f"Routing ticket #{t['id']} to group='{group}' -> chat_id={chat_id}")
    if chat_id is None:
        return None
    text = ticket_group_text(t)
    try:
        msg = await context_bot.send_message(chat_id=chat_id, text=text, reply_markup=kb_initial(t["id"]), parse_mode="HTML")
        return msg
    except Exception:
        logger.exception(f"send_to_group failed (group={group}, chat_id={chat_id})")
        return None

# ============================================
# КОМАНДЫ
# ============================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Привет! Напишите заявку — я определю группу (СВС/СГЭ/ССТ) и категорию и отправлю в чат группы. "
        "Исполнитель сможет принять/отклонить/завершить; при завершении заявка сразу закрывается (вам придёт уведомление)."
    )

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Команды:\n"
        "/start — приветствие\n"
        "/help — помощь\n"
        "/whoami — показать ваш user_id и текущий chat_id\n"
        "/echo_chat_id_any — вернуть chat_id текущего чата (диагностика)\n"
        "/echo_chat_id — то же, но только для ADMIN_IDS\n"
        "/debug_env — показать chat_id групп и аудит-канала (ADMIN)\n"
        "/export_excel — выгрузить Excel со статистикой заявок\n"
        "/export_csv — выгрузить CSV со статистикой заявок\n"
        "Просто пришлите текст заявки."
    )

def admin_only(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not update.effective_user or update.effective_user.id not in get_admins():
            return await update.message.reply_text("Только для админов.")
        return await func(update, context)
    return wrapper

@admin_only
async def echo_chat_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"chat_id: {update.effective_chat.id}")

async def whoami(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u, c = update.effective_user, update.effective_chat
    await update.message.reply_text(f"user_id: {u.id if u else 'unknown'}\nchat_id: {c.id}\nchat_type: {c.type}")

async def echo_chat_id_any(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"chat_id: {update.effective_chat.id}")

@admin_only
async def debug_env(update: Update, context: ContextTypes.DEFAULT_TYPE):
    svs = get_group_chat_id("СВС")
    sge = get_group_chat_id("СГЭ")
    sst = get_group_chat_id("ССТ")
    audit = get_audit_chat_id()
    await update.message.reply_text(f"CHAT_ID_SVS={svs}\nCHAT_ID_SGE={sge}\nCHAT_ID_SST={sst}\nAUDIT_CHAT_ID={audit}")

# ============================================
# ПРИЁМ ТЕКСТА ОТ АВТОРА (классификация → подтверждение)
# + защита: если от пользователя ожидается комментарий к отказу, это сообщение не считается новой заявкой
# ============================================

CONFIRM_CB = "ticket_confirm"
REPORT_CB  = "ticket_report_mistake"

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return

    user_id = update.effective_user.id if update.effective_user else None
    if user_id and PENDING_REJECT_COMMENT_BY_USER.get(user_id):
        return

    text = update.message.text or ""
    u = update.effective_user
    ch = update.effective_chat

    try:
        result = classify(text)
        group = result.get("group", "Неопределено")
        category = result.get("category", "Другое")

        t_id = new_ticket_id()
        ticket = {
            "id": t_id,
            "submitter_id": u.id if u else None,
            "submitter_name": (u.full_name if u else None),
            "submitter_chat_id": ch.id if ch else None,
            "text": text,
            "classification": result,
            "status": "new",
        }
        context.user_data["last_ticket"] = ticket
        save_ticket_event({"event": "new_text", **ticket})

        kb = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("Подтвердить", callback_data=CONFIRM_CB)],
                [InlineKeyboardButton("Сообщить об ошибке", callback_data=REPORT_CB)],
            ]
        )
        msg = (
            "Предварительная классификация (УТО):\n"
            f"• Группа-исполнитель: <b>{group}</b>\n"
            f"• Категория: <b>{category}</b>\n"
            f"• Номер заявки: <b>#{t_id}</b>\n\n"
            "Если всё верно — подтвердите. Если нет — сообщите об ошибке."
        )
        await update.message.reply_html(msg, reply_markup=kb)

        await audit_log(context.bot, f"📝 <b>Новая заявка (черновик)</b> #{t_id} от {user_link_html(ticket['submitter_id'], ticket['submitter_name'])}\n"
                                     f"Группа: {group} / Категория: {category}")

    except Exception:
        logger.exception("handle_text failed")
        await update.message.reply_text("Ошибка обработки заявки. Попробуйте ещё раз или /help.")

# ============================================
# EXPORT: чтение событий и агрегация в таблицу
# ============================================

def _parse_iso(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        if ts.endswith("Z"):
            ts = ts[:-1]
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    except Exception:
        return None

def _dur_str(delta: Optional[timedelta]) -> str:
    if not delta:
        return ""
    total_sec = int(delta.total_seconds())
    h, rem = divmod(total_sec, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h:02d}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"

def load_events() -> List[Dict[str, Any]]:
    if not TICKETS_FILE.exists():
        return []
    events: List[Dict[str, Any]] = []
    with TICKETS_FILE.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                events.append(obj)
            except Exception:
                logger.exception("Bad JSON line in tickets.jsonl")
    def key_ts(e: Dict[str, Any]):
        return _parse_iso(e.get("ts")) or datetime.min.replace(tzinfo=UTC)
    events.sort(key=key_ts)
    return events

def aggregate_tickets(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    idx: Dict[str, Dict[str, Any]] = {}
    for e in events:
        t_id = e.get("id") or e.get("ticket_id")
        if not t_id:
            continue
        row = idx.setdefault(t_id, {
            "ticket_id": t_id,
            "group": (e.get("classification") or {}).get("group") or e.get("group"),
            "category": (e.get("classification") or {}).get("category") or e.get("category"),
            "author_id": e.get("submitter_id"),
            "author_name": e.get("submitter_name"),
            "executor_id": e.get("executor_id"),
            "executor_name": e.get("executor_name"),
            "created_ts": None,
            "queued_ts": None,
            "accepted_ts": None,
            "rejected_ts": None,
            "closed_ts": None,
            "reject_comment": None,
            "final_status": None,
        })
        ev = e.get("event")
        ts = _parse_iso(e.get("ts"))
        if ev == "new_text":
            row["created_ts"] = row["created_ts"] or ts
        elif ev == "queued_to_group":
            row["queued_ts"] = row["queued_ts"] or ts
        elif ev == "accepted":
            row["accepted_ts"] = row["accepted_ts"] or ts
            row["executor_id"] = e.get("executor_id") or row["executor_id"]
            row["executor_name"] = e.get("executor_name") or row["executor_name"]
        elif ev == "rejected":
            row["rejected_ts"] = row["rejected_ts"] or ts
            row["executor_id"] = e.get("executor_id") or row["executor_id"]
            row["executor_name"] = e.get("executor_name") or row["executor_name"]
            row["reject_comment"] = e.get("comment") or row["reject_comment"]
            row["final_status"] = "rejected"
        elif ev == "closed_by_executor":
            row["closed_ts"] = row["closed_ts"] or ts
            row["executor_id"] = e.get("executor_id") or row["executor_id"]
            row["executor_name"] = e.get("executor_name") or row["executor_name"]
            row["final_status"] = "closed"

    for r in idx.values():
        if not r["final_status"]:
            if r["closed_ts"]:
                r["final_status"] = "closed"
            elif r["rejected_ts"]:
                r["final_status"] = "rejected"
            elif r["accepted_ts"]:
                r["final_status"] = "accepted"
            elif r["queued_ts"]:
                r["final_status"] = "queued"
            elif r["created_ts"]:
                r["final_status"] = "created"
            else:
                r["final_status"] = "unknown"

        created = r["created_ts"]
        queued  = r["queued_ts"]
        accepted= r["accepted_ts"]
        rejected= r["rejected_ts"]
        closed  = r["closed_ts"]

        r["time_to_queue"]   = _dur_str((queued - created) if (created and queued) else None)
        r["time_to_accept"]  = _dur_str((accepted - queued) if (accepted and queued) else None)
        r["time_in_progress"]= _dur_str((closed - accepted) if (closed and accepted) else None)
        end_ts = closed or rejected
        r["total_time"]      = _dur_str((end_ts - created) if (end_ts and created) else None)

        for k in ("created_ts", "queued_ts", "accepted_ts", "rejected_ts", "closed_ts"):
            v = r[k]
            r[k] = v.isoformat(timespec="seconds") if isinstance(v, datetime) else ""

    rows = list(idx.values())
    rows.sort(key=lambda x: x["created_ts"] or "")
    return rows

def write_csv(rows: List[Dict[str, Any]], path: Path) -> None:
    if not rows:
        header = [
            "ticket_id","group","category","author_id","author_name","executor_id","executor_name",
            "created_ts","queued_ts","accepted_ts","rejected_ts","closed_ts",
            "time_to_queue","time_to_accept","time_in_progress","total_time","final_status","reject_comment"
        ]
        with path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f, delimiter=";")
            writer.writerow(header)
        return

    header = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(header)
        for r in rows:
            writer.writerow([r.get(k, "") for k in header])

# --- NEW: безопасное приведение к ячейке Excel
def _to_excel_cell(v: Any) -> Any:
    if v is None:
        return ""
    if isinstance(v, (str, int, float, bool)):
        return v
    if isinstance(v, datetime):
        # чтобы не путать часовые пояса, кладём строкой ISO
        return v.isoformat(timespec="seconds")
    # dict / list / tuple / set / прочее — сериализуем в JSON
    try:
        return json.dumps(v, ensure_ascii=False)
    except Exception:
        return str(v)

def write_xlsx(rows: List[Dict[str, Any]], events: List[Dict[str, Any]], path: Path) -> Tuple[bool, str]:
    try:
        from openpyxl import Workbook
        from openpyxl.utils import get_column_letter
    except Exception:
        return False, "openpyxl не установлен"

    wb = Workbook()
    ws1 = wb.active
    ws1.title = "tickets"

    # tickets sheet
    if rows:
        header = list(rows[0].keys())
    else:
        header = [
            "ticket_id","group","category","author_id","author_name","executor_id","executor_name",
            "created_ts","queued_ts","accepted_ts","rejected_ts","closed_ts",
            "time_to_queue","time_to_accept","time_in_progress","total_time","final_status","reject_comment"
        ]
    ws1.append(header)
    for r in rows:
        ws1.append([_to_excel_cell(r.get(k, "")) for k in header])

    # events sheet (raw)
    ws2 = wb.create_sheet(title="events")
    if events:
        eheader = sorted({k for e in events for k in e.keys()})
    else:
        eheader = ["ts","event","id","ticket_id","submitter_id","submitter_name","group","category","executor_id","executor_name","comment","text"]
    ws2.append(eheader)
    for e in events:
        ws2.append([_to_excel_cell(e.get(k, "")) for k in eheader])

    # автоширина
    for ws in (ws1, ws2):
        for col_idx, _ in enumerate(ws.iter_cols(min_row=1, max_row=1, values_only=True), start=1):
            max_len = 0
            for cell in ws[get_column_letter(col_idx)]:
                try:
                    val = str(cell.value) if cell.value is not None else ""
                except Exception:
                    val = ""
                max_len = max(max_len, len(val))
            ws.column_dimensions[get_column_letter(col_idx)].width = min(max(10, max_len + 2), 60)

    # устойчивое сохранение — 3 попытки
    for i in range(3):
        try:
            wb.save(path)
            return True, "ok"
        except Exception as ex:
            if i == 2:
                return False, f"save failed: {ex}"
            time.sleep(0.5)

# ============================================
# EXPORT: команды бота
# ============================================

async def export_excel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    events = load_events()
    if not events:
        await update.message.reply_text("Пока нет данных для экспорта (файл data/tickets.jsonl пуст).")
        return

    rows = aggregate_tickets(events)
    ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    out_path = DATA_DIR | f"tickets_{ts}.xlsx" if hasattr(DATA_DIR, "__or__") else DATA_DIR / f"tickets_{ts}.xlsx"

    ok, msg = write_xlsx(rows, events, out_path)
    if not ok:
        await update.message.reply_text(f"Не удалось сформировать Excel: {msg}. Попробуйте /export_csv.")
        return

    try:
        with out_path.open("rb") as f:
            await context.bot.send_document(
                chat_id=chat_id,
                document=InputFile(f, filename=out_path.name),
                caption=f"Экспорт заявок (актуально на {ts} UTC)."
            )
        await audit_log(context.bot, f"📊 <b>Экспорт Excel</b> отправлен ({out_path.name})")
    except Exception:
        logger.exception("send excel failed")
        await update.message.reply_text("Не удалось отправить Excel в чат. Проверьте логи.")

async def export_csv(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    events = load_events()
    if not events:
        await update.message.reply_text("Пока нет данных для экспорта (файл data/tickets.jsonl пуст).")
        return

    rows = aggregate_tickets(events)
    ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    out_path = DATA_DIR / f"tickets_{ts}.csv"
    try:
        write_csv(rows, out_path)
        with out_path.open("rb") as f:
            await context.bot.send_document(
                chat_id=chat_id,
                document=InputFile(f, filename=out_path.name),
                caption=f"Экспорт заявок (CSV, {ts} UTC)."
            )
        await audit_log(context.bot, f"📊 <b>Экспорт CSV</b> отправлен ({out_path.name})")
    except Exception:
        logger.exception("send csv failed")
        await update.message.reply_text("Не удалось отправить CSV в чат. Проверьте логи.")

# ============================================
# CALLBACKS: подтверждение автора, действия исполнителя
# ============================================

async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return
    data = query.data or ""
    user = update.effective_user

    try:
        if data == CONFIRM_CB:
            ticket = context.user_data.get("last_ticket")
            if not ticket:
                await query.answer("Не найден контекст заявки, отправьте текст ещё раз.")
                return

            ticket["status"] = "queued"
            TICKETS[ticket["id"]] = ticket

            msg = await send_to_group(context.bot, ticket)
            if msg:
                ticket["group_chat_id"] = msg.chat.id
                ticket["group_message_id"] = msg.message_id
                save_ticket_event({"event": "queued_to_group", **ticket})
                await query.answer("Заявка отправлена в группу.")
                await query.edit_message_reply_markup(reply_markup=None)
                await audit_log(context.bot, f"📤 <b>Отправлена в группу</b> #{ticket['id']} → {ticket['classification']['group']} / {ticket['classification']['category']}")
            else:
                await query.answer("Не удалось отправить в чат группы. Проверьте настройки.")
                try:
                    await context.bot.send_message(
                        chat_id=ticket["submitter_chat_id"],
                        text="Не удалось отправить вашу заявку в чат группы. Проверьте настройки chat_id или обратитесь к администратору."
                    )
                except Exception:
                    pass
                return

        elif data == REPORT_CB:
            save_feedback({"user_id": user.id if user else None, "feedback": "heuristics_mistake"})
            await query.answer("Принято. Улучшим правила.")
            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass
            await audit_log(context.bot, f"⚠️ <b>Отправитель сообщил об ошибке эвристики</b> (user_id={user.id if user else 'unknown'})")

        elif data.startswith("t:"):
            parts = data.split(":")
            if len(parts) != 3:
                await query.answer("Некорректное действие.")
                return
            _, action, t_id = parts
            t = TICKETS.get(t_id)
            if not t:
                await query.answer("Заявка не найдена (возможно, бот перезапускался).")
                return

            executor_id = user.id if user else None
            executor_name = user.full_name if user else None

            if query.message and (t.get("group_chat_id") != query.message.chat.id or t.get("group_message_id") != query.message.message_id):
                await query.answer("Это сообщение уже устарело.")
                return

            if action == "accept":
                if t["status"] in {"accepted", "closed"}:
                    await query.answer("Уже в работе/закрыта.")
                    return

                t["status"] = "accepted"
                t["executor_id"] = executor_id
                t["executor_name"] = executor_name
                TICKETS[t_id] = t

                try:
                    await query.edit_message_text(
                        text=ticket_group_text(t),
                        reply_markup=kb_after_accept(t_id),
                        parse_mode="HTML",
                    )
                except Exception:
                    logger.exception("edit after accept failed")

                try:
                    await context.bot.send_message(
                        chat_id=t["submitter_chat_id"],
                        text=(f"Заявка #{t_id} принята в работу.\nИсполнитель: {user_link_html(executor_id, executor_name)}"),
                        parse_mode="HTML",
                    )
                except Exception:
                    logger.exception("notify submitter accept failed")

                save_ticket_event({"event": "accepted", "ticket_id": t_id, "executor_id": executor_id, "executor_name": executor_name})
                await audit_log(context.bot, f"✅ <b>Принята в работу</b> #{t_id} исполнителем {user_link_html(executor_id, executor_name)}")
                await query.answer("Взято в работу.")

            elif action == "reject":
                if t["status"] == "closed":
                    await query.answer("Заявка уже закрыта.")
                    return
                if t.get("executor_id") and t["status"] == "accepted" and t["executor_id"] != executor_id:
                    await query.answer("Отклонить может только принявший исполнитель.")
                    return

                PENDING_REJECT_COMMENT_BY_USER[executor_id] = t_id
                await query.answer("Укажите причину отказа ответным сообщением (в группе или в личке с ботом).")
                try:
                    await query.message.reply_text(
                        f"{user_link_html(executor_id, executor_name)}, пожалуйста, укажите причину отказа одним сообщением.",
                        parse_mode="HTML",
                    )
                except Exception:
                    pass

            elif action == "complete":
                if t["status"] != "accepted":
                    await query.answer("Сначала возьмите заявку в работу.")
                    return
                if t.get("executor_id") != executor_id:
                    await query.answer("Завершить может только исполнитель, который принял заявку.")
                    return

                t["status"] = "closed"
                t["closed"] = True
                t["completed_by"] = executor_id
                TICKETS[t_id] = t
                save_ticket_event({"event": "closed_by_executor", "ticket_id": t_id, "executor_id": executor_id})

                try:
                    await query.edit_message_text(
                        text=ticket_group_text(t),
                        reply_markup=None,
                        parse_mode="HTML",
                    )
                except Exception:
                    logger.exception("edit after close failed")

                try:
                    await context.bot.send_message(
                        chat_id=t["submitter_chat_id"],
                        text=(f"Исполнитель {user_link_html(executor_id, executor_name)} закрыл заявку #{t_id}. ✅"),
                        parse_mode="HTML",
                    )
                except Exception:
                    logger.exception("notify submitter closed failed")

                await audit_log(context.bot, f"🧾 <b>Закрыта исполнителем</b> #{t_id} ({user_link_html(executor_id, executor_name)})")
                await query.answer("Заявка закрыта.")

            else:
                await query.answer("Неизвестное действие.")
        else:
            await query.answer("Неизвестная команда.")
    except Exception:
        logger.exception("on_callback failed")
        try:
            await query.answer("Ошибка обработки нажатия.")
        except Exception:
            if query.message:
                await query.message.reply_text("Ошибка обработки нажатия.")

# ============================================
# ПРИЁМ КОММЕНТАРИЯ ОТКЛОНЕНИЯ (из ЛЮБОГО чата)
# ============================================

async def handle_text_reject_comment(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user:
        return

    u = update.effective_user
    pending_tid = PENDING_REJECT_COMMENT_BY_USER.get(u.id)
    if not pending_tid:
        return

    PENDING_REJECT_COMMENT_BY_USER.pop(u.id, None)

    t = TICKETS.get(pending_tid)
    if not t:
        return

    if t["status"] == "accepted" and t.get("executor_id") not in (None, u.id):
        await update.message.reply_text("Отклонить может только принявший исполнитель.")
        return

    comment = (update.message.text or "").strip()
    if not comment:
        await update.message.reply_text("Комментарий пуст. Пожалуйста, укажите причину отказа текстом.")
        return

    t["status"] = "rejected"
    t["executor_id"] = u.id
    t["executor_name"] = u.full_name
    t["reject_comment"] = comment
    TICKETS[pending_tid] = t
    save_ticket_event({"event": "rejected", "ticket_id": pending_tid, "executor_id": u.id, "comment": comment})

    try:
        await context.bot.edit_message_text(
            chat_id=t["group_chat_id"],
            message_id=t["group_message_id"],
            text=ticket_group_text(t),
            parse_mode="HTML",
            reply_markup=kb_initial(pending_tid),
        )
    except Exception:
        logger.exception("edit group after reject failed")

    try:
        await context.bot.send_message(
            chat_id=t["submitter_chat_id"],
            text=(
                f"Заявка #{pending_tid} <b>отклонена</b> исполнителем {user_link_html(u.id, u.full_name)}.\n"
                f"Причина: {html_escape(comment, quote=False)}"
            ),
            parse_mode="HTML",
        )
    except Exception:
        logger.exception("notify submitter rejected failed")

    await audit_log(context.bot, f"❌ <b>Отклонена</b> #{pending_tid} исполнителем {user_link_html(u.id, u.full_name)}\nПричина: {html_escape(comment, quote=False)}")

# ============================================
# ГЛОБАЛЬНЫЙ ОБРАБОТЧИК ОШИБОК
# ============================================

async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Unhandled error")
    try:
        if update and hasattr(update, "effective_chat") and update.effective_chat:
            await context.bot.send_message(update.effective_chat.id, "Упс. Что-то пошло не так. Попробуйте ещё раз.")
    except TelegramError:
        logger.exception("Failed to notify user about error")

# ============================================
# MAIN
# ============================================

def main() -> None:
    setup_logging(LOGS_DIR)
    load_env(PROJECT_ROOT)

    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("Не найден BOT_TOKEN в .env")

    logger.info(
        f"ENV chat ids: SVS={get_group_chat_id('СВС')} "
        f"SGE={get_group_chat_id('СГЭ')} "
        f"SST={get_group_chat_id('ССТ')} "
        f"AUDIT={get_audit_chat_id()}"
    )

    app = ApplicationBuilder().token(token).build()

    # Команды
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("whoami", whoami))
    app.add_handler(CommandHandler("echo_chat_id_any", echo_chat_id_any))
    app.add_handler(CommandHandler("echo_chat_id", echo_chat_id))  # admin-only
    app.add_handler(CommandHandler("debug_env", debug_env))        # admin-only
    app.add_handler(CommandHandler("export_excel", export_excel))
    app.add_handler(CommandHandler("export_csv", export_csv))

    # 1) Текст от автора (в личке) — создаём заявки
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND, handle_text))

    # 2) Комментарий к отказу — из любого чата (ставим после приёма заявок)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_reject_comment))

    # Кнопки
    app.add_handler(CallbackQueryHandler(on_callback))

    app.add_error_handler(on_error)

    logger.info("Bot is starting via long polling...")
    app.run_polling(close_loop=False)

if __name__ == "__main__":
    main()











