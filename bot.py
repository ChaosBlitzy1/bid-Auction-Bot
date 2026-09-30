import asyncio
import io
import os
import re
import secrets
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import discord
from discord import app_commands
from discord.ext import commands, tasks


def _int_env(name: str) -> int | None:
    """Read a Discord snowflake from the environment, ignoring bad values."""
    raw = os.getenv(name)
    if raw is None or not raw.strip().isdigit():
        return None
    return int(raw.strip())


def _int_env_list(name: str) -> set[int]:
    """Read a comma-separated list of Discord snowflakes from the environment."""
    return {
        int(part.strip())
        for part in (os.getenv(name) or "").split(",")
        if part.strip().isdigit()
    }


DATABASE_FILE = Path(__file__).with_name("auctions.sqlite3")
MAX_BID = 2_147_483_647
BID_COOLDOWN_SECONDS = 2.0
ANTI_SNIPE_SECONDS = 15
ANTI_SNIPE_WINDOW_SECONDS = 60
AUCTION_EXPIRY_CHECK_SECONDS = 10
TIME_PATTERN = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d$")
QUEUE_ROLE_IDS = _int_env_list("QUEUE_ROLE_IDS") or {
    1484957759349854260,
    1459747965509046386,
    1484615687048659044,
}
AUCTION_ALERT_ROLE_ID = 1485265698556084225
# The 30-second warning is a separate alert. It previously shared the auction
# alert role ID, so both pings hit the same role. Set THIRTY_SECOND_ALERT_ROLE_ID
# in the environment to route it somewhere else; it defaults to no separate ping
# rather than duplicating the auction alert.
THIRTY_SECOND_ALERT_ROLE_ID = _int_env("THIRTY_SECOND_ALERT_ROLE_ID")
# Winners must not ping the server's Moderators role inside their private
# auction-win ticket. This is enforced even if that role is made mentionable.
BLOCKED_WINNER_MENTION_ROLE_ID = 1486144171839459649
PAYMENT_METHODS = (
    "PayPal",
    "Cash App",
    "Venmo",
    "Apple Pay",
    "Revolut",
    "Crypto",
)
DEFAULT_PAYMENT_METHOD_AVAILABILITY = {
    method: method != "Cash App" for method in PAYMENT_METHODS
}
# Each winner ticket pays ONE flat fee, chosen from this table using the
# ticket's combined winning total (the auction amounts added together):
#   $150 or more  -> $5
#   $100 to $149  -> $3
#   $50 to $99    -> $2
#   under $50     -> $1
# Fees are never summed per auction. Two $64 wins are $128 together, so the
# ticket owes $3 once, not $1 + $1.
WINNER_FEE_TIERS = (
    (150, 5),
    (100, 3),
    (50, 2),
    (0, 1),
)
QUEUE_CATEGORY_NAME = "📋・auction-queue"
QUEUE_CHANNEL_NAME = "⏳・waiting-queue"
WAITING_TO_PAY_CATEGORY = "⏳・waiting-to-pay"
PAID_NOT_CLAIMED_CATEGORY = "💵・paid-not-claimed"
PAID_AND_CLAIMED_CATEGORY = "✅・paid-and-claimed"
SELLER_TICKET_CATEGORY = "📨・auction-requests"
TICKET_PANEL_CHANNEL_ID = _int_env("TICKET_PANEL_CHANNEL_ID")
# The seller-ticket panel lives in this channel. It is a built-in default rather
# than an environment variable, because the panel is the only entry point to the
# seller-ticket flow: when TICKET_PANEL_CHANNEL_ID was unset the whole feature
# silently disappeared. TICKET_PANEL_CHANNEL_ID still overrides this.
DEFAULT_TICKET_PANEL_CHANNEL_ID = 1486110550915158026
DEFAULT_TICKET_PANEL_CHANNEL_NAME = "📨・submit-for-auction"
TRANSCRIPT_CHANNEL_ID = _int_env("TRANSCRIPT_CHANNEL_ID") or 1486111228811280618
# The auction manager role is NOT built in. It used to default to
# AUCTION_ALERT_ROLE_ID, but that is the "Bidders (ping)" role, so a server with
# no configured manager role silently pinged every bidder whenever a private
# ticket was opened. A manager role must be chosen explicitly with
# /auction_setup, or set AUCTION_MANAGER_ROLE_ID in the environment.
DEFAULT_MANAGER_ROLE_ID = _int_env("AUCTION_MANAGER_ROLE_ID")
# Never treat a role that is used for mass alerts as the manager role: pinging it
# inside a private ticket exposes the ticket to everyone in that role.
FORBIDDEN_MANAGER_ROLE_IDS = {AUCTION_ALERT_ROLE_ID}

intents = discord.Intents.default()
# Ticket transcripts include members' message bodies only when this privileged
# intent is enabled both here and in the Discord Developer Portal.
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

last_bid_times: dict[tuple[int, int], float] = {}
bid_lock = asyncio.Lock()
queue_start_lock = asyncio.Lock()
sync_done = False
BOT_STARTED_AT = datetime.now(timezone.utc)


SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS guild_config (
    guild_id INTEGER PRIMARY KEY,
    manager_role_id INTEGER,
    log_channel_id INTEGER,
    auction_channel_id INTEGER,
    ticket_panel_message_id INTEGER,
    queue_message_id INTEGER,
    seller_tickets_enabled INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS auctions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    public_id TEXT,
    guild_id INTEGER NOT NULL,
    channel_id INTEGER NOT NULL,
    message_id INTEGER,
    host_id INTEGER NOT NULL,
    item TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    photo_url TEXT,
    starting_bid INTEGER NOT NULL,
    reserve_price INTEGER NOT NULL DEFAULT 0,
    current_bid INTEGER NOT NULL,
    highest_bidder_id INTEGER,
    status TEXT NOT NULL CHECK(status IN ('active', 'paused', 'ended', 'cancelled')),
    starts_at REAL NOT NULL,
    ends_at REAL NOT NULL,
    paused_remaining REAL,
    ending_announced INTEGER NOT NULL DEFAULT 0,
    thirty_second_announced INTEGER NOT NULL DEFAULT 0,
    created_at REAL NOT NULL,
    ended_at REAL,
    winner_id INTEGER,
    final_bid INTEGER,
    closed_by INTEGER,
    winner_channel_id INTEGER,
    winner_message_id INTEGER,
    payment_method TEXT,
    transaction_status TEXT NOT NULL DEFAULT 'waiting_to_pay',
    is_repeat_win INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS auction_winner_archive (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    guild_id INTEGER NOT NULL,
    original_auction_id INTEGER NOT NULL,
    item TEXT NOT NULL,
    winner_id INTEGER,
    final_bid INTEGER,
    status TEXT NOT NULL,
    ended_at REAL,
    archived_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS bids (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    auction_id INTEGER NOT NULL REFERENCES auctions(id),
    bidder_id INTEGER NOT NULL,
    amount INTEGER NOT NULL,
    created_at REAL NOT NULL,
    valid INTEGER NOT NULL DEFAULT 1,
    removed_by INTEGER,
    removed_reason TEXT
);

CREATE TABLE IF NOT EXISTS payment_method_settings (
    guild_id INTEGER NOT NULL,
    method TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (guild_id, method)
);

CREATE TABLE IF NOT EXISTS schedules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    guild_id INTEGER NOT NULL,
    channel_id INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL CHECK(day_of_week BETWEEN 0 AND 6),
    time_utc TEXT NOT NULL,
    item TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    starting_bid INTEGER NOT NULL,
    duration_minutes INTEGER NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    last_run_date TEXT,
    last_announcement_date TEXT,
    created_by INTEGER NOT NULL DEFAULT 0,
    photo_url TEXT
);

CREATE TABLE IF NOT EXISTS scheduled_announcements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    guild_id INTEGER NOT NULL,
    channel_id INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL CHECK(day_of_week BETWEEN 0 AND 6),
    time_utc TEXT NOT NULL,
    announcement TEXT NOT NULL,
    role_id INTEGER,
    enabled INTEGER NOT NULL DEFAULT 1,
    last_sent_date TEXT,
    created_by INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    guild_id INTEGER NOT NULL,
    auction_id INTEGER,
    actor_id INTEGER NOT NULL,
    action TEXT NOT NULL,
    details TEXT NOT NULL DEFAULT '',
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS auction_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    auction_id INTEGER NOT NULL REFERENCES auctions(id),
    channel_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    kind TEXT NOT NULL,
    deleted INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS queue_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    guild_id INTEGER NOT NULL,
    channel_id INTEGER,
    item TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    photo_url TEXT NOT NULL,
    starting_bid INTEGER NOT NULL,
    reserve_price INTEGER NOT NULL DEFAULT 0,
    duration_minutes INTEGER NOT NULL,
    position INTEGER NOT NULL,
    created_by INTEGER NOT NULL,
    created_at REAL NOT NULL,
    scheduled_time TEXT,
    scheduled_date TEXT,
    scheduled_at REAL
);

CREATE TABLE IF NOT EXISTS seller_tickets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    guild_id INTEGER NOT NULL,
    channel_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    fortnite_username TEXT NOT NULL,
    bid_details TEXT NOT NULL,
    game_type TEXT NOT NULL,
    payment_method TEXT NOT NULL,
    item_details TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'open',
    created_at REAL NOT NULL,
    closed_at REAL
);

CREATE INDEX IF NOT EXISTS idx_auctions_guild_status ON auctions(guild_id, status);
CREATE INDEX IF NOT EXISTS idx_bids_auction ON bids(auction_id, created_at);
CREATE INDEX IF NOT EXISTS idx_schedules_due ON schedules(enabled, day_of_week, time_utc);
"""


def connect() -> sqlite3.Connection:
    connection = sqlite3.connect(DATABASE_FILE, timeout=10)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def initialize_database():
    with connect() as connection:
        connection.executescript(SCHEMA)
        columns = {
            row["name"] for row in connection.execute("PRAGMA table_info(schedules)")
        }
        if "last_announcement_date" not in columns:
            connection.execute("ALTER TABLE schedules ADD COLUMN last_announcement_date TEXT")
        if "created_by" not in columns:
            connection.execute("ALTER TABLE schedules ADD COLUMN created_by INTEGER NOT NULL DEFAULT 0")
        if "photo_url" not in columns:
            connection.execute("ALTER TABLE schedules ADD COLUMN photo_url TEXT")
        config_columns = {
            row["name"] for row in connection.execute("PRAGMA table_info(guild_config)")
        }
        if "ticket_panel_message_id" not in config_columns:
            connection.execute("ALTER TABLE guild_config ADD COLUMN ticket_panel_message_id INTEGER")
        if "queue_message_id" not in config_columns:
            connection.execute("ALTER TABLE guild_config ADD COLUMN queue_message_id INTEGER")
        if "seller_tickets_enabled" not in config_columns:
            connection.execute("ALTER TABLE guild_config ADD COLUMN seller_tickets_enabled INTEGER NOT NULL DEFAULT 1")
        auction_columns = {
            row["name"] for row in connection.execute("PRAGMA table_info(auctions)")
        }
        migrations = {
            "public_id": "ALTER TABLE auctions ADD COLUMN public_id TEXT",
            "photo_url": "ALTER TABLE auctions ADD COLUMN photo_url TEXT",
            "reserve_price": "ALTER TABLE auctions ADD COLUMN reserve_price INTEGER NOT NULL DEFAULT 0",
            "winner_channel_id": "ALTER TABLE auctions ADD COLUMN winner_channel_id INTEGER",
            "winner_message_id": "ALTER TABLE auctions ADD COLUMN winner_message_id INTEGER",
            "payment_method": "ALTER TABLE auctions ADD COLUMN payment_method TEXT",
            "transaction_status": "ALTER TABLE auctions ADD COLUMN transaction_status TEXT NOT NULL DEFAULT 'waiting_to_pay'",
            "thirty_second_announced": "ALTER TABLE auctions ADD COLUMN thirty_second_announced INTEGER NOT NULL DEFAULT 0",
            "is_repeat_win": "ALTER TABLE auctions ADD COLUMN is_repeat_win INTEGER NOT NULL DEFAULT 0",
        }
        for column, statement in migrations.items():
            if column not in auction_columns:
                connection.execute(statement)
        connection.execute(
            "UPDATE auctions SET public_id = printf('9%017d', id) "
            "WHERE public_id IS NULL OR public_id = ''"
        )
        connection.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_auctions_public_id ON auctions(public_id)"
        )
        queue_columns = {
            row["name"] for row in connection.execute("PRAGMA table_info(queue_items)")
        }
        if "scheduled_time" not in queue_columns:
            connection.execute("ALTER TABLE queue_items ADD COLUMN scheduled_time TEXT")
        if "channel_id" not in queue_columns:
            connection.execute("ALTER TABLE queue_items ADD COLUMN channel_id INTEGER")
        if "scheduled_date" not in queue_columns:
            connection.execute("ALTER TABLE queue_items ADD COLUMN scheduled_date TEXT")
        if "scheduled_at" not in queue_columns:
            connection.execute("ALTER TABLE queue_items ADD COLUMN scheduled_at REAL")


def now() -> float:
    return time.time()


def utc_date() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def format_amount(amount: int) -> str:
    return f"{amount:,}"


def bid_confirmation_message(current_bid: int, amount_placed: int, new_bid_total: int) -> str:
    return (
        f"The auction is at **${format_amount(current_bid)}**. "
        f"You are placing **${format_amount(amount_placed)}**. "
        f"So the new bid will be at **${format_amount(new_bid_total)}**. "
        "Would you like to confirm?"
    )


def parse_day(day: str) -> int | None:
    names = {
        "monday": 0,
        "tuesday": 1,
        "wednesday": 2,
        "thursday": 3,
        "friday": 4,
        "saturday": 5,
        "sunday": 6,
    }
    return names.get(day.lower())


def parse_queue_time(value: str | None) -> str | None:
    if not value:
        return None
    normalized = value.strip().lower().replace(" ", "")
    for pattern in ("%I%p", "%I:%M%p", "%H:%M"):
        try:
            return datetime.strptime(normalized, pattern).strftime("%H:%M")
        except ValueError:
            continue
    return None


def parse_queue_datetime(value: str | None) -> float | None:
    if not value:
        return None
    normalized = re.sub(r"\s+", " ", value.strip().lower().replace(" at ", " "))
    local_timezone = datetime.now().astimezone().tzinfo
    current = datetime.now(local_timezone)
    time_match = re.search(r"(\d{1,2}(?::\d{2})?\s*(?:am|pm)|\d{1,2}:\d{2})$", normalized)
    if not time_match:
        return None
    time_text = time_match.group(1).replace(" ", "")
    date_text = normalized[:time_match.start()].strip()
    parsed_time = None
    for pattern in ("%I:%M%p", "%I%p", "%H:%M"):
        try:
            parsed_time = datetime.strptime(time_text, pattern).time()
            break
        except ValueError:
            continue
    if parsed_time is None:
        return None

    if not date_text or date_text == "today":
        target_date = current.date()
        # "today 3pm" typed at 6pm resolved to 3pm today, which is already in
        # the past, so the schedule worker fired it on the next tick. A time
        # that has already passed today rolls forward to tomorrow instead.
        if datetime.combine(target_date, parsed_time, local_timezone) <= current:
            target_date = (current + timedelta(days=1)).date()
    elif date_text == "tomorrow":
        target_date = (current + timedelta(days=1)).date()
    elif date_text in {"monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"}:
        target_weekday = parse_day(date_text)
        days_ahead = (target_weekday - current.weekday()) % 7
        if days_ahead == 0 and datetime.combine(current.date(), parsed_time, local_timezone) <= current:
            days_ahead = 7
        target_date = (current + timedelta(days=days_ahead)).date()
    else:
        parsed_date = None
        for pattern in ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%m/%d", "%m-%d"):
            try:
                parsed_date = datetime.strptime(date_text, pattern)
                break
            except ValueError:
                continue
        if parsed_date is None:
            return None
        year = parsed_date.year if parsed_date.year != 1900 else current.year
        target_date = parsed_date.replace(year=year).date()
        if target_date < current.date() and parsed_date.year == 1900:
            target_date = target_date.replace(year=year + 1)

    return datetime.combine(target_date, parsed_time, local_timezone).timestamp()


def format_queue_datetime(timestamp: float | None) -> str:
    if not timestamp:
        return "Time not set"
    return f"<t:{int(timestamp)}:F>\n<t:{int(timestamp)}:R>"


def get_config(guild_id: int) -> sqlite3.Row | None:
    with connect() as connection:
        return connection.execute(
            "SELECT * FROM guild_config WHERE guild_id = ?",
            (guild_id,),
        ).fetchone()


def manager_role_ids(guild_id: int) -> list[int]:
    """The auction manager role IDs, or [] when none are configured.

    /auction_setup stores the manager roles in guild_config, which always wins.
    When it is empty, the queue staff roles are used, because those are the
    staff who already moderate the queue and open seller tickets. There is no
    built-in fallback to a mass-alert role: guessing one used to ping every
    bidder and leak private tickets.
    """
    config = get_config(guild_id)
    role_ids: list[int] = []
    if config and config["manager_role_id"]:
        role_ids = [config["manager_role_id"]]
    elif DEFAULT_MANAGER_ROLE_ID:
        role_ids = [DEFAULT_MANAGER_ROLE_ID]
    if not role_ids:
        role_ids = sorted(QUEUE_ROLE_IDS)
    # A role used for server-wide alerts is never a manager role: pinging it
    # inside a private ticket would expose the ticket to everyone in that role.
    return [role_id for role_id in role_ids if role_id not in FORBIDDEN_MANAGER_ROLE_IDS]


def get_manager_roles(guild: discord.Guild) -> list[discord.Role]:
    """Return the manager roles that still exist in this server."""
    return [role for role in (guild.get_role(rid) for rid in manager_role_ids(guild.id)) if role]


def manager_ping_text(guild: discord.Guild) -> str:
    """Mentions for every manager role, safe to paste into a message."""
    return " ".join(role.mention for role in get_manager_roles(guild))


def log_channel_id(guild_id: int) -> int | None:
    """The auction log channel: saved setup first, then the built-in one."""
    config = get_config(guild_id)
    if config and config["log_channel_id"]:
        return config["log_channel_id"]
    return _int_env("AUCTION_LOG_CHANNEL_ID")


def seller_tickets_enabled(guild_id: int) -> bool:
    config = get_config(guild_id)
    return config is None or config["seller_tickets_enabled"] != 0


def set_seller_tickets_enabled(guild_id: int, enabled: bool):
    with connect() as connection:
        connection.execute(
            """
            INSERT INTO guild_config(guild_id, seller_tickets_enabled)
            VALUES (?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET seller_tickets_enabled = excluded.seller_tickets_enabled
            """,
            (guild_id, int(enabled)),
        )


def update_config(
    guild_id: int,
    new_manager_role_id=None,
    new_log_channel_id=None,
    new_auction_channel_id=None,
):
    # The parameter names are prefixed to avoid shadowing the manager_role_id()
    # and log_channel_id() resolvers used everywhere else.
    with connect() as connection:
        connection.execute(
            """
            INSERT INTO guild_config(guild_id, manager_role_id, log_channel_id, auction_channel_id)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET
                manager_role_id = COALESCE(excluded.manager_role_id, manager_role_id),
                log_channel_id = COALESCE(excluded.log_channel_id, log_channel_id),
                auction_channel_id = COALESCE(excluded.auction_channel_id, auction_channel_id)
            """,
            (guild_id, new_manager_role_id, new_log_channel_id, new_auction_channel_id),
        )


def is_staff(member: discord.Member) -> bool:
    if member.guild_permissions.administrator or member.guild_permissions.manage_guild:
        return True
    manager_ids = manager_role_ids(member.guild.id)
    if any(role.id in manager_ids for role in member.roles):
        return True
    return any(role.id in QUEUE_ROLE_IDS for role in member.roles)


def is_moderator(member: discord.Member) -> bool:
    return is_staff(member) or member.guild_permissions.manage_messages


def log_action(guild_id: int, actor_id: int, action: str, auction_id: int | None = None, details: str = ""):
    with connect() as connection:
        connection.execute(
            """
            INSERT INTO audit_log(guild_id, auction_id, actor_id, action, details, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (guild_id, auction_id, actor_id, action, details, now()),
        )


def fetch_auction(auction_id: int) -> sqlite3.Row | None:
    with connect() as connection:
        return connection.execute(
            "SELECT * FROM auctions WHERE id = ?",
            (auction_id,),
        ).fetchone()


def auction_reference(auction: sqlite3.Row) -> str:
    """Return the long numeric ID displayed for an auction."""
    return str(auction["public_id"] or f"9{auction['id']:017d}")


def fetch_auction_by_reference(reference: str) -> sqlite3.Row | None:
    """Find an auction by its copyable ID; retain short-ID compatibility."""
    reference = reference.strip()
    if not reference.isdecimal():
        return None
    with connect() as connection:
        auction = connection.execute(
            "SELECT * FROM auctions WHERE public_id = ?", (reference,)
        ).fetchone()
        if auction is not None:
            return auction
        if len(reference) <= 9:
            return connection.execute(
                "SELECT * FROM auctions WHERE id = ?", (int(reference),)
            ).fetchone()
    return None


def reset_auction_numbering(guild_id: int, actor_id: int) -> tuple[bool, str]:
    with connect() as connection:
        active = connection.execute(
            "SELECT COUNT(*) FROM auctions WHERE guild_id = ? AND status IN ('active', 'paused')",
            (guild_id,),
        ).fetchone()[0]
        if active:
            return False, "End or cancel all active auctions before resetting numbering."

        auctions = connection.execute(
            "SELECT id, item, winner_id, final_bid, status, ended_at FROM auctions WHERE guild_id = ?",
            (guild_id,),
        ).fetchall()
        for auction in auctions:
            connection.execute(
                """
                INSERT INTO auction_winner_archive(
                    guild_id, original_auction_id, item, winner_id, final_bid,
                    status, ended_at, archived_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    guild_id,
                    auction["id"],
                    auction["item"],
                    auction["winner_id"],
                    auction["final_bid"],
                    auction["status"],
                    auction["ended_at"],
                    now(),
                ),
            )
            connection.execute("DELETE FROM auction_messages WHERE auction_id = ?", (auction["id"],))
            connection.execute("DELETE FROM bids WHERE auction_id = ?", (auction["id"],))
            connection.execute("DELETE FROM audit_log WHERE auction_id = ?", (auction["id"],))
        connection.execute("DELETE FROM auctions WHERE guild_id = ?", (guild_id,))
        remaining = connection.execute("SELECT COUNT(*) FROM auctions").fetchone()[0]
        if not remaining:
            connection.execute("DELETE FROM sqlite_sequence WHERE name = 'auctions'")
    log_action(guild_id, actor_id, "reset_numbering", details=f"Archived {len(auctions)} auctions")
    if remaining:
        return True, f"Archived {len(auctions)} auctions. New IDs continue after other guild auctions."
    return True, f"Archived {len(auctions)} auctions. The next auction will be **#1**."


def fetch_second_place(auction_id: int, highest_bidder_id: int | None):
    with connect() as connection:
        if highest_bidder_id:
            return connection.execute(
                """
                SELECT bidder_id, amount FROM bids
                WHERE auction_id = ? AND valid = 1 AND bidder_id != ?
                ORDER BY amount DESC, created_at ASC, id ASC LIMIT 1
                """,
                (auction_id, highest_bidder_id),
            ).fetchone()
        return connection.execute(
            """
            SELECT bidder_id, amount FROM bids
            WHERE auction_id = ? AND valid = 1
            ORDER BY amount DESC, created_at ASC, id ASC LIMIT 1
            """,
            (auction_id,),
        ).fetchone()


def record_temporary_message(auction_id: int, channel_id: int, message_id: int, kind: str):
    with connect() as connection:
        connection.execute(
            "INSERT INTO auction_messages(auction_id, channel_id, message_id, kind) VALUES (?, ?, ?, ?)",
            (auction_id, channel_id, message_id, kind),
        )


async def send_temporary_message(auction_id: int, channel: discord.abc.Messageable, content: str, kind: str):
    try:
        message = await channel.send(content)
        channel_id = getattr(channel, "id", None)
        if channel_id:
            record_temporary_message(auction_id, channel_id, message.id, kind)
        return message
    except discord.HTTPException:
        return None


async def delete_temporary_messages(auction_id: int):
    with connect() as connection:
        messages = connection.execute(
            "SELECT channel_id, message_id FROM auction_messages WHERE auction_id = ? AND deleted = 0",
            (auction_id,),
        ).fetchall()
    for row in messages:
        channel = bot.get_channel(row["channel_id"])
        if channel is None:
            continue
        try:
            message = await channel.fetch_message(row["message_id"])
            await message.delete()
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            pass
    with connect() as connection:
        connection.execute("UPDATE auction_messages SET deleted = 1 WHERE auction_id = ?", (auction_id,))


async def cleanup_bid_messages(auction_id: int):
    with connect() as connection:
        bid_count = connection.execute(
            "SELECT COUNT(*) FROM bids WHERE auction_id = ? AND valid = 1",
            (auction_id,),
        ).fetchone()[0]
        if bid_count < 5:
            return
        messages = connection.execute(
            "SELECT channel_id, message_id FROM auction_messages "
            "WHERE auction_id = ? AND kind = 'outbid' AND deleted = 0",
            (auction_id,),
        ).fetchall()
    for row in messages:
        channel = bot.get_channel(row["channel_id"])
        if channel is None:
            continue
        try:
            message = await channel.fetch_message(row["message_id"])
            await message.delete()
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            pass
    with connect() as connection:
        connection.execute(
            "UPDATE auction_messages SET deleted = 1 "
            "WHERE auction_id = ? AND kind = 'outbid'",
            (auction_id,),
        )


async def send_outbid_notification(
    auction_id: int,
    previous_bidder: int,
    previous_amount: int,
    new_bidder: int,
):
    auction = fetch_auction(auction_id)
    channel = bot.get_channel(auction["channel_id"]) if auction else None
    if channel is None or auction is None:
        return
    await cleanup_bid_messages(auction_id)
    await send_temporary_message(
        auction_id,
        channel,
        f"📣 <@{previous_bidder}> was outbid at **${format_amount(previous_amount)}** "
        f"by <@{new_bidder}> on Auction **{auction_reference(auction)}** — **{auction['item']}**!",
        "outbid",
    )


def create_auction_record(guild_id: int, channel_id: int, host_id: int, item: str, description: str, starting_bid: int, duration_minutes: int, reserve_price: int = 0, photo_url: str | None = None) -> int:
    timestamp = now()
    with connect() as connection:
        while True:
            # 18 digits are easy to copy from Discord. Legacy IDs use a 9 prefix,
            # so this range cannot overlap with values assigned during migration.
            public_id = str(100_000_000_000_000_000 + secrets.randbelow(800_000_000_000_000_000))
            try:
                cursor = connection.execute(
                    """
                    INSERT INTO auctions(
                        public_id, guild_id, channel_id, host_id, item, description, photo_url,
                        starting_bid, reserve_price, current_bid, status, starts_at,
                        ends_at, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?)
                    """,
                    (
                        public_id,
                        guild_id,
                        channel_id,
                        host_id,
                        item[:256],
                        description[:1000],
                        photo_url,
                        starting_bid,
                        reserve_price,
                        starting_bid,
                        timestamp,
                        timestamp + duration_minutes * 60,
                        timestamp,
                    ),
                )
                break
            except sqlite3.IntegrityError:
                continue
        auction_id = cursor.lastrowid
    log_action(guild_id, host_id, "created", auction_id, item[:256])
    return auction_id


def set_message_id(auction_id: int, message_id: int):
    with connect() as connection:
        connection.execute(
            "UPDATE auctions SET message_id = ? WHERE id = ?",
            (message_id, auction_id),
        )


def update_auction_status(auction_id: int, status: str, actor_id: int, paused_remaining=None):
    auction = fetch_auction(auction_id)
    if auction is None:
        return
    timestamp = now()
    with connect() as connection:
        if status in ("ended", "cancelled"):
            connection.execute(
                """
                UPDATE auctions
                SET status = ?, ended_at = ?, closed_by = ?,
                    winner_id = CASE WHEN ? = 'ended' AND highest_bidder_id IS NOT NULL AND (reserve_price = 0 OR current_bid >= reserve_price) THEN highest_bidder_id ELSE NULL END,
                    final_bid = CASE WHEN ? = 'ended' AND highest_bidder_id IS NOT NULL AND (reserve_price = 0 OR current_bid >= reserve_price) THEN current_bid ELSE NULL END
                WHERE id = ?
                """,
                (status, timestamp, actor_id, status, status, auction_id),
            )
        else:
            connection.execute(
                "UPDATE auctions SET status = ?, paused_remaining = ? WHERE id = ?",
                (status, paused_remaining, auction_id),
            )
    log_action(auction["guild_id"], actor_id, status, auction_id)


def extend_auction_record(auction_id: int, seconds: int, actor_id: int):
    with connect() as connection:
        connection.execute(
            # The countdown warnings are cleared as well: the new end time moves
            # back outside the 30s/1m windows, so leaving the flags set would
            # permanently silence the alerts for the extended auction.
            "UPDATE auctions SET ends_at = ends_at + ?, "
            "ending_announced = 0, thirty_second_announced = 0 "
            "WHERE id = ? AND status = 'active'",
            (seconds, auction_id),
        )
    auction = fetch_auction(auction_id)
    if auction:
        log_action(auction["guild_id"], actor_id, "extended", auction_id, f"{seconds} seconds")


def remove_bid_record(auction_id: int, bid_id: int, actor_id: int, reason: str):
    # Bound before the block so the audit log below is safe even when the
    # auction row has already been deleted.
    auction = None
    with connect() as connection:
        connection.execute(
            """
            UPDATE bids
            SET valid = 0, removed_by = ?, removed_reason = ?
            WHERE id = ? AND auction_id = ? AND valid = 1
            """,
            (actor_id, reason[:300], bid_id, auction_id),
        )
        highest = connection.execute(
            """
            SELECT bidder_id, amount FROM bids
            WHERE auction_id = ? AND valid = 1
            ORDER BY amount DESC, created_at ASC, id ASC LIMIT 1
            """,
            (auction_id,),
        ).fetchone()
        auction = connection.execute(
            "SELECT guild_id, starting_bid, status, winner_id, reserve_price FROM auctions WHERE id = ?",
            (auction_id,),
        ).fetchone()
        if auction:
            new_bid = highest["amount"] if highest else auction["starting_bid"]
            new_bidder = highest["bidder_id"] if highest else None
            # An ended auction keeps its winner in winner_id/final_bid, so merely
            # recalculating current_bid left the auction pointing at a bidder
            # whose bid no longer exists. The winner has to be recomputed too.
            if auction["status"] == "ended":
                keeps_winner = (
                    auction["winner_id"] == new_bidder
                    and (auction["reserve_price"] == 0 or new_bid >= auction["reserve_price"])
                )
                connection.execute(
                    "UPDATE auctions SET current_bid = ?, highest_bidder_id = ?, "
                    "winner_id = ?, final_bid = ? WHERE id = ?",
                    (
                        new_bid,
                        new_bidder,
                        auction["winner_id"] if keeps_winner else new_bidder,
                        new_bid if keeps_winner else (new_bid if new_bidder else None),
                        auction_id,
                    ),
                )
            else:
                connection.execute(
                    "UPDATE auctions SET current_bid = ?, highest_bidder_id = ? WHERE id = ?",
                    (new_bid, new_bidder, auction_id),
                )
    if auction:
        log_action(auction["guild_id"], actor_id, "bid_removed", auction_id, reason)


async def send_log(guild_id: int, content: str, title: str = "Auction Log", color: discord.Color = discord.Color.blurple()):
    guild = bot.get_guild(guild_id)
    channel = get_log_channel(guild)
    if channel is None and guild is not None:
        # Nothing was configured, so the log channel is created on first use and
        # saved. This is what removes the need to run /auction_setup after every
        # restart.
        channel = await ensure_log_channel(guild)
    if channel:
        try:
            embed = discord.Embed(
                title=title,
                description=content,
                color=color,
                timestamp=datetime.now(timezone.utc),
            )
            embed.set_footer(text=f"Guild ID: {guild_id}")
            await channel.send(embed=embed)
        except discord.HTTPException:
            pass


async def close_ticket_channel(
    channel: discord.TextChannel,
    guild_id: int,
    closer: discord.Member,
    ticket_label: str,
    delete_channel: bool = True,
):
    transcript_channel = None
    if TRANSCRIPT_CHANNEL_ID is not None:
        transcript_channel = bot.get_channel(TRANSCRIPT_CHANNEL_ID)
    if transcript_channel is None and TRANSCRIPT_CHANNEL_ID is not None:
        try:
            transcript_channel = await bot.fetch_channel(TRANSCRIPT_CHANNEL_ID)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            transcript_channel = None
    if transcript_channel is None:
        print("TRANSCRIPT_CHANNEL_ID is not configured or could not be resolved.")
        return False, None
    if transcript_channel.guild.id != guild_id:
        print(f"Transcript channel {TRANSCRIPT_CHANNEL_ID} belongs to guild {transcript_channel.guild.id}, not {guild_id}.")
        return False, None
    transcript_lines = []
    try:
        async for message in channel.history(limit=None, oldest_first=True):
            timestamp = message.created_at.strftime("%Y-%m-%d %H:%M UTC")
            content = message.content or "[embed/component message]"
            transcript_lines.append(f"[{timestamp}] {message.author} ({message.author.id}): {content}")
            for attachment in message.attachments:
                transcript_lines.append(f"Attachment: {attachment.url}")
    except discord.HTTPException:
        transcript_lines.append("[Transcript could not read the complete channel history]")

    if not transcript_lines:
        transcript_lines.append("[No messages recorded]")
    transcript = "\n".join(transcript_lines)
    transcript_url = None
    first_message = None
    try:
        transcript_file = discord.File(
            io.BytesIO(transcript.encode("utf-8")),
            filename=f"{ticket_label.lower().replace(' ', '-')}-transcript.txt",
        )
        embed = discord.Embed(
            title=f"Ticket Transcript | {ticket_label}",
            description="The complete ticket history is attached as a text file.",
            color=discord.Color.dark_grey(),
            timestamp=datetime.now(timezone.utc),
        )
        embed.add_field(name="Closed by", value=f"{closer.mention} ({closer.id})", inline=True)
        embed.add_field(name="Original channel", value=f"#{channel.name}", inline=True)
        first_message = await transcript_channel.send(embed=embed, file=transcript_file)
        transcript_url = f"https://discord.com/channels/{guild_id}/{TRANSCRIPT_CHANNEL_ID}/{first_message.id}"
        link_embed = embed.copy()
        link_embed.add_field(
            name="View transcript on Discord Web",
            value=f"[Open transcript]({transcript_url})",
            inline=False,
        )
        await first_message.edit(embed=link_embed)
    except discord.Forbidden as error:
        print(f"Transcript permission error in channel {TRANSCRIPT_CHANNEL_ID}: {error}")
        return False, None
    except discord.HTTPException as error:
        print(f"Transcript upload error in channel {TRANSCRIPT_CHANNEL_ID}: {error}")
        return False, None

    if delete_channel:
        await channel.delete(reason=f"Ticket closed by {closer}")
    return True, transcript_url


async def ensure_log_channel(guild: discord.Guild) -> discord.TextChannel | None:
    """Return the auction log channel.

    No channel is ever created: the separate auction-logs channel was removed,
    so logs fall back to the seller-ticket panel channel rather than spawning a
    new one. The resolved channel is still saved, so this only has to happen once.
    """
    saved = log_channel_id(guild.id)
    if saved:
        channel = guild.get_channel(saved)
        if channel is not None:
            return channel
        if bot.get_channel(saved) is not None:
            return bot.get_channel(saved)
    # The saved channel is gone, so fall back to the panel channel and save it
    # rather than creating a dedicated log channel.
    channel = await ensure_ticket_panel_channel(guild)
    if channel is None:
        return None
    with connect() as connection:
        connection.execute(
            """
            INSERT INTO guild_config(guild_id, log_channel_id)
            VALUES (?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET log_channel_id = excluded.log_channel_id
            """,
            (guild.id, channel.id),
        )
    return channel


def get_log_channel(guild: discord.Guild | None):
    """Resolve the log channel for the guild, or None when unavailable."""
    if guild is None:
        return None
    saved = log_channel_id(guild.id)
    if not saved:
        return None
    return guild.get_channel(saved) or bot.get_channel(saved)


def has_queue_role(member: discord.Member) -> bool:
    return any(role.id in QUEUE_ROLE_IDS for role in member.roles)


def is_image_attachment(attachment: discord.Attachment) -> bool:
    return bool(attachment.content_type and attachment.content_type.startswith("image/"))


async def require_queue_staff(interaction: discord.Interaction) -> bool:
    if not await require_server(interaction):
        return False
    if not has_queue_role(interaction.user):
        await interaction.response.send_message("Only the configured auction queue staff roles can use the queue.", ephemeral=True)
        return False
    return True


async def ensure_queue_channel(guild: discord.Guild) -> discord.TextChannel:
    category = discord.utils.get(guild.categories, name=QUEUE_CATEGORY_NAME)
    allowed_roles = [guild.get_role(role_id) for role_id in QUEUE_ROLE_IDS]
    allowed_roles = [role for role in allowed_roles if role]
    overwrites = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False, read_message_history=False),
    }
    for role in allowed_roles:
        overwrites[role] = discord.PermissionOverwrite(view_channel=True, read_message_history=True, send_messages=True, attach_files=True)
    if guild.me:
        overwrites[guild.me] = discord.PermissionOverwrite(
            view_channel=True,
            read_message_history=True,
            send_messages=True,
            manage_channels=True,
            manage_messages=True,
        )
    if category is None:
        category = await guild.create_category(QUEUE_CATEGORY_NAME, overwrites=overwrites, reason="Create private auction queue")
    channel = discord.utils.get(category.text_channels, name=QUEUE_CHANNEL_NAME)
    if channel is None:
        channel = await guild.create_text_channel(
            QUEUE_CHANNEL_NAME,
            category=category,
            overwrites=overwrites,
            topic="Private staff queue for preparing upcoming auctions.",
            reason="Create private auction queue channel",
        )
    else:
        await channel.edit(overwrites=overwrites, reason="Refresh private auction queue permissions")
    return channel


async def refresh_queue_message(channel: discord.TextChannel):
    with connect() as connection:
        rows = connection.execute(
            "SELECT * FROM queue_items WHERE guild_id = ? "
            "ORDER BY scheduled_at IS NULL, scheduled_at, position, id",
            (channel.guild.id,),
        ).fetchall()
    embed = discord.Embed(
        title="📋 Auction Queue",
        description="Private staff queue. Use the queue commands to prepare and start auctions.",
        color=discord.Color.blurple(),
    )
    if rows:
        for row in rows:
            # channel_id is optional in the queue, so an item that relies on the
            # guild's default auction channel has no channel of its own.
            channel_label = (
                f"<#{row['channel_id']}>" if row["channel_id"] else "Default auction channel"
            )
            embed.add_field(
                name=f"Auction ID (queue) `{row['id']}` | Position {row['position']} | {row['item']}",
                value=(
                    f"Channel: {channel_label} | "
                    f"Time of auction: **{format_queue_datetime(row['scheduled_at'])}** | "
                    f"Starting: **{format_amount(row['starting_bid'])}** | "
                    f"Reserve: **{format_amount(row['reserve_price']) if row['reserve_price'] else 'None'}**"
                ),
                inline=False,
            )
    else:
        embed.add_field(name="Queue is empty", value="Add an item with `/queue_add`.", inline=False)
    config = get_config(channel.guild.id)
    queue_message = None
    if config and config["queue_message_id"]:
        try:
            queue_message = await channel.fetch_message(config["queue_message_id"])
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            queue_message = None
    if queue_message is None:
        queue_message = await channel.send(embed=embed)
        with connect() as connection:
            connection.execute(
                """
                INSERT INTO guild_config(guild_id, queue_message_id)
                VALUES (?, ?)
                ON CONFLICT(guild_id) DO UPDATE SET queue_message_id = excluded.queue_message_id
                """,
                (channel.guild.id, queue_message.id),
            )
    else:
        await queue_message.edit(embed=embed)


def queue_next_position(guild_id: int) -> int:
    with connect() as connection:
        row = connection.execute("SELECT COALESCE(MAX(position), 0) + 1 AS next_position FROM queue_items WHERE guild_id = ?", (guild_id,)).fetchone()
    return row["next_position"]


async def start_queued_item(queue_id: int) -> int | None:
    # Keep the item in the queue until Discord confirms that the auction was posted.
    # The lock prevents a manual start and the scheduler from posting it twice.
    async with queue_start_lock:
        with connect() as connection:
            item = connection.execute(
                "SELECT * FROM queue_items WHERE id = ?",
                (queue_id,),
            ).fetchone()
            if not item:
                return None
            config = get_config(item["guild_id"])
            destination_channel_id = item["channel_id"] or (config["auction_channel_id"] if config else None)
        channel = bot.get_channel(destination_channel_id) if destination_channel_id else None
        if channel is None:
            return None

        auction_id = create_auction_record(
            item["guild_id"], destination_channel_id, item["created_by"], item["item"],
            item["description"], item["starting_bid"], item["duration_minutes"],
            item["reserve_price"], item["photo_url"],
        )
        try:
            message = await channel.send(
                content=f"<@&{AUCTION_ALERT_ROLE_ID}>",
                embed=auction_embed(fetch_auction(auction_id)),
                view=AuctionView(auction_id),
                allowed_mentions=discord.AllowedMentions(roles=True),
            )
        except (discord.Forbidden, discord.HTTPException):
            update_auction_status(auction_id, "cancelled", item["created_by"])
            return None

        set_message_id(auction_id, message.id)
        with connect() as connection:
            deleted = connection.execute("DELETE FROM queue_items WHERE id = ?", (queue_id,))
            if deleted.rowcount == 1:
                connection.execute(
                    "UPDATE queue_items SET position = position - 1 WHERE guild_id = ? AND position > ?",
                    (item["guild_id"], item["position"]),
                )
        return auction_id


def save_payment_method(auction_id: int, payment_method: str):
    with connect() as connection:
        connection.execute(
            "UPDATE auctions SET payment_method = ? WHERE id = ?",
            (payment_method[:500], auction_id),
        )


def set_transaction_status(auction_id: int, status: str):
    with connect() as connection:
        connection.execute(
            "UPDATE auctions SET transaction_status = ? WHERE id = ?",
            (status, auction_id),
        )


def set_ticket_transaction_status(auction_id: int, status: str) -> int:
    """Apply a transaction status to every auction sharing the ticket.

    A winner ticket can hold several auctions, but staff pay and claim the
    ticket as a whole. Setting the status on a single auction would move the
    ticket to paid-and-claimed while the other auctions on it still showed as
    unpaid, so the status is written to every open auction in the ticket.
    """
    auction = fetch_auction(auction_id)
    if auction is None or not auction["winner_channel_id"]:
        set_transaction_status(auction_id, status)
        return 1
    with connect() as connection:
        result = connection.execute(
            """
            UPDATE auctions
            SET transaction_status = ?
            WHERE winner_channel_id = ? AND transaction_status != 'closed'
            """,
            (status, auction["winner_channel_id"]),
        )
    return result.rowcount


def switch_winner(auction_id: int, new_winner_id: int, new_final_bid: int) -> None:
    """Point an ended auction at a new winner and reset its payment state.

    The existing winner channel is deliberately kept so the ticket stays a
    winner ticket and the vouch reminders keep working. The payment method is
    reset so the new winner must select one.
    """
    with connect() as connection:
        connection.execute(
            """
            UPDATE auctions
            SET winner_id = ?, final_bid = ?, highest_bidder_id = ?,
                current_bid = ?,
                payment_method = NULL,
                transaction_status = 'waiting_to_pay'
            WHERE id = ?
            """,
            (new_winner_id, new_final_bid, new_winner_id, new_final_bid, auction_id),
        )


def payment_method_enabled(guild_id: int, method: str) -> bool:
    if method not in PAYMENT_METHODS:
        return False
    with connect() as connection:
        row = connection.execute(
            "SELECT enabled FROM payment_method_settings WHERE guild_id = ? AND method = ?",
            (guild_id, method),
        ).fetchone()
    return bool(row["enabled"]) if row else DEFAULT_PAYMENT_METHOD_AVAILABILITY[method]


def set_payment_method_enabled(guild_id: int, method: str, enabled: bool):
    if method not in PAYMENT_METHODS:
        raise ValueError(f"Unsupported payment method: {method}")
    with connect() as connection:
        connection.execute(
            """
            INSERT INTO payment_method_settings(guild_id, method, enabled)
            VALUES (?, ?, ?)
            ON CONFLICT(guild_id, method) DO UPDATE SET enabled = excluded.enabled
            """,
            (guild_id, method, int(enabled)),
        )


async def get_transaction_category(guild: discord.Guild, category_name: str) -> discord.CategoryChannel:
    category = discord.utils.get(guild.categories, name=category_name)
    if category:
        return category
    overwrites = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False),
    }
    for role_id in QUEUE_ROLE_IDS:
        role = guild.get_role(role_id)
        if role:
            overwrites[role] = discord.PermissionOverwrite(
                view_channel=True,
                read_message_history=True,
                send_messages=True,
                manage_messages=True,
            )
    if guild.me:
        overwrites[guild.me] = discord.PermissionOverwrite(
            view_channel=True,
            read_message_history=True,
            send_messages=True,
            manage_channels=True,
            manage_messages=True,
        )
    return await guild.create_category(category_name, overwrites=overwrites, reason="Create auction transaction category")


async def get_seller_ticket_category(guild: discord.Guild) -> discord.CategoryChannel:
    category = discord.utils.get(guild.categories, name=SELLER_TICKET_CATEGORY)
    if category:
        return category
    overwrites = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False),
    }
    for role_id in QUEUE_ROLE_IDS:
        role = guild.get_role(role_id)
        if role:
            overwrites[role] = discord.PermissionOverwrite(
                view_channel=True,
                read_message_history=True,
                send_messages=True,
                manage_messages=True,
            )
    if guild.me:
        overwrites[guild.me] = discord.PermissionOverwrite(
            view_channel=True,
            read_message_history=True,
            send_messages=True,
            manage_channels=True,
            manage_messages=True,
        )
    return await guild.create_category(
        SELLER_TICKET_CATEGORY,
        overwrites=overwrites,
        reason="Create private seller auction ticket category",
    )


def fee_for_amount(amount: int) -> int:
    """Return the flat fee for an amount.

    Tiers are checked highest-first, so $160 returns $5 even though it is also
    "$150+" and "$100+". Call this with a winner ticket's combined winning
    total, never with a single win that is then added to other wins' fees.
    """
    for threshold, fee in WINNER_FEE_TIERS:
        if amount >= threshold:
            return fee
    return WINNER_FEE_TIERS[-1][1]


def amount_due_for(winning_total: int, fee_total: int) -> int:
    """Winning total plus the fees charged for the auctions won."""
    return winning_total + fee_total


def winner_ticket_winning_total(rows: list[sqlite3.Row]) -> int:
    """Sum of every winning amount on a ticket."""
    return sum(row["final_bid"] for row in rows)


def winner_ticket_fees(rows: list[sqlite3.Row]) -> int:
    """Total fee for a winner ticket.

    There is exactly ONE fee per ticket, read from the tier table using the
    ticket's combined winning total. Fees are never summed per auction, so two
    $64 wins are $128 together and the ticket owes $3 once instead of $1 + $1:
        $150 or more -> $5
        $100 to $149 -> $3
        $50 to $99   -> $2
        under $50    -> $1
    """
    if not rows:
        return 0
    return fee_for_amount(winner_ticket_winning_total(rows))


def winner_ticket_lines(rows: list[sqlite3.Row]) -> str:
    """One line per auction win, showing the winning amount.

    The ticket fee is shown once, on the totals, so individual lines never
    carry their own fee.
    """
    return "\n".join(
        f"`{auction_reference(row)}` — {row['item']} — **${format_amount(row['final_bid'])}**"
        for row in rows
    )


def winner_ticket_total(winner_channel_id: int | None) -> tuple[int, int]:
    """Return the total winning amount and item count for a winner ticket.

    Rows with no final_bid are excluded so this agrees with
    winner_ticket_items(); otherwise a won auction whose bid was later removed
    was counted as a win while contributing nothing to the total.
    """
    if not winner_channel_id:
        return 0, 0
    with connect() as connection:
        row = connection.execute(
            "SELECT COALESCE(SUM(final_bid), 0) AS total, COUNT(*) AS wins "
            "FROM auctions WHERE winner_channel_id = ? AND winner_id IS NOT NULL "
            "AND final_bid IS NOT NULL",
            (winner_channel_id,),
        ).fetchone()
    return row["total"], row["wins"]


def winner_ticket_items(winner_channel_id: int | None) -> list[sqlite3.Row]:
    """Return every won auction on a winner ticket, oldest first."""
    if not winner_channel_id:
        return []
    with connect() as connection:
        return connection.execute(
            "SELECT * FROM auctions "
            "WHERE winner_channel_id = ? AND winner_id IS NOT NULL AND final_bid IS NOT NULL "
            "ORDER BY ended_at ASC, id ASC",
            (winner_channel_id,),
        ).fetchall()


def build_winner_receipt(winner_channel_id: int, winner: discord.Member | None, channel: discord.TextChannel) -> discord.Embed:
    """Build a full receipt of everything the winner bought on this ticket."""
    rows = winner_ticket_items(winner_channel_id)
    winning_total = sum(row["final_bid"] for row in rows)
    fees = winner_ticket_fees(rows)
    total_due = amount_due_for(winning_total, fees)
    embed = discord.Embed(
        title="🧾 Receipt | Bid$ Auction Wins",
        description=(
            f"Hi {winner.mention if winner else 'there'}! Here is your full receipt for every "
            f"auction you won in this ticket. Thank you for bidding with us!"
        ),
        color=discord.Color.gold(),
        timestamp=datetime.now(timezone.utc),
    )
    # Discord caps field values at 1,024 characters, so long receipts are split.
    lines = [
        f"`{auction_reference(row)}` — **{row['item']}** — **${format_amount(row['final_bid'])}**"
        + (f"\nPayment: {row['payment_method']}" if row["payment_method"] else "")
        for row in rows
    ] or ["No completed auction wins were recorded on this ticket."]
    fields: list[str] = []
    current: list[str] = []
    length = 0
    for line in lines:
        if current and length + len(line) + 1 > 1000:
            fields.append("\n".join(current))
            current, length = [], 0
        current.append(line)
        length += len(line) + 1
    if current:
        fields.append("\n".join(current))
    for number, chunk in enumerate(fields, start=1):
        embed.add_field(
            name="Items won" if len(fields) == 1 else f"Items won ({number}/{len(fields)})",
            value=chunk,
            inline=False,
        )
    embed.add_field(name="Auctions won", value=f"**{len(rows)}**", inline=True)
    embed.add_field(
        name="Winning total",
        value=f"**${format_amount(winning_total)}**",
        inline=True,
    )
    embed.add_field(
        name="Fees",
        value=(
            f"**${format_amount(fees)}** (one fee per ticket, based on the "
            f"${format_amount(winning_total)} winning total: $1 under $50, "
            f"$2 from $50, $3 from $100, $5 from $150)"
        ),
        inline=True,
    )
    embed.add_field(
        name="Total amount due",
        value=f"**${format_amount(total_due)}**",
        inline=True,
    )
    embed.add_field(
        name="Paid via",
        value=", ".join(sorted({row["payment_method"] for row in rows if row["payment_method"]})) or "Not recorded",
        inline=True,
    )
    embed.add_field(
        name="Completed",
        value=f"<t:{int(now())}:F>",
        inline=True,
    )
    embed.set_footer(text=f"Ticket: {channel.name} | Total amount due: ${format_amount(total_due)}")
    if rows and rows[0]["photo_url"]:
        embed.set_thumbnail(url=rows[0]["photo_url"])
    return embed


async def send_winner_receipt(winner_channel_id: int, channel: discord.TextChannel) -> bool:
    """DM the winner their receipt once the ticket is paid and claimed."""
    with connect() as connection:
        row = connection.execute(
            "SELECT winner_id FROM auctions "
            "WHERE winner_channel_id = ? AND winner_id IS NOT NULL "
            "ORDER BY ended_at DESC LIMIT 1",
            (winner_channel_id,),
        ).fetchone()
    if row is None:
        return False
    winner = channel.guild.get_member(row["winner_id"])
    if winner is None:
        try:
            winner = await channel.guild.fetch_member(row["winner_id"])
        except discord.HTTPException:
            winner = None
    embed = build_winner_receipt(winner_channel_id, winner, channel)
    try:
        if winner is None:
            await channel.send(embed=embed)
            return True
        await winner.send(embed=embed)
        return True
    except discord.Forbidden:
        await send_log(
            channel.guild.id,
            f"Could not DM the receipt for ticket {channel.mention} to <@{row['winner_id']}> because their DMs are closed.",
            title="Receipt Not Delivered",
            color=discord.Color.red(),
        )
        await channel.send(embed=embed)
        return False
    except discord.HTTPException as error:
        await send_log(
            channel.guild.id,
            f"Could not deliver the receipt for ticket {channel.mention}: {error}",
            title="Receipt Not Delivered",
            color=discord.Color.red(),
        )
        return False


def create_seller_ticket_record(
    guild_id: int,
    channel_id: int,
    user_id: int,
    fortnite_username: str,
    bid_details: str,
    game_type: str,
    payment_method: str,
    item_details: str,
) -> int:
    with connect() as connection:
        ticket_id = connection.execute(
            """
            INSERT INTO seller_tickets(
                guild_id, channel_id, user_id, fortnite_username, bid_details,
                game_type, payment_method, item_details, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                guild_id,
                channel_id,
                user_id,
                fortnite_username[:100],
                bid_details[:500],
                game_type[:50],
                payment_method[:100],
                item_details[:1000],
                now(),
            ),
        ).lastrowid
    return ticket_id


class SellerTicketView(discord.ui.View):
    def __init__(self, ticket_id: int):
        super().__init__(timeout=None)
        self.ticket_id = ticket_id
        self.children[0].custom_id = f"seller-ticket:{ticket_id}:close"

    @discord.ui.button(label="Close ticket", style=discord.ButtonStyle.danger, custom_id="seller-ticket:close")
    async def close_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            await interaction.response.send_message("Only auction staff can close seller tickets.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        deleted, transcript_url = await close_ticket_channel(
            interaction.channel,
            interaction.guild.id,
            interaction.user,
            f"Seller Auction Request #{self.ticket_id}",
            delete_channel=False,
        )
        if deleted:
            with connect() as connection:
                connection.execute(
                    "UPDATE seller_tickets SET status = 'closed', closed_at = ? WHERE id = ?",
                    (now(), self.ticket_id),
                )
            await interaction.followup.send(
                f"Ticket transcript sent and ticket deleted. [View transcript]({transcript_url})",
                ephemeral=True,
            )
            await interaction.channel.delete(reason=f"Ticket closed by {interaction.user}")
        else:
            await interaction.followup.send("I could not send the transcript, so the ticket was kept.", ephemeral=True)


class SellerTicketModal(discord.ui.Modal, title="Auction Request"):
    fortnite_username = discord.ui.TextInput(
        label="Fortnite username",
        placeholder="Username where the items are located",
        max_length=100,
    )
    bid_details = discord.ui.TextInput(
        label="Starting bid and reserve price",
        placeholder="Example: Starting 100, reserve none (or 250)",
        max_length=500,
    )
    game_type = discord.ui.TextInput(
        label="Goup or STB?",
        placeholder="Enter Goup or STB",
        max_length=50,
    )
    payment_method = discord.ui.TextInput(
        label="Preferred payment method",
        placeholder="PayPal, Cash App, Venmo, Apple Pay, Revolut, or Crypto",
        max_length=100,
    )
    item_details = discord.ui.TextInput(
        label="How many items and what items?",
        placeholder="Example: 3 items - item 1, item 2, item 3",
        style=discord.TextStyle.paragraph,
        max_length=1000,
    )

    async def on_submit(self, interaction: discord.Interaction):
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("Seller tickets can only be created in a server.", ephemeral=True)
            return
        if not seller_tickets_enabled(guild.id):
            await interaction.response.send_message("Seller auction tickets are temporarily disabled because the queue is full. Please try again later.", ephemeral=True)
            return
        category = await get_seller_ticket_category(guild)
        overwrites = {
            # Private ticket: @everyone is denied outright, so no other member
            # of the server can see or read it. Only the seller who opened it
            # and staff are granted access below.
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            # The seller can read and talk in their own ticket so they can answer
            # questions and add anything staff asked for.
            interaction.user: discord.PermissionOverwrite(
                view_channel=True,
                read_message_history=True,
                send_messages=True,
                attach_files=True,
                mention_everyone=False,
            ),
        }
        manager_roles = get_manager_roles(guild)
        for manager_role in manager_roles:
            overwrites[manager_role] = discord.PermissionOverwrite(
                view_channel=True,
                read_message_history=True,
                send_messages=True,
                manage_messages=True,
            )
        for role_id in QUEUE_ROLE_IDS:
            role = guild.get_role(role_id)
            if role:
                overwrites[role] = discord.PermissionOverwrite(
                    view_channel=True,
                    read_message_history=True,
                    send_messages=True,
                    manage_messages=True,
                )
        if guild.me:
            overwrites[guild.me] = discord.PermissionOverwrite(
                view_channel=True,
                read_message_history=True,
                send_messages=True,
                manage_channels=True,
                manage_messages=True,
            )
        safe_name = re.sub(r"[^a-z0-9-]", "-", interaction.user.name.lower()).strip("-") or str(interaction.user.id)
        channel = await guild.create_text_channel(
            name=f"auction-request-{safe_name[:70]}",
            category=category,
            overwrites=overwrites,
            topic="Private seller request for auction intake",
            reason="Create seller auction request ticket",
        )
        ticket_id = create_seller_ticket_record(
            guild.id,
            channel.id,
            interaction.user.id,
            self.fortnite_username.value,
            self.bid_details.value,
            self.game_type.value,
            self.payment_method.value,
            self.item_details.value,
        )
        embed = discord.Embed(
            title=f"Auction Request #{ticket_id}",
            description="Seller intake received. Please upload clear pictures of every item in this ticket.",
            color=discord.Color.blurple(),
        )
        embed.add_field(name="Fortnite username", value=self.fortnite_username.value, inline=False)
        embed.add_field(name="Starting bid / reserve", value=self.bid_details.value, inline=False)
        embed.add_field(name="Goup or STB", value=self.game_type.value, inline=True)
        embed.add_field(name="Payment method", value=self.payment_method.value, inline=True)
        embed.add_field(name="Items", value=self.item_details.value, inline=False)
        # Only the auction manager role is pinged. If none is configured, the
        # "Bidders (ping)" alert role is never substituted, so a private ticket
        # can never be announced to the whole server.
        manager_ping = manager_ping_text(guild)
        if not manager_ping:
            manager_ping = "\n⚠️ No auction manager role could be found, so nobody was pinged. Run `/auction_setup` to fix this.\n"
        await channel.send(
            content=(
                f"{manager_ping}\n".rstrip()
                + "🔔 **New auction request ticket.**\n"
                "Please upload pictures of all items here so staff can review them."
            ),
            embed=embed,
            view=SellerTicketView(ticket_id),
            # Nothing here may ping @everyone or any role other than the managers.
            allowed_mentions=discord.AllowedMentions(roles=get_manager_roles(guild)),
        )
        # The seller needs the link: the ticket is private to them and staff, so
        # this reveals nothing to anyone else.
        await interaction.response.send_message(
            f"✅ Your auction request ticket is ready: {channel.mention}\n"
            "The auction manager has been notified and will help you there.",
            ephemeral=True,
        )
        await send_log(guild.id, f"New seller auction request **#{ticket_id}** created by {interaction.user.mention} in {channel.mention}.", title="New Auction Request", color=discord.Color.blue())


class SellerTicketPanelView(discord.ui.View):
    def __init__(self, enabled: bool = True):
        super().__init__(timeout=None)
        self.children[0].disabled = not enabled

    @discord.ui.button(label="Create auction ticket", style=discord.ButtonStyle.primary, custom_id="seller-ticket:create")
    async def create_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not seller_tickets_enabled(interaction.guild.id):
            await interaction.response.send_message("Seller auction tickets are temporarily disabled because the queue is full.", ephemeral=True)
            return
        await interaction.response.send_modal(SellerTicketModal())

    @discord.ui.button(
        label="Staff ticket controls",
        style=discord.ButtonStyle.secondary,
        custom_id="seller-ticket:staff-controls",
        row=1,
    )
    async def staff_ticket_controls(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Open the seller-ticket controls without requiring a slash command."""
        if not await require_staff(interaction):
            return
        await interaction.response.send_message(
            embed=ticket_dashboard_embed(interaction.guild.id),
            view=TicketDashboardView(),
            ephemeral=True,
        )


def ticket_dashboard_embed(guild_id: int) -> discord.Embed:
    enabled = seller_tickets_enabled(guild_id)
    with connect() as connection:
        seller_open = connection.execute(
            "SELECT COUNT(*) AS count FROM seller_tickets WHERE guild_id = ? AND status = 'open'",
            (guild_id,),
        ).fetchone()["count"]
        seller_total = connection.execute(
            "SELECT COUNT(*) AS count FROM seller_tickets WHERE guild_id = ?",
            (guild_id,),
        ).fetchone()["count"]
        seller_closed = connection.execute(
            "SELECT COUNT(*) AS count FROM seller_tickets WHERE guild_id = ? AND status = 'closed'",
            (guild_id,),
        ).fetchone()["count"]
        winner_total = connection.execute(
            "SELECT COUNT(*) AS count FROM auctions WHERE guild_id = ? AND winner_channel_id IS NOT NULL",
            (guild_id,),
        ).fetchone()["count"]
        winner_open = connection.execute(
            "SELECT COUNT(*) AS count FROM auctions WHERE guild_id = ? AND winner_channel_id IS NOT NULL AND transaction_status != 'closed'",
            (guild_id,),
        ).fetchone()["count"]
        winner_closed = connection.execute(
            "SELECT COUNT(*) AS count FROM auctions WHERE guild_id = ? AND winner_channel_id IS NOT NULL AND transaction_status = 'closed'",
            (guild_id,),
        ).fetchone()["count"]
        active_auctions = connection.execute(
            "SELECT COUNT(*) AS count FROM auctions WHERE guild_id = ? AND status = 'active'",
            (guild_id,),
        ).fetchone()["count"]
        active_schedules = connection.execute(
            "SELECT COUNT(*) AS count FROM schedules WHERE guild_id = ? AND enabled = 1",
            (guild_id,),
        ).fetchone()["count"]
        active_announcements = connection.execute(
            "SELECT COUNT(*) AS count FROM scheduled_announcements WHERE guild_id = ? AND enabled = 1",
            (guild_id,),
        ).fetchone()["count"]
    embed = discord.Embed(
        title="🎟️ Ticket Dashboard",
        description=(
            f"Seller ticket intake is **{'enabled' if enabled else 'disabled'}**.\n"
            "Use the controls below to pause or resume new seller tickets."
        ),
        color=discord.Color.green() if enabled else discord.Color.red(),
    )
    embed.add_field(
        name="Seller auction tickets",
        value=(
            f"Open: **{seller_open}**\n"
            f"Created: **{seller_total}**\n"
            f"Closed history: **{seller_closed}**"
        ),
        inline=True,
    )
    embed.add_field(
        name="Winner payment tickets",
        value=(
            f"Open: **{winner_open}**\n"
            f"Created: **{winner_total}**\n"
            f"Closed history: **{winner_closed}**"
        ),
        inline=True,
    )
    embed.add_field(
        name="Auction activity",
        value=(
            f"Active auctions: **{active_auctions}**\n"
            f"Recurring auctions: **{active_schedules}**\n"
            f"Scheduled announcements: **{active_announcements}**"
        ),
        inline=False,
    )
    embed.set_footer(text="Winner payment tickets are never disabled by the seller-ticket controls.")
    return embed


class TicketDashboardView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=300)

    async def guard(self, interaction: discord.Interaction) -> bool:
        if not is_staff(interaction.user):
            await interaction.response.send_message("Only auction staff can manage seller tickets.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Disable seller tickets", style=discord.ButtonStyle.danger)
    async def disable(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self.guard(interaction):
            return
        await interaction.response.defer()
        set_seller_tickets_enabled(interaction.guild.id, False)
        await ensure_ticket_panel(interaction.guild)
        await interaction.edit_original_response(embed=ticket_dashboard_embed(interaction.guild.id), view=self)

    @discord.ui.button(label="Enable seller tickets", style=discord.ButtonStyle.success)
    async def enable(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self.guard(interaction):
            return
        await interaction.response.defer()
        set_seller_tickets_enabled(interaction.guild.id, True)
        await ensure_ticket_panel(interaction.guild)
        await interaction.edit_original_response(embed=ticket_dashboard_embed(interaction.guild.id), view=self)


def ticket_panel_embed(enabled: bool = True) -> discord.Embed:
    status_text = (
        "Click the button below to open a private auction request ticket."
        if enabled
        else "Seller auction tickets are currently paused because the queue is full."
    )
    return discord.Embed(
        title="📨 Submit Items for Auction",
        description=f"{status_text} You will be asked for your Fortnite username, bid preferences, Goup or STB, payment method, and item list. You can upload item pictures inside the ticket.",
        color=discord.Color.blurple(),
    ).add_field(
        name="Before opening a ticket",
        value="Have your item pictures ready and make sure every item is clearly visible.",
        inline=False,
    )


async def find_ticket_panel_message(guild: discord.Guild):
    """Locate the existing seller-ticket panel message in this server.

    The saved message ID is only useful together with its channel, and the
    channel was previously only known from an environment variable. The panel
    is now found by scanning the server for the stored message ID, so an
    existing panel keeps working even when TICKET_PANEL_CHANNEL_ID is unset.
    """
    config = get_config(guild.id)
    if not config or not config["ticket_panel_message_id"]:
        return None, None
    message_id = config["ticket_panel_message_id"]
    for channel in guild.text_channels:
        try:
            message = await channel.fetch_message(message_id)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            continue
        return channel, message
    return None, None


async def ensure_ticket_panel_channel(guild: discord.Guild) -> discord.TextChannel | None:
    """Return the channel holding the seller-ticket panel, creating one if needed."""
    # The configured channel wins, then the built-in default, then a channel with
    # the expected name, and only then a new channel is created.
    for candidate in (TICKET_PANEL_CHANNEL_ID, DEFAULT_TICKET_PANEL_CHANNEL_ID):
        if candidate is None:
            continue
        channel = guild.get_channel(candidate) or bot.get_channel(candidate)
        if isinstance(channel, discord.TextChannel):
            return channel
    channel = discord.utils.get(guild.text_channels, name=DEFAULT_TICKET_PANEL_CHANNEL_NAME)
    if channel is not None:
        return channel
    try:
        return await guild.create_text_channel(
            DEFAULT_TICKET_PANEL_CHANNEL_NAME,
            topic="Submit your items for auction.",
            reason="Create the seller auction ticket panel channel",
        )
    except (discord.Forbidden, discord.HTTPException) as error:
        print(f"Could not create the ticket panel channel: {error}")
        return None


async def ensure_ticket_panel(guild: discord.Guild | None = None):
    """Post or refresh the seller-ticket panel, the entry point for seller tickets.

    Previously this returned early whenever TICKET_PANEL_CHANNEL_ID was unset,
    which removed the "Submit Items for Auction" button and with it the whole
    seller-ticket flow. The channel is now resolved from the saved panel
    message or created on demand, so the panel is always restored.
    """
    if guild is None:
        guild = bot.get_guild(int(GUILD_ID)) if GUILD_ID and GUILD_ID.isdigit() else None
    if guild is None:
        print("Could not identify a server for the ticket panel; skipping.")
        return

    existing_channel, panel_message = await find_ticket_panel_message(guild)
    if existing_channel is not None:
        channel = existing_channel
    else:
        channel = await ensure_ticket_panel_channel(guild)
    if channel is None:
        return
    enabled = seller_tickets_enabled(guild.id)
    if panel_message is None:
        panel_message = await channel.send(
            embed=ticket_panel_embed(enabled), view=SellerTicketPanelView(enabled)
        )
    else:
        await panel_message.edit(
            embed=ticket_panel_embed(enabled), view=SellerTicketPanelView(enabled)
        )
    with connect() as connection:
        connection.execute(
            """
            INSERT INTO guild_config(guild_id, ticket_panel_message_id)
            VALUES (?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET ticket_panel_message_id = excluded.ticket_panel_message_id
            """,
            (guild.id, panel_message.id),
        )
    bot.add_view(SellerTicketPanelView(enabled), message_id=panel_message.id)


async def move_winner_channel(auction: sqlite3.Row, status: str):
    if not auction["winner_channel_id"]:
        return
    guild = bot.get_guild(auction["guild_id"])
    channel = guild.get_channel(auction["winner_channel_id"]) if guild else None
    if channel is None:
        return
    category_name = {
        "waiting_to_pay": WAITING_TO_PAY_CATEGORY,
        "paid_not_claimed": PAID_NOT_CLAIMED_CATEGORY,
        "paid_and_claimed": PAID_AND_CLAIMED_CATEGORY,
    }[status]
    category = await get_transaction_category(guild, category_name)
    await channel.edit(category=category, reason=f"Auction transaction moved to {category_name}")


async def enable_winner_chat(
    guild: discord.Guild,
    channel: discord.TextChannel,
    winner_id: int,
    reason: str,
):
    winner = guild.get_member(winner_id)
    if winner is None:
        winner = await guild.fetch_member(winner_id)
    await channel.set_permissions(
        winner,
        view_channel=True,
        send_messages=True,
        read_message_history=True,
        attach_files=True,
        mention_everyone=False,
        reason=reason,
    )


async def send_winner_vouch_reminder(channel: discord.TextChannel, winner_id: int):
    await channel.send(
        f"<@{winner_id}> Thanks for using Bid$. Please make sure to vouch for the auction manager/owner who helped you today in <#{TRANSCRIPT_CHANNEL_ID}> please and thank you :)"
    )


async def post_winner_switch_notice(
    channel: discord.TextChannel,
    auction: sqlite3.Row,
    replacement: discord.Member,
    amount: int,
    actor: discord.Member,
):
    """Announce a winner change in the ticket and refresh its payment controls."""
    embed = discord.Embed(
        title=f"New Winner | Auction #{auction['id']}",
        description=(
            f"**Item:** {auction['item']}\n"
            f"**New winner:** {replacement.mention}\n"
            f"**Winning amount:** {format_amount(amount)}\n"
            f"**Auction ID:** #{auction['id']}"
        ),
        color=discord.Color.orange(),
    )
    if auction["photo_url"]:
        embed.set_thumbnail(url=auction["photo_url"])
    embed.set_footer(text=f"Winner changed by {actor}")
    try:
        ticket_message = await channel.send(
            content=(
                f"{replacement.mention}\n"
                f"🔄 **The winner of this auction is now you.**\n"
                f"Staff, please assist the new winner. Select a payment method below to unlock chat."
            ),
            embed=embed,
            view=PaymentView(auction["id"]),
        )
        with connect() as connection:
            connection.execute(
                "UPDATE auctions SET winner_message_id = ? WHERE id = ?",
                (ticket_message.id, auction["id"]),
            )
    except (discord.Forbidden, discord.HTTPException):
        pass
    try:
        await replacement.send(
            f"🔄 You are now the winner of auction **#{auction['id']}**!\n"
            f"**Item:** {auction['item']}\n"
            f"**Winning amount:** {format_amount(amount)}\n\n"
            f"Please enter the server and complete your payment here: "
            f"https://discord.com/channels/{channel.guild.id}/{channel.id}"
        )
    except (discord.Forbidden, discord.HTTPException):
        pass


class PaymentSelect(discord.ui.Select):
    def __init__(self, auction_id: int):
        self.auction_id = auction_id
        super().__init__(
            placeholder="Choose payment method",
            custom_id=f"winner:{auction_id}:payment-method",
            options=[discord.SelectOption(label=method, value=method) for method in PAYMENT_METHODS],
        )

    async def callback(self, interaction: discord.Interaction):
        auction = fetch_auction(self.auction_id)
        if not auction or auction["winner_id"] != interaction.user.id:
            await interaction.response.send_message("Only the auction winner can choose the payment method.", ephemeral=True)
            return
        method = self.values[0]
        if not payment_method_enabled(interaction.guild.id, method):
            await interaction.response.send_message(
                "This payment method is not available at the moment. Please try a different payment method. Thank you :)",
                ephemeral=True,
            )
            return
        await interaction.response.defer(ephemeral=True)
        save_payment_method(self.auction_id, method)
        try:
            await enable_winner_chat(
                interaction.guild,
                interaction.channel,
                auction["winner_id"],
                "Winner selected a payment method",
            )
        except (discord.Forbidden, discord.HTTPException):
            await interaction.followup.send(
                f"Your payment method was saved as **{method}**, but staff could not unlock chat automatically.",
                ephemeral=True,
            )
        else:
            await interaction.followup.send(
                f"Your payment method was saved as **{method}**. You can now chat in this ticket.",
                ephemeral=True,
            )
        await send_log(
            interaction.guild.id,
            f"💳 Payment method received for auction **#{self.auction_id}** from {interaction.user.mention}: **{method}**",
        )
        await interaction.channel.send(
            f"💳 **Payment method submitted:** {method}\n"
            f"Staff can now continue the transaction with {interaction.user.mention}."
        )


class PaymentView(discord.ui.View):
    def __init__(
        self,
        auction_id: int,
        include_payment_select: bool = True,
        include_status_buttons: bool = True,
        include_cashout: bool = True,
    ):
        super().__init__(timeout=None)
        self.auction_id = auction_id
        # Persistent views are registered again on restart.  These IDs must be
        # unique per auction or a button can be dispatched to another ticket.
        if include_cashout:
            self.cashout.custom_id = f"winner:{auction_id}:cashout"
        else:
            # Cash-out only lives on the first win of a ticket, so the total is
            # calculated once for every win the member holds on that ticket.
            self.remove_item(self.cashout)
        if include_status_buttons:
            self.mark_paid.custom_id = f"winner:{auction_id}:mark-paid"
            self.mark_claimed.custom_id = f"winner:{auction_id}:mark-claimed"
        else:
            # A repeat win on an existing ticket only needs the cash-out button.
            self.remove_item(self.mark_paid)
            self.remove_item(self.mark_claimed)
        if include_payment_select:
            self.add_item(PaymentSelect(auction_id))
        if not self.children:
            # Discord rejects a view with no components, and a repeat win with
            # every control switched off would post nothing at all. The payment
            # select is the fallback so the ticket message is always usable.
            self.add_item(PaymentSelect(auction_id))

    @discord.ui.button(label="Cashout", style=discord.ButtonStyle.green, custom_id="winner:cashout")
    async def cashout(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Let the winner tell staff they are finished bidding for the day."""
        auction = fetch_auction(self.auction_id)
        if not auction or not auction["winner_channel_id"]:
            await interaction.response.send_message("Winner ticket not found.", ephemeral=True)
            return
        if interaction.user.id != auction["winner_id"]:
            await interaction.response.send_message(
                "Only the auction winner can cash out from this ticket.", ephemeral=True
            )
            return
        button.disabled = True
        # Only ping the configured auction manager role, never the owner or
        # bot developer roles.
        staff_ping = manager_ping_text(interaction.guild)
        winning_total, wins = winner_ticket_total(auction["winner_channel_id"])
        breakdown = winner_ticket_items(auction["winner_channel_id"])
        # One tiered fee for the whole ticket, based on the combined total of
        # every auction won here — never one fee per auction added together.
        fees = winner_ticket_fees(breakdown)
        total_due = amount_due_for(winning_total, fees)
        item_lines = winner_ticket_lines(breakdown) or "No won items were found on this ticket."
        await interaction.response.send_message(
            f"💸 **Cash-out requested.**\n"
            f"**Winning total:** ${format_amount(winning_total)} "
            f"({wins} auction win{'s' if wins != 1 else ''})\n"
            f"**Fee{'s' if wins != 1 else ''}:** ${format_amount(fees)} "
            f"(one fee per ticket on the ${format_amount(winning_total)} winning total — "
            f"$1 under $50, $2 from $50, $3 from $100, $5 from $150)\n"
            f"**Total amount due: ${format_amount(total_due)}** (winning total + fee)\n\n"
            f"{item_lines}",
            ephemeral=True,
        )
        try:
            await interaction.channel.send(
                f"{staff_ping}\n".rstrip()
                + f"\n💸 **{interaction.user.mention} has cashed out.**\n"
                "They are done bidding for the day. Staff, please assist them in this ticket.\n"
                f"**Winning total:** ${format_amount(winning_total)} "
                f"({wins} win{'s' if wins != 1 else ''})\n"
                f"**Fee{'s' if wins != 1 else ''}:** ${format_amount(fees)} "
                f"(based on the ${format_amount(winning_total)} winning total)\n"
                f"**Total to collect (winning total + fee):** **${format_amount(total_due)}**\n\n"
                f"{item_lines}"
            )
        except discord.HTTPException:
            pass
        log_action(
            interaction.guild.id,
            interaction.user.id,
            "cashout",
            self.auction_id,
        )
        await send_log(
            interaction.guild.id,
            f"💸 **Cash out requested** in {interaction.channel.mention} by {interaction.user.mention}.\n"
            f"**Winning total:** ${format_amount(winning_total)} | **Fees:** ${format_amount(fees)} | "
            f"**Total amount due:** **${format_amount(total_due)}** across {wins} win(s) on this ticket.",
            title="Winner Cash Out",
            color=discord.Color.green(),
        )

    @discord.ui.button(label="Mark paid", style=discord.ButtonStyle.success, custom_id="winner:mark-paid")
    async def mark_paid(self, interaction: discord.Interaction, button: discord.ui.Button):
        auction = fetch_auction(self.auction_id)
        if not auction or not auction["winner_channel_id"]:
            await interaction.response.send_message("Winner ticket not found.", ephemeral=True)
            return
        if interaction.user.id == auction["winner_id"]:
            await interaction.response.send_message("The auction winner cannot mark their own ticket paid.", ephemeral=True)
            return
        if not is_staff(interaction.user):
            await interaction.response.send_message("Only auction staff can mark tickets paid.", ephemeral=True)
            return
        updated_count = set_ticket_transaction_status(self.auction_id, "paid_not_claimed")
        await move_winner_channel(auction, "paid_not_claimed")
        plural = "" if updated_count == 1 else "s"
        await interaction.response.send_message(
            f"Payment recorded on {updated_count} auction{plural} in this ticket. "
            "Ticket moved to **paid-not-claimed**.",
            ephemeral=True,
        )

    @discord.ui.button(label="Mark claimed", style=discord.ButtonStyle.primary, custom_id="winner:mark-claimed")
    async def mark_claimed(self, interaction: discord.Interaction, button: discord.ui.Button):
        auction = fetch_auction(self.auction_id)
        if not auction or auction["transaction_status"] != "paid_not_claimed":
            await interaction.response.send_message("Mark the ticket paid before marking it claimed.", ephemeral=True)
            return
        if interaction.user.id == auction["winner_id"]:
            await interaction.response.send_message("The auction winner cannot mark their own ticket claimed.", ephemeral=True)
            return
        if not is_staff(interaction.user):
            await interaction.response.send_message("Only auction staff can mark tickets claimed.", ephemeral=True)
            return
        updated_count = set_ticket_transaction_status(self.auction_id, "paid_and_claimed")
        await move_winner_channel(auction, "paid_and_claimed")
        await send_winner_vouch_reminder(interaction.channel, auction["winner_id"])
        delivered = await send_winner_receipt(auction["winner_channel_id"], interaction.channel)
        plural = "" if updated_count == 1 else "s"
        await interaction.response.send_message(
            f"{updated_count} auction{plural} in this ticket marked claimed. "
            "Ticket moved to **paid-and-claimed**. "
            + (
                "A receipt was DM'd to the winner."
                if delivered
                else "The receipt could not be DM'd, so it was posted in the ticket instead."
            ),
            ephemeral=True,
        )


async def create_winner_channel(auction: sqlite3.Row):
    if not auction["winner_id"] or auction["winner_channel_id"]:
        return auction["winner_channel_id"]
    guild = bot.get_guild(auction["guild_id"])
    if guild is None:
        return None
    winner = guild.get_member(auction["winner_id"])
    if winner is None:
        try:
            winner = await guild.fetch_member(auction["winner_id"])
        except discord.HTTPException:
            return None

    with connect() as connection:
        existing = connection.execute(
            "SELECT winner_channel_id FROM auctions "
            "WHERE guild_id = ? AND winner_id = ? AND winner_channel_id IS NOT NULL "
            "ORDER BY ended_at DESC LIMIT 1",
            (auction["guild_id"], auction["winner_id"]),
        ).fetchone()
    if existing:
        channel = guild.get_channel(existing["winner_channel_id"])
        if channel:
            with connect() as connection:
                connection.execute(
                    "UPDATE auctions SET winner_channel_id = ? WHERE id = ?",
                    (channel.id, auction["id"]),
                )
            embed = discord.Embed(
                title=f"New Auction Win | #{auction['id']}",
                description=(
                    f"**Item:** {auction['item']}\n"
                    f"**Winning amount:** {format_amount(auction['final_bid'])}\n"
                    f"**Auction ID:** #{auction['id']}"
                ),
                color=discord.Color.green(),
            )
            if auction["photo_url"]:
                embed.set_thumbnail(url=auction["photo_url"])
            with connect() as connection:
                connection.execute(
                    "UPDATE auctions SET is_repeat_win = 1 WHERE id = ?",
                    (auction["id"],),
                )
            # The winner already has this ticket, so the payment-method question
            # and the staff status buttons are not shown again here, even if
            # they never answered it the first time.
            ticket_message = await channel.send(
                content=(
                    f"{winner.mention}\n🎉 **Another auction win was added to this ticket.**\n"
                    "Staff, please assist the winner with this auction."
                ),
                embed=embed,
                view=PaymentView(
                    auction["id"],
                    include_payment_select=False,
                    include_status_buttons=False,
                    include_cashout=False,
                ),
            )
            with connect() as connection:
                connection.execute(
                    "UPDATE auctions SET winner_message_id = ? WHERE id = ?",
                    (ticket_message.id, auction["id"]),
                )
            return channel.id

    overwrites = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False),
        winner: discord.PermissionOverwrite(
            view_channel=True,
            send_messages=False,
            read_message_history=True,
            attach_files=False,
            mention_everyone=False,
        ),
    }
    for manager_role in get_manager_roles(guild):
        overwrites[manager_role] = discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            read_message_history=True,
            manage_messages=True,
        )
    for role_id in QUEUE_ROLE_IDS:
        staff_role = guild.get_role(role_id)
        if staff_role:
            overwrites[staff_role] = discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                read_message_history=True,
                manage_messages=True,
            )
    host = guild.get_member(auction["host_id"])
    if host:
        overwrites[host] = discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            read_message_history=True,
        )
    if guild.me:
        overwrites[guild.me] = discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            read_message_history=True,
            manage_channels=True,
        )

    waiting_category = await get_transaction_category(guild, WAITING_TO_PAY_CATEGORY)
    safe_username = re.sub(r"[^a-z0-9-]", "-", winner.name.lower()).strip("-") or str(winner.id)
    safe_username = safe_username[:75]
    channel = await guild.create_text_channel(
        name=f"auction-win-{safe_username}",
        category=waiting_category,
        overwrites=overwrites,
        topic=f"Private transaction for auction #{auction['id']}",
        reason=f"Winner transaction for auction #{auction['id']}",
    )
    with connect() as connection:
        connection.execute(
            "UPDATE auctions SET winner_channel_id = ? WHERE id = ?",
            (channel.id, auction["id"]),
        )
    embed = discord.Embed(
        title=f"Winner Transaction | Auction #{auction['id']}",
        description=(
            f"**Item:** {auction['item']}\n"
            f"**Winner:** {winner.mention}\n"
            f"**Final amount:** {format_amount(auction['final_bid'])}\n"
            f"**Auction ID:** #{auction['id']}"
        ),
[… MISSING SECTION FROM YOUR PASTE — re-paste this part and I will merge it in. It contained: the rest of create_winner_channel(), auction_embed(), AuctionView, ConfirmBidView, StaffControlsView, ticket-close views (TicketCloseConfirmView), auction_dashboard_embed/AuctionDashboardView, the auction expiry worker, schedule worker, refresh_auction_message(), require_server/require_staff, on_ready, and several slash commands …]
        "Are you sure you want to close this ticket?",
        view=TicketCloseConfirmView(interaction.user.id),
        ephemeral=True,
    )


@bot.tree.command(name="ticket_textperms", description="Unlock chat for the winner in this winner ticket.")
async def ticket_textperms(interaction: discord.Interaction):
    if not await require_staff(interaction):
        return
    if not isinstance(interaction.channel, discord.TextChannel):
        await interaction.response.send_message("This command must be used inside a winner ticket.", ephemeral=True)
        return
    with connect() as connection:
        winner_ticket = connection.execute(
            "SELECT winner_id FROM auctions WHERE winner_channel_id = ? AND winner_id IS NOT NULL LIMIT 1",
            (interaction.channel.id,),
        ).fetchone()
    if not winner_ticket:
        await interaction.response.send_message("This channel is not a winner ticket.", ephemeral=True)
        return
    try:
        await enable_winner_chat(
            interaction.guild,
            interaction.channel,
            winner_ticket["winner_id"],
            f"Chat unlocked by staff member {interaction.user}",
        )
    except (discord.Forbidden, discord.HTTPException) as error:
        await interaction.response.send_message(
            f"I could not unlock chat for the winner. Check my Manage Channels permission. ({error})",
            ephemeral=True,
        )
        return
    await interaction.response.send_message(
        f"Chat unlocked only for <@{winner_ticket['winner_id']}> and staff.",
        ephemeral=True,
    )


@bot.tree.command(name="ticket-winner-vouch", description="Ask a winner to vouch for the staff member who helped them.")
async def ticket_winner_vouch(interaction: discord.Interaction):
    if not await require_staff(interaction):
        return
    if not isinstance(interaction.channel, discord.TextChannel):
        await interaction.response.send_message("This command must be used inside a winner ticket.", ephemeral=True)
        return
    with connect() as connection:
        winner_ticket = connection.execute(
            "SELECT winner_id FROM auctions WHERE winner_channel_id = ? AND winner_id IS NOT NULL LIMIT 1",
            (interaction.channel.id,),
        ).fetchone()
    if not winner_ticket:
        await interaction.response.send_message("This channel is not a winner ticket.", ephemeral=True)
        return
    await send_winner_vouch_reminder(interaction.channel, winner_ticket["winner_id"])
    await interaction.response.send_message(
        "Vouch reminder sent in this winner ticket.",
        ephemeral=True,
    )


@bot.tree.command(name="queue_setup", description="Create or repair the private emoji-named auction queue.")
async def queue_setup(interaction: discord.Interaction):
    if not await require_queue_staff(interaction):
        return
    channel = await ensure_queue_channel(interaction.guild)
    await refresh_queue_message(channel)
    await interaction.response.send_message(f"Private queue ready: {channel.mention}", ephemeral=True)


@bot.tree.command(name="queue_add", description="Add an image-backed auction to the private staff queue.")
@app_commands.describe(item="Item or Brainrot", starting_bid="Starting amount", duration_minutes="Duration when started", photo="Required item image", channel="Channel where this auction will be posted", reserve_price="Optional reserve price", description="Optional details", auction_time="Examples: today 5pm, tomorrow 5:10pm, Friday 6pm, or 09/14 5pm")
async def queue_add(interaction: discord.Interaction, item: str, starting_bid: app_commands.Range[int, 1, MAX_BID], duration_minutes: app_commands.Range[int, 1, 10080], photo: discord.Attachment, channel: discord.TextChannel, reserve_price: app_commands.Range[int, 0, MAX_BID] = 0, description: str = "", auction_time: str | None = None):
    if not await require_queue_staff(interaction):
        return
    if not is_image_attachment(photo):
        await interaction.response.send_message("The queue photo must be an image file.", ephemeral=True)
        return
    if reserve_price and reserve_price < starting_bid:
        await interaction.response.send_message("The reserve price must be at least the starting amount.", ephemeral=True)
        return
    scheduled_at = parse_queue_datetime(auction_time)
    if auction_time and scheduled_at is None:
        await interaction.response.send_message("Use `today 5pm`, `tomorrow 5:10pm`, `Friday 6pm`, or `09/14 5pm`. The bot uses its local timezone and Discord displays it in each member's timezone.", ephemeral=True)
        return
    await ensure_queue_channel(interaction.guild)
    position = queue_next_position(interaction.guild.id)
    with connect() as connection:
        queue_id = connection.execute(
            """
            INSERT INTO queue_items(guild_id, channel_id, item, description, photo_url, starting_bid, reserve_price, duration_minutes, position, created_by, created_at, scheduled_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (interaction.guild.id, channel.id, item[:256], description[:1000], photo.url, starting_bid, reserve_price, duration_minutes, position, interaction.user.id, now(), scheduled_at),
        ).lastrowid
    queue_channel = await ensure_queue_channel(interaction.guild)
    await refresh_queue_message(queue_channel)
    time_label = format_queue_datetime(scheduled_at) if scheduled_at else "manual start"
    await interaction.response.send_message(f"Queued **#{queue_id} - {item}** for **{time_label}** in {channel.mention} at position {position}.", ephemeral=True)


@bot.tree.command(name="queue_list", description="Show the private staff auction queue.")
async def queue_list(interaction: discord.Interaction):
    if not await require_queue_staff(interaction):
        return
    channel = await ensure_queue_channel(interaction.guild)
    await refresh_queue_message(channel)
    await interaction.response.send_message(f"Queue refreshed in {channel.mention}.", ephemeral=True)


@bot.tree.command(name="queue_remove", description="Remove an item from the staff queue.")
@app_commands.describe(queue_id="Queue item ID")
async def queue_remove(interaction: discord.Interaction, queue_id: int):
    if not await require_queue_staff(interaction):
        return
    with connect() as connection:
        item = connection.execute("SELECT * FROM queue_items WHERE id = ? AND guild_id = ?", (queue_id, interaction.guild.id)).fetchone()
        if item:
            connection.execute("DELETE FROM queue_items WHERE id = ?", (queue_id,))
            connection.execute("UPDATE queue_items SET position = position - 1 WHERE guild_id = ? AND position > ?", (interaction.guild.id, item["position"]))
    if not item:
        await interaction.response.send_message("That queue item was not found.", ephemeral=True)
        return
    await refresh_queue_message(await ensure_queue_channel(interaction.guild))
    await interaction.response.send_message(f"Removed queue item **#{queue_id}**.", ephemeral=True)


@bot.tree.command(name="queue_move", description="Move a queued auction to another position.")
@app_commands.describe(queue_id="Queue item ID", position="New position, starting at 1")
async def queue_move(interaction: discord.Interaction, queue_id: int, position: app_commands.Range[int, 1, 1000]):
    if not await require_queue_staff(interaction):
        return
    with connect() as connection:
        item = connection.execute("SELECT * FROM queue_items WHERE id = ? AND guild_id = ?", (queue_id, interaction.guild.id)).fetchone()
        count = connection.execute("SELECT COUNT(*) AS count FROM queue_items WHERE guild_id = ?", (interaction.guild.id,)).fetchone()["count"]
        if not item:
            await interaction.response.send_message("That queue item was not found.", ephemeral=True)
            return
        target = min(position, count)
        old = item["position"]
        if target < old:
            connection.execute("UPDATE queue_items SET position = position + 1 WHERE guild_id = ? AND position >= ? AND position < ?", (interaction.guild.id, target, old))
        elif target > old:
            connection.execute("UPDATE queue_items SET position = position - 1 WHERE guild_id = ? AND position > ? AND position <= ?", (interaction.guild.id, old, target))
        connection.execute("UPDATE queue_items SET position = ? WHERE id = ?", (target, queue_id))
    await refresh_queue_message(await ensure_queue_channel(interaction.guild))
    await interaction.response.send_message(f"Moved queue item **#{queue_id}** to position {target}.", ephemeral=True)


@bot.tree.command(name="queue_edit", description="Edit a queued auction before it goes live.")
@app_commands.describe(queue_id="Queue item ID", item="New item name", starting_bid="New starting amount", duration_minutes="New duration", photo="New required item image", channel="New destination channel", reserve_price="New optional reserve", description="New description", auction_time="Examples: today 5pm, tomorrow 5:10pm, Friday 6pm, or 09/14 5pm")
async def queue_edit(interaction: discord.Interaction, queue_id: int, item: str | None = None, starting_bid: app_commands.Range[int, 1, MAX_BID] | None = None, duration_minutes: app_commands.Range[int, 1, 10080] | None = None, photo: discord.Attachment | None = None, channel: discord.TextChannel | None = None, reserve_price: app_commands.Range[int, 0, MAX_BID] | None = None, description: str | None = None, auction_time: str | None = None):
    if not await require_queue_staff(interaction):
        return
    with connect() as connection:
        current = connection.execute("SELECT * FROM queue_items WHERE id = ? AND guild_id = ?", (queue_id, interaction.guild.id)).fetchone()
        if not current:
            await interaction.response.send_message("That queue item was not found.", ephemeral=True)
            return
        values = {
            "channel_id": channel.id if channel else current["channel_id"],
            "item": item if item is not None else current["item"],
            "starting_bid": starting_bid if starting_bid is not None else current["starting_bid"],
            "duration_minutes": duration_minutes if duration_minutes is not None else current["duration_minutes"],
            "photo_url": photo.url if photo else current["photo_url"],
            "reserve_price": reserve_price if reserve_price is not None else current["reserve_price"],
            "description": description if description is not None else current["description"],
        }
        scheduled_at = parse_queue_datetime(auction_time) if auction_time is not None else current["scheduled_at"]
        if auction_time is not None and scheduled_at is None:
            await interaction.response.send_message("Use `today 5pm`, `tomorrow 5:10pm`, `Friday 6pm`, or `09/14 5pm`. The bot uses its local timezone and Discord displays it in each member's timezone.", ephemeral=True)
            return
        if photo and not is_image_attachment(photo):
            await interaction.response.send_message("The queue photo must be an image file.", ephemeral=True)
            return
        new_starting_bid = values["starting_bid"]
        new_reserve = values["reserve_price"]
        if new_reserve and new_reserve < new_starting_bid:
            await interaction.response.send_message("The reserve price must be at least the starting amount.", ephemeral=True)
            return
        # Positional placeholders are filled from named values rather than
        # dict ordering, so adding or reordering a key cannot silently write
        # the wrong column.
        connection.execute(
            """
            UPDATE queue_items
            SET channel_id = :channel_id, item = :item, starting_bid = :starting_bid,
                duration_minutes = :duration_minutes, photo_url = :photo_url,
                reserve_price = :reserve_price, description = :description,
                scheduled_at = :scheduled_at
            WHERE id = :id
            """,
            {
                "channel_id": values["channel_id"],
                "item": values["item"],
                "starting_bid": new_starting_bid,
                "duration_minutes": values["duration_minutes"],
                "photo_url": values["photo_url"],
                "reserve_price": new_reserve,
                "description": values["description"],
                "scheduled_at": scheduled_at,
                "id": queue_id,
            },
        )
    await refresh_queue_message(await ensure_queue_channel(interaction.guild))
    await interaction.response.send_message(f"Updated queue item **#{queue_id}**.", ephemeral=True)


@bot.tree.command(name="queue_start", description="Start the first or selected queued auction publicly.")
@app_commands.describe(queue_id="Queue item ID")
async def queue_start(interaction: discord.Interaction, queue_id: int):
    if not await require_queue_staff(interaction):
        return
    with connect() as connection:
        item = connection.execute("SELECT * FROM queue_items WHERE id = ? AND guild_id = ?", (queue_id, interaction.guild.id)).fetchone()
    if not item:
        await interaction.response.send_message("That queue item was not found.", ephemeral=True)
        return
    auction_id = await start_queued_item(queue_id)
    if auction_id is None:
        await interaction.response.send_message(
            "I could not post that auction. The queue item was kept; check the destination channel and my permissions.",
            ephemeral=True,
        )
        return
    auction = fetch_auction(auction_id)
    channel = bot.get_channel(auction["channel_id"]) if auction else None
    if channel is None:
        await interaction.response.send_message(
            "The auction was started, but its channel is no longer available to me.",
            ephemeral=True,
        )
        return
    await refresh_queue_message(await ensure_queue_channel(interaction.guild))
    await interaction.response.send_message(f"Started auction **#{auction_id}** in {channel.mention}.", ephemeral=True)


@bot.tree.command(
    name="auction_win_paymentticket",
    description="Enable or disable a payment method for winner tickets.",
)
@app_commands.describe(method="Payment method to change", enabled="Whether winners may use this method")
@app_commands.choices(
    method=[app_commands.Choice(name=method, value=method) for method in PAYMENT_METHODS]
)
async def auction_win_paymentticket(
    interaction: discord.Interaction,
    method: app_commands.Choice[str],
    enabled: bool,
):
    if not await require_staff(interaction):
        return
    set_payment_method_enabled(interaction.guild.id, method.value, enabled)
    state = "enabled" if enabled else "disabled"
    await interaction.response.send_message(
        f"**{method.value}** is now {state} for winner payment tickets.",
        ephemeral=True,
    )


@bot.tree.command(name="auction_create", description="Create and immediately start an auction.")
@app_commands.describe(channel="Which channel should receive this auction?", item="Item or Brainrot", starting_bid="Starting amount", duration_minutes="Duration in minutes", photo="Required item photo", reserve_price="Optional reserve price, or 0 for no reserve", description="Optional item details")
async def auction_create(interaction: discord.Interaction, channel: discord.TextChannel, item: str, starting_bid: app_commands.Range[int, 1, MAX_BID], duration_minutes: app_commands.Range[int, 1, 10080], photo: discord.Attachment, reserve_price: app_commands.Range[int, 0, MAX_BID] = 0, description: str = ""):
    if not await require_staff(interaction):
        return
    if not is_image_attachment(photo):
        await interaction.response.send_message("The auction photo must be an image file.", ephemeral=True)
        return
    if reserve_price and reserve_price < starting_bid:
        await interaction.response.send_message("The reserve price must be at least the starting amount.", ephemeral=True)
        return
    auction_id = create_auction_record(
        interaction.guild.id,
        channel.id,
        interaction.user.id,
        item,
        description,
        starting_bid,
        duration_minutes,
        reserve_price,
        photo.url,
    )
    auction = fetch_auction(auction_id)
    try:
        message = await channel.send(
            content=f"<@&{AUCTION_ALERT_ROLE_ID}>",
            embed=auction_embed(auction),
            view=AuctionView(auction_id),
            allowed_mentions=discord.AllowedMentions(roles=True),
        )
    except (discord.Forbidden, discord.HTTPException):
        with connect() as connection:
            connection.execute("UPDATE auctions SET status = 'cancelled', ended_at = ?, closed_by = ? WHERE id = ?", (now(), interaction.user.id, auction_id))
        await interaction.response.send_message(
            f"I could not post in {channel.mention}. Check my **View Channel** and **Send Messages** permissions there.",
            ephemeral=True,
        )
        return
    set_message_id(auction_id, message.id)
    await interaction.response.send_message(
        f"✅ Auction **#{auction_id}** started in {channel.mention}.",
        ephemeral=True,
    )
    await send_log(interaction.guild.id, f"Auction #{auction_id} started by {interaction.user.mention}.")


@bot.tree.command(name="auction_test", description="Post a test auction with no image.")
@app_commands.describe(
    channel="Which channel should receive this test auction?",
    duration_minutes="Duration in minutes",
    starting_bid="Starting amount",
    item="Item name",
    reserve_price="Optional reserve price, or 0 for no reserve",
)
async def auction_test(
    interaction: discord.Interaction,
    channel: discord.TextChannel,
    duration_minutes: app_commands.Range[int, 1, 10080] = 5,
    starting_bid: app_commands.Range[int, 1, MAX_BID] = 1,
    item: str = "Test Auction",
    reserve_price: app_commands.Range[int, 0, MAX_BID] = 0,
):
    if not await require_staff(interaction):
        return
    if reserve_price and reserve_price < starting_bid:
        await interaction.response.send_message("The reserve price must be at least the starting amount.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    # No image on purpose: this is a quick staff test of the auction flow.
    auction_id = create_auction_record(
        interaction.guild.id,
        channel.id,
        interaction.user.id,
        item,
        "Test auction posted by staff. No item image is attached.",
        starting_bid,
        duration_minutes,
        reserve_price,
        None,
    )
    auction = fetch_auction(auction_id)
    try:
        message = await channel.send(
            content=f"<@&{AUCTION_ALERT_ROLE_ID}>",
            embed=auction_embed(auction),
            view=AuctionView(auction_id),
            allowed_mentions=discord.AllowedMentions(roles=True),
        )
    except (discord.Forbidden, discord.HTTPException):
        update_auction_status(auction_id, "cancelled", interaction.user.id)
        await interaction.followup.send(
            f"I could not post in {channel.mention}. Check my **View Channel** and **Send Messages** permissions there.",
            ephemeral=True,
        )
        return
    set_message_id(auction_id, message.id)
    await interaction.followup.send(
        f"Test auction **{auction_reference(auction)}** started in {channel.mention}.",
        ephemeral=True,
    )
    await send_log(interaction.guild.id, f"Test auction #{auction_id} started by {interaction.user.mention} in {channel.mention}.")


@bot.tree.command(name="bid", description="Place a bid without using the auction button.")
@app_commands.describe(auction_id="Copy/paste the long Auction ID", amount="Your bid amount")
async def bid(interaction: discord.Interaction, auction_id: str, amount: app_commands.Range[int, 1, MAX_BID]):
    if not await require_server(interaction):
        return
    auction = fetch_auction_by_reference(auction_id)
    if not auction or auction["guild_id"] != interaction.guild.id:
        await interaction.response.send_message("That auction was not found.", ephemeral=True)
        return
    await interaction.response.send_message(
        bid_confirmation_message(auction["current_bid"], amount, amount),
        view=ConfirmBidView(auction["id"], interaction.user.id, amount),
        ephemeral=True,
    )


@bot.tree.command(name="bot_status", description="Show the bot's health, configuration, and any action needed.")
async def bot_status(interaction: discord.Interaction):
    if not await require_staff(interaction):
        return

    guild = interaction.guild
    warnings: list[str] = []
    errors: list[str] = []
    database_status = "Healthy"
    active_auctions = overdue_auctions = missing_messages = due_queue_items = 0
    try:
        with connect() as connection:
            integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
            if integrity.lower() != "ok":
                errors.append(f"Database integrity check returned: {integrity[:120]}")
                database_status = "Integrity issue detected"
            active_auctions = connection.execute(
                "SELECT COUNT(*) FROM auctions WHERE guild_id = ? AND status IN ('active', 'paused')",
                (guild.id,),
            ).fetchone()[0]
            overdue_auctions = connection.execute(
                "SELECT COUNT(*) FROM auctions WHERE guild_id = ? AND status = 'active' AND ends_at <= ?",
                (guild.id, now()),
            ).fetchone()[0]
            missing_messages = connection.execute(
                "SELECT COUNT(*) FROM auctions WHERE guild_id = ? AND status = 'active' AND message_id IS NULL",
                (guild.id,),
            ).fetchone()[0]
            due_queue_items = connection.execute(
                "SELECT COUNT(*) FROM queue_items WHERE guild_id = ? AND scheduled_at IS NOT NULL AND scheduled_at <= ?",
                (guild.id, now()),
            ).fetchone()[0]
    except sqlite3.Error as error:
        database_status = "Unavailable"
        errors.append(f"Database error: {type(error).__name__}: {error}")

    config = get_config(guild.id)
    if config is None:
        warnings.append("Auction setup has not been completed; no default auction channel is configured.")
    else:
        for label, channel_id in (
            ("Log channel", log_channel_id(guild.id)),
            ("Default auction channel", config["auction_channel_id"]),
        ):
            warning = channel_health(guild, channel_id, label)
            if warning:
                warnings.append(warning)
    if not get_manager_roles(guild):
        warnings.append(
            "No auction manager role could be found, so nobody is pinged in private tickets. "
            "Run `/auction_setup` with a manager role to fix this."
        )

    for label, channel_id in (
        ("Ticket panel channel", TICKET_PANEL_CHANNEL_ID),
        ("Transcript channel", TRANSCRIPT_CHANNEL_ID),
    ):
        if channel_id is None:
            # The ticket panel is now created automatically, so its absence is
            # not a problem worth flagging. The transcript channel has no
            # automatic fallback, so it still is.
            if label != "Transcript channel":
                continue
            warnings.append(
                "Transcript channel is not configured in the bot's environment, so ticket "
                "transcripts cannot be saved."
            )
            continue
        warning = channel_health(guild, channel_id, label)
        if warning:
            warnings.append(warning)

    if not intents.message_content:
        errors.append("Message Content Intent is disabled in the bot code; ticket transcripts will be incomplete.")
    if not sync_done:
        warnings.append("Slash-command synchronization has not finished yet.")
    if overdue_auctions:
        warnings.append(f"{overdue_auctions} active auction(s) are past their end time and awaiting the expiry worker.")
    if missing_messages:
        warnings.append(f"{missing_messages} active auction(s) have no public message.")
    if due_queue_items:
        warnings.append(f"{due_queue_items} scheduled queue item(s) are overdue and awaiting processing.")

    auction_worker_status = worker_health(auction_worker)
    schedule_worker_status = worker_health(schedule_worker)
    if auction_worker_status != "Running":
        errors.append(f"Auction expiry worker: {auction_worker_status}.")
    if schedule_worker_status != "Running":
        errors.append(f"Schedule worker: {schedule_worker_status}.")

    if errors:
        title, color, summary = "Bot Status — Action Required", discord.Color.red(), "The bot is online, but one or more critical checks need attention."
    elif warnings:
        title, color, summary = "Bot Status — Attention Recommended", discord.Color.orange(), "The core bot is healthy, with configuration or queue items to review."
    else:
        title, color, summary = "Bot Status — Healthy", discord.Color.green(), "All automated checks passed. The bot and its auction services are operating normally."

    uptime_seconds = int((datetime.now(timezone.utc) - BOT_STARTED_AT).total_seconds())
    embed = discord.Embed(title=title, description=summary, color=color, timestamp=datetime.now(timezone.utc))
    embed.add_field(
        name="Connection",
        value=(
            f"Bot: {bot.user.mention if bot.user else 'connecting'}\n"
            f"Gateway latency: **{bot.latency * 1000:.0f} ms**\n"
            f"Uptime: <t:{int(BOT_STARTED_AT.timestamp())}:R> ({uptime_seconds // 3600}h {(uptime_seconds % 3600) // 60}m)"
        ),
        inline=False,
    )
    embed.add_field(
        name="Core services",
        value=(
            f"Database: **{database_status}**\n"
            f"Auction expiry worker: **{auction_worker_status}**\n"
            f"Schedule / queue worker: **{schedule_worker_status}**\n"
            # Reported from the live intent object, not hardcoded: transcripts
            # silently lose message bodies when this is actually off.
            f"Message Content Intent: **{'Enabled' if intents.message_content else 'Disabled'}**"
        ),
        inline=False,
    )
    embed.add_field(
        name="Auction activity",
        value=(
            f"Active or paused auctions: **{active_auctions}**\n"
            f"Overdue active auctions: **{overdue_auctions}**\n"
            f"Active auctions without a message: **{missing_messages}**\n"
            f"Overdue scheduled queue items: **{due_queue_items}**"
        ),
        inline=False,
    )
    notes = errors + warnings
    # Discord caps a field at 1,024 characters, so the list is trimmed - but the
    # remainder is counted, otherwise staff would see a short list and assume
    # everything else was fine.
    shown = notes[:8]
    if len(notes) > len(shown):
        shown = shown + [f"...and {len(notes) - len(shown)} more issue(s) not shown."]
    embed.add_field(
        name="Issues found" if notes else "Checks completed",
        value="\n".join(f"• {note}" for note in shown) if notes else "No problems detected.",
        inline=False,
    )
    embed.set_footer(text=f"Server: {guild.name} | Run this command again after resolving an issue.")
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(
    name="auction_setup",
    description="Configure the manager role, log channel, and default auction channel. Saved permanently.",
)
@app_commands.describe(
    manager_role="Role allowed to manage auctions (optional - queue staff are used by default)",
    log_channel="Channel for auction logs (optional - one is created automatically)",
    auction_channel="Default scheduled-auction channel",
)
async def auction_setup(
    interaction: discord.Interaction,
    manager_role: discord.Role | None = None,
    log_channel: discord.TextChannel | None = None,
    auction_channel: discord.TextChannel | None = None,
):
    if not await require_server(interaction):
        return
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("Administrator permission is required for setup.", ephemeral=True)
        return
    guild = interaction.guild
    if manager_role is not None and manager_role.id in FORBIDDEN_MANAGER_ROLE_IDS:
        await interaction.response.send_message(
            f"{manager_role.mention} is the auction **alert** role, not the manager role. "
            "Using it would ping every bidder whenever a private ticket is opened. "
            "Please pick a manager-only role instead.",
            ephemeral=True,
        )
        return
    # Every value is stored in guild_config, so this only has to be run once.
    # It is never reset by a restart, and omitted options keep their current
    # value rather than being blanked.
    update_config(
        guild.id,
        manager_role.id if manager_role else None,
        log_channel.id if log_channel else None,
        auction_channel.id if auction_channel else None,
    )
    resolved_managers = get_manager_roles(guild)
    resolved_log = get_log_channel(guild)
    lines = ["✅ Auction settings saved. These persist across restarts.", ""]
    if resolved_managers:
        lines.append(f"**Manager role(s):** {' '.join(r.mention for r in resolved_managers)}")
    elif manager_role is not None:
        lines.append(
            f"⚠️ The manager role {manager_role.mention} does not exist in this server, "
            "so the queue staff roles will be pinged instead."
        )
    else:
        lines.append(
            "ℹ️ No manager role was given, so the queue staff roles will be pinged instead."
        )
    if resolved_log is not None:
        lines.append(f"**Log channel:** {resolved_log.mention}")
    else:
        lines.append("**Log channel:** will be created automatically on the next log entry.")
    if auction_channel:
        lines.append(f"**Default auction channel:** {auction_channel.mention}")
    await interaction.response.send_message("\n".join(lines), ephemeral=True)


@bot.tree.command(name="auction_schedule", description="Create a recurring weekly auction schedule in UTC.")
@app_commands.describe(day="Day of week", time_utc="24-hour UTC time, for example 18:30", item="Item or Brainrot", starting_bid="Starting bid", duration_minutes="Duration in minutes", photo="Required item photo", description="Optional item details")
@app_commands.choices(day=[app_commands.Choice(name=name.title(), value=name) for name in ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")])
async def auction_schedule(interaction: discord.Interaction, day: app_commands.Choice[str], time_utc: str, item: str, starting_bid: app_commands.Range[int, 1, MAX_BID], duration_minutes: app_commands.Range[int, 1, 10080], photo: discord.Attachment, description: str = ""):
    if not await require_staff(interaction):
        return
    if not is_image_attachment(photo):
        await interaction.response.send_message("The schedule photo must be an image file.", ephemeral=True)
        return
    day_number = parse_day(day.value)
    if not TIME_PATTERN.match(time_utc):
        await interaction.response.send_message("Use a 24-hour UTC time such as `18:30`.", ephemeral=True)
        return
    config = get_config(interaction.guild.id)
    channel_id = config["auction_channel_id"] if config and config["auction_channel_id"] else interaction.channel.id
    with connect() as connection:
        cursor = connection.execute(
            """
            INSERT INTO schedules(guild_id, channel_id, day_of_week, time_utc, item, description, starting_bid, duration_minutes, created_by, photo_url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (interaction.guild.id, channel_id, day_number, time_utc, item[:256], description[:1000], starting_bid, duration_minutes, interaction.user.id, photo.url),
        )
        schedule_id = cursor.lastrowid
    await interaction.response.send_message(f"Weekly schedule **#{schedule_id}** created for **{day.value.title()} {time_utc} UTC**.", ephemeral=True)


@bot.tree.command(name="schedule_announcement", description="Schedule a recurring weekly server announcement in UTC.")
@app_commands.describe(
    day="Day of week",
    time_utc="24-hour UTC time, for example 18:30",
    announcement="Announcement text to post",
    role="Which role should be pinged?",
    channel="Where the announcement should be posted",
)
@app_commands.choices(day=[app_commands.Choice(name=name.title(), value=name) for name in ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")])
async def schedule_announcement(
    interaction: discord.Interaction,
    day: app_commands.Choice[str],
    time_utc: str,
    announcement: str,
    role: discord.Role | None = None,
    channel: discord.TextChannel | None = None,
):
    if not await require_staff(interaction):
        return
    if not TIME_PATTERN.match(time_utc):
        await interaction.response.send_message("Use a 24-hour UTC time such as `18:30`.", ephemeral=True)
        return
    target_channel = channel or interaction.channel
    with connect() as connection:
        cursor = connection.execute(
            """
            INSERT INTO scheduled_announcements(
                guild_id, channel_id, day_of_week, time_utc, announcement, role_id, created_by
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                interaction.guild.id,
                target_channel.id,
                parse_day(day.value),
                time_utc,
                announcement[:1800],
                role.id if role else None,
                interaction.user.id,
            ),
        )
        announcement_id = cursor.lastrowid
    role_text = role.mention if role else "no role"
    await interaction.response.send_message(
        f"Weekly announcement **#{announcement_id}** scheduled for **{day.value.title()} {time_utc} UTC** in {target_channel.mention} ({role_text}).",
        ephemeral=True,
    )


@bot.tree.command(name="server_announcement", description="Post an announcement and optionally ping a server role.")
@app_commands.describe(
    announcement="Write the announcement you want to post",
    role="Which role do you want to ping?",
    channel="Where the announcement should be posted",
)
async def server_announcement(
    interaction: discord.Interaction,
    announcement: str,
    role: discord.Role | None = None,
    channel: discord.TextChannel | None = None,
):
    if not await require_staff(interaction):
        return
    target_channel = channel or interaction.channel
    role_mention = f"{role.mention} " if role else ""
    await target_channel.send(
        f"{role_mention}{announcement}",
        allowed_mentions=discord.AllowedMentions(users=False, roles=True, everyone=False),
    )
    await interaction.response.send_message(
        f"Announcement posted in {target_channel.mention}{f' with {role.mention} ping' if role else ''}.",
        ephemeral=True,
    )


@bot.tree.command(name="auction_history", description="View completed auctions in this server.")
@app_commands.describe(limit="Number of records to show")
async def auction_history(interaction: discord.Interaction, limit: app_commands.Range[int, 1, 10] = 10):
    if not await require_server(interaction):
        return
    with connect() as connection:
        rows = connection.execute(
            "SELECT * FROM auctions WHERE guild_id = ? AND status IN ('ended', 'cancelled') ORDER BY ended_at DESC LIMIT ?",
            (interaction.guild.id, limit),
        ).fetchall()
    if not rows:
        await interaction.response.send_message("No completed auctions yet.", ephemeral=True)
        return
    embed = discord.Embed(title="Auction History", color=discord.Color.gold())
    for row in rows:
        result = f"Winner: <@{row['winner_id']}> for **{format_amount(row['final_bid'])}**" if row["winner_id"] else row["status"].title()
        embed.add_field(name=f"#{row['id']} - {row['item']}", value=result, inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="auction_dashboard", description="Open the auction dashboard and winner history.")
async def auction_dashboard(interaction: discord.Interaction):
    if not await require_staff(interaction):
        return
    await interaction.response.send_message(
        embed=auction_dashboard_embed(interaction.guild.id),
        view=AuctionDashboardView(interaction.user.id),
        ephemeral=True,
    )


@bot.tree.command(
    name="auction_switch_winner",
    description="Give an ended auction to a different winner when the first winner cannot pay.",
)
@app_commands.describe(
    reason="Why the winner is being changed",
    new_winner="Replacement winner, or leave empty to use the second-highest bidder",
)
async def auction_switch_winner(
    interaction: discord.Interaction,
    reason: str,
    new_winner: discord.Member | None = None,
):
    if not await require_staff(interaction):
        return
    if not isinstance(interaction.channel, discord.TextChannel):
        await interaction.response.send_message(
            "This command must be used inside the winner ticket.",
            ephemeral=True,
        )
        return

    # Resolve the auction from the ticket this command was used in, so it can
    # never be pointed at an unrelated auction.
    channel_id = interaction.channel.id
    with connect() as connection:
        auction = connection.execute(
            "SELECT * FROM auctions WHERE winner_channel_id = ? ORDER BY id DESC LIMIT 1",
            (channel_id,),
        ).fetchone()
        seller_ticket = connection.execute(
            "SELECT * FROM seller_tickets WHERE channel_id = ?",
            (channel_id,),
        ).fetchone()

    if auction is None:
        if seller_ticket is not None:
            await interaction.response.send_message(
                "This is a seller request ticket, not a winner ticket. Use the auction commands from the auction's own channel.",
                ephemeral=True,
            )
        else:
            await interaction.response.send_message(
                "This channel is not a winner ticket, so I will not change any winner.",
                ephemeral=True,
            )
        return
    if auction["guild_id"] != interaction.guild.id:
        await interaction.response.send_message("Auction not found.", ephemeral=True)
        return
    if auction["status"] != "ended":
        await interaction.response.send_message(
            "Only an ended auction has a winner to replace.",
            ephemeral=True,
        )
        return
    if not auction["winner_id"]:
        await interaction.response.send_message(
            "This auction has no winner, so there is nobody to replace.",
            ephemeral=True,
        )
        return
    if auction["transaction_status"] in ("paid_not_claimed", "paid_and_claimed"):
        await interaction.response.send_message(
            "This auction is already marked paid. Close the transaction or resolve it before switching winners.",
            ephemeral=True,
        )
        return

    previous_winner_id = auction["winner_id"]
    previous_channel_id = auction["winner_channel_id"]

    # Work out the replacement: an explicit member, or the second-highest bidder.
    if new_winner is not None:
        if new_winner.id == previous_winner_id:
            await interaction.response.send_message(
                "That member is already the winner of this auction.",
                ephemeral=True,
            )
            return
        replacement_id = new_winner.id
        replacement_amount = auction["final_bid"]
    else:
        second = fetch_second_place(auction["id"], previous_winner_id)
        if second is None:
            await interaction.response.send_message(
                "There is no second bidder to promote. Name a replacement winner explicitly.",
                ephemeral=True,
            )
            return
        replacement_id = second["bidder_id"]
        replacement_amount = second["amount"]

    await interaction.response.defer(ephemeral=True)
    guild = interaction.guild
    replacement = guild.get_member(replacement_id)
    if replacement is None:
        try:
            replacement = await guild.fetch_member(replacement_id)
        except discord.NotFound:
            await interaction.followup.send(
                "That member has left the server, so they cannot be the winner.",
                ephemeral=True,
            )
            return
        except discord.HTTPException as error:
            await interaction.followup.send(f"Could not look up that member: {error}", ephemeral=True)
            return

    switch_winner(auction["id"], replacement_id, replacement_amount)
    log_action(
        guild.id,
        interaction.user.id,
        "winner_switched",
        auction["id"],
        f"{previous_winner_id} -> {replacement_id}: {reason}",
    )

    # Tell the previous winner and revoke their view, but keep the channel as
    # the live winner ticket so the vouch flow keeps working.
    previous_channel = guild.get_channel(previous_channel_id) if previous_channel_id else None
    if previous_channel is not None:
        try:
            await previous_channel.send(
                f"⚠️ **The winner of this auction has changed.**\n"
                f"{interaction.user.mention} reassigned this auction to {replacement.mention}.\n"
                f"Reason: {reason}"
            )
        except discord.HTTPException:
            pass

    # The ticket now belongs to the new winner: rename it, hand it the winner
    # permission set, and return it to the waiting-to-pay category.
    if previous_channel is not None:
        previous_winner = guild.get_member(previous_winner_id)
        revoked_target = previous_winner if previous_winner else discord.Object(id=previous_winner_id)
        try:
            await previous_channel.set_permissions(
                revoked_target,
                view_channel=False,
                reason=f"Winner replaced by {interaction.user}",
            )
        except (discord.Forbidden, discord.HTTPException):
            pass
        try:
            await previous_channel.set_permissions(
                replacement,
                view_channel=True,
                read_message_history=True,
                send_messages=False,
                attach_files=False,
                mention_everyone=False,
                reason=f"New winner assigned by {interaction.user}",
            )
        except (discord.Forbidden, discord.HTTPException):
            pass
        try:
            safe_username = re.sub(r"[^a-z0-9-]", "-", replacement.name.lower()).strip("-") or str(replacement.id)
            waiting_category = await get_transaction_category(guild, WAITING_TO_PAY_CATEGORY)
            await previous_channel.edit(
                name=f"auction-win-{safe_username[:75]}",
                category=waiting_category,
                topic=f"Private transaction for auction #{auction['id']}",
                reason=f"Winner replaced by {interaction.user}",
            )
        except (discord.Forbidden, discord.HTTPException):
            pass
        await post_winner_switch_notice(
            previous_channel,
            fetch_auction(auction["id"]),
            replacement,
            replacement_amount,
            interaction.user,
        )

    await refresh_auction_message(auction["id"])
    await interaction.followup.send(
        f"Winner switched to {replacement.mention} at **${format_amount(replacement_amount)}**.\n"
        f"Reason: {reason}",
        ephemeral=True,
    )
    await send_log(
        guild.id,
        (
            f"Winner for auction **{auction_reference(auction)}** changed from <@{previous_winner_id}> "
            f"to {replacement.mention} by {interaction.user.mention}.\n"
            f"**Reason:** {reason}\n"
            f"The existing winner ticket was kept and reassigned to the new winner."
        ),
        title="Winner Changed",
        color=discord.Color.orange(),
    )


@bot.tree.command(name="auction_remove_bid", description="Invalidate a bid and recalculate the winner.")
@app_commands.describe(auction_id="Copy/paste the long Auction ID", bid_id="Bid record ID", reason="Reason for removal")
async def auction_remove_bid(interaction: discord.Interaction, auction_id: str, bid_id: int, reason: str):
    if not await require_server(interaction):
        return
    if not is_moderator(interaction.user):
        await interaction.response.send_message("Moderator or auction manager permission is required.", ephemeral=True)
        return
    auction = fetch_auction_by_reference(auction_id)
    if not auction or auction["guild_id"] != interaction.guild.id:
        await interaction.response.send_message("Auction not found.", ephemeral=True)
        return
    remove_bid_record(auction["id"], bid_id, interaction.user.id, reason)
    await refresh_auction_message(auction["id"])
    await send_log(interaction.guild.id, f"Bid **#{bid_id}** removed from auction **{auction_reference(auction)}** by {interaction.user.mention}: {reason}")
    await interaction.response.send_message("Bid removed and auction totals recalculated.", ephemeral=True)


@bot.tree.command(name="auction_schedule_list", description="List recurring auction schedules.")
async def auction_schedule_list(interaction: discord.Interaction):
    if not await require_staff(interaction):
        return
    with connect() as connection:
        schedules = connection.execute(
            "SELECT * FROM schedules WHERE guild_id = ? AND enabled = 1 ORDER BY day_of_week, time_utc",
            (interaction.guild.id,),
        ).fetchall()
    if not schedules:
        await interaction.response.send_message("No recurring schedules are configured.", ephemeral=True)
        return
    names = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
    embed = discord.Embed(title="Recurring Auction Schedules", color=discord.Color.blurple())
    for schedule in schedules:
        embed.add_field(
            name=f"#{schedule['id']} | {names[schedule['day_of_week']]} {schedule['time_utc']} UTC",
            value=f"{schedule['item']} | Starting bid {format_amount(schedule['starting_bid'])} | {schedule['duration_minutes']} minutes",
            inline=False,
        )
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="auction_schedule_remove", description="Disable a recurring auction schedule.")
@app_commands.describe(schedule_id="Schedule number")
async def auction_schedule_remove(interaction: discord.Interaction, schedule_id: int):
    if not await require_staff(interaction):
        return
    with connect() as connection:
        result = connection.execute(
            "UPDATE schedules SET enabled = 0 WHERE id = ? AND guild_id = ? AND enabled = 1",
            (schedule_id, interaction.guild.id),
        )
    if result.rowcount == 0:
        await interaction.response.send_message("That active schedule was not found.", ephemeral=True)
        return
    await interaction.response.send_message(f"Schedule #{schedule_id} disabled.", ephemeral=True)


@bot.tree.command(name="auction_staff", description="Open staff controls for an auction.")
@app_commands.describe(auction_id="Copy/paste the long Auction ID")
async def auction_staff(interaction: discord.Interaction, auction_id: str):
    if not await require_staff(interaction):
        return
    auction = fetch_auction_by_reference(auction_id)
    if not auction or auction["guild_id"] != interaction.guild.id:
        await interaction.response.send_message("Auction not found.", ephemeral=True)
        return
    await interaction.response.send_message("Staff controls:", view=StaffControlsView(auction["id"]), ephemeral=True)


@bot.event
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    print(f"Command error: {error!r}")
    if not interaction.response.is_done():
        await interaction.response.send_message("That action could not be completed. Check the command values and try again.", ephemeral=True)


initialize_database()

TOKEN = os.getenv("DISCORD_BOT_TOKEN") or os.getenv("BOT_TOKEN")
GUILD_ID = os.getenv("DISCORD_GUILD_ID") or "1459730457380786371"
if not TOKEN:
    raise RuntimeError("Set DISCORD_BOT_TOKEN in PowerShell before starting the bot.")

bot.run(TOKEN)