import asyncio
import io
import os
import re
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import discord
from discord import app_commands
from discord.ext import commands, tasks


DATABASE_FILE = Path(__file__).with_name("auctions.sqlite3")
MAX_BID = 2_147_483_647
BID_COOLDOWN_SECONDS = 2.0
ANTI_SNIPE_SECONDS = 15
ANTI_SNIPE_WINDOW_SECONDS = 60
AUCTION_EXPIRY_CHECK_SECONDS = 10
TIME_PATTERN = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d$")
QUEUE_ROLE_IDS = {
    1484957759349854260,
    1459747965509046386,
    1484615687048659044,
}
AUCTION_ALERT_ROLE_ID = 1485265698556084225
QUEUE_CATEGORY_NAME = "📋・auction-queue"
QUEUE_CHANNEL_NAME = "⏳・waiting-queue"
WAITING_TO_PAY_CATEGORY = "⏳・waiting-to-pay"
PAID_NOT_CLAIMED_CATEGORY = "💵・paid-not-claimed"
PAID_AND_CLAIMED_CATEGORY = "✅・paid-and-claimed"
SELLER_TICKET_CATEGORY = "📨・auction-requests"
TICKET_PANEL_CHANNEL_ID = 1486110550915158026
TRANSCRIPT_CHANNEL_ID = 1486111228811280618

intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)

last_bid_times: dict[tuple[int, int], float] = {}
bid_lock = asyncio.Lock()
sync_done = False


SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS guild_config (
    guild_id INTEGER PRIMARY KEY,
    manager_role_id INTEGER,
    log_channel_id INTEGER,
    auction_channel_id INTEGER,
    ticket_panel_message_id INTEGER,
    seller_tickets_enabled INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS auctions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
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
    created_at REAL NOT NULL,
    ended_at REAL,
    winner_id INTEGER,
    final_bid INTEGER,
    closed_by INTEGER,
    winner_channel_id INTEGER,
    payment_method TEXT,
    transaction_status TEXT NOT NULL DEFAULT 'waiting_to_pay'
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
        if "seller_tickets_enabled" not in config_columns:
            connection.execute("ALTER TABLE guild_config ADD COLUMN seller_tickets_enabled INTEGER NOT NULL DEFAULT 1")
        auction_columns = {
            row["name"] for row in connection.execute("PRAGMA table_info(auctions)")
        }
        migrations = {
            "photo_url": "ALTER TABLE auctions ADD COLUMN photo_url TEXT",
            "reserve_price": "ALTER TABLE auctions ADD COLUMN reserve_price INTEGER NOT NULL DEFAULT 0",
            "winner_channel_id": "ALTER TABLE auctions ADD COLUMN winner_channel_id INTEGER",
            "payment_method": "ALTER TABLE auctions ADD COLUMN payment_method TEXT",
            "transaction_status": "ALTER TABLE auctions ADD COLUMN transaction_status TEXT NOT NULL DEFAULT 'waiting_to_pay'",
        }
        for column, statement in migrations.items():
            if column not in auction_columns:
                connection.execute(statement)
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


def bid_confirmation_message(current_bid: int, new_bid: int) -> str:
    return (
        f"The auction is at **${format_amount(current_bid)}**. "
        f"You are placing **${format_amount(new_bid)}**. "
        f"So the new bid will be at **${format_amount(new_bid)}**. "
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


def update_config(guild_id: int, manager_role_id=None, log_channel_id=None, auction_channel_id=None):
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
            (guild_id, manager_role_id, log_channel_id, auction_channel_id),
        )


def is_staff(member: discord.Member) -> bool:
    if member.guild_permissions.administrator or member.guild_permissions.manage_guild:
        return True
    config = get_config(member.guild.id)
    if config and config["manager_role_id"] and any(
        role.id == config["manager_role_id"] for role in member.roles
    ):
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
        f"by <@{new_bidder}> on Auction **#{auction_id}** — **{auction['item']}**!",
        "outbid",
    )


def create_auction_record(guild_id: int, channel_id: int, host_id: int, item: str, description: str, starting_bid: int, duration_minutes: int, reserve_price: int = 0, photo_url: str | None = None) -> int:
    timestamp = now()
    with connect() as connection:
        cursor = connection.execute(
            """
            INSERT INTO auctions(
                guild_id, channel_id, host_id, item, description, photo_url,
                starting_bid, reserve_price, current_bid, status, starts_at,
                ends_at, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?)
            """,
            (
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
            "UPDATE auctions SET ends_at = ends_at + ? WHERE id = ? AND status = 'active'",
            (seconds, auction_id),
        )
    auction = fetch_auction(auction_id)
    if auction:
        log_action(auction["guild_id"], actor_id, "extended", auction_id, f"{seconds} seconds")


def remove_bid_record(auction_id: int, bid_id: int, actor_id: int, reason: str):
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
            "SELECT guild_id, starting_bid FROM auctions WHERE id = ?",
            (auction_id,),
        ).fetchone()
        if auction:
            connection.execute(
                "UPDATE auctions SET current_bid = ?, highest_bidder_id = ? WHERE id = ?",
                (
                    highest["amount"] if highest else auction["starting_bid"],
                    highest["bidder_id"] if highest else None,
                    auction_id,
                ),
            )
    if auction:
        log_action(auction["guild_id"], actor_id, "bid_removed", auction_id, reason)


async def send_log(guild_id: int, content: str, title: str = "Auction Log", color: discord.Color = discord.Color.blurple()):
    config = get_config(guild_id)
    if not config or not config["log_channel_id"]:
        return
    channel = bot.get_channel(config["log_channel_id"])
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
    transcript_channel = bot.get_channel(TRANSCRIPT_CHANNEL_ID)
    if transcript_channel is None:
        try:
            transcript_channel = await bot.fetch_channel(TRANSCRIPT_CHANNEL_ID)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            transcript_channel = None
    if transcript_channel is None or getattr(transcript_channel, "guild", None) is None:
        print(f"Transcript channel {TRANSCRIPT_CHANNEL_ID} could not be resolved.")
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
    chunks = [transcript[index:index + 3800] for index in range(0, len(transcript), 3800)]
    transcript_url = None
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
    return True, transcript_url if first_message else None


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
            embed.add_field(
                name=f"Auction ID (queue) `{row['id']}` | Position {row['position']} | {row['item']}",
                value=(
                    f"Channel: <#{row['channel_id']}> | "
                    f"Time of auction: **{format_queue_datetime(row['scheduled_at'])}** | "
                    f"Starting: **{format_amount(row['starting_bid'])}** | "
                    f"Reserve: **{format_amount(row['reserve_price']) if row['reserve_price'] else 'None'}**"
                ),
                inline=False,
            )
    else:
        embed.add_field(name="Queue is empty", value="Add an item with `/queue_add`.", inline=False)
    await channel.send(embed=embed)


def queue_next_position(guild_id: int) -> int:
    with connect() as connection:
        row = connection.execute("SELECT COALESCE(MAX(position), 0) + 1 AS next_position FROM queue_items WHERE guild_id = ?", (guild_id,)).fetchone()
    return row["next_position"]


async def start_queued_item(queue_id: int) -> int | None:
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
        deleted = connection.execute("DELETE FROM queue_items WHERE id = ?", (queue_id,))
        if deleted.rowcount != 1:
            return None
        connection.execute(
            "UPDATE queue_items SET position = position - 1 WHERE guild_id = ? AND position > ?",
            (item["guild_id"], item["position"]),
        )
    auction_id = create_auction_record(
        item["guild_id"],
        destination_channel_id,
        item["created_by"],
        item["item"],
        item["description"],
        item["starting_bid"],
        item["duration_minutes"],
        item["reserve_price"],
        item["photo_url"],
    )
    auction = fetch_auction(auction_id)
    message = await channel.send(
        content=f"<@&{AUCTION_ALERT_ROLE_ID}>",
        embed=auction_embed(auction),
        view=AuctionView(auction_id),
        allowed_mentions=discord.AllowedMentions(roles=True),
    )
    set_message_id(auction_id, message.id)
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
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            interaction.user: discord.PermissionOverwrite(
                view_channel=True,
                read_message_history=True,
                send_messages=True,
                attach_files=True,
            ),
        }
        config = get_config(guild.id)
        manager_role = guild.get_role(config["manager_role_id"]) if config and config["manager_role_id"] else None
        if manager_role:
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
        manager_ping = manager_role.mention if manager_role else ""
        await channel.send(
            content=(
                f"{interaction.user.mention} {manager_ping}\n"
                "🔔 **New auction request ticket.**\n"
                "Please upload pictures of all items here so staff can review them."
            ),
            embed=embed,
            view=SellerTicketView(ticket_id),
        )
        await interaction.response.send_message(
            f"Your auction request ticket has been created: {channel.mention}",
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
        await ensure_ticket_panel()
        await interaction.edit_original_response(embed=ticket_dashboard_embed(interaction.guild.id), view=self)

    @discord.ui.button(label="Enable seller tickets", style=discord.ButtonStyle.success)
    async def enable(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self.guard(interaction):
            return
        await interaction.response.defer()
        set_seller_tickets_enabled(interaction.guild.id, True)
        await ensure_ticket_panel()
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


async def ensure_ticket_panel():
    channel = bot.get_channel(TICKET_PANEL_CHANNEL_ID)
    if channel is None:
        try:
            channel = await bot.fetch_channel(TICKET_PANEL_CHANNEL_ID)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            print(f"Could not access ticket panel channel {TICKET_PANEL_CHANNEL_ID}.")
            return
    if not isinstance(channel, discord.TextChannel):
        print(f"Ticket panel channel {TICKET_PANEL_CHANNEL_ID} is not a text channel.")
        return
    config = get_config(channel.guild.id)
    enabled = seller_tickets_enabled(channel.guild.id)
    panel_message = None
    if config and config["ticket_panel_message_id"]:
        try:
            panel_message = await channel.fetch_message(config["ticket_panel_message_id"])
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            panel_message = None
    if panel_message is None:
        panel_message = await channel.send(embed=ticket_panel_embed(enabled), view=SellerTicketPanelView(enabled))
        with connect() as connection:
            connection.execute(
                """
                INSERT INTO guild_config(guild_id, ticket_panel_message_id)
                VALUES (?, ?)
                ON CONFLICT(guild_id) DO UPDATE SET ticket_panel_message_id = excluded.ticket_panel_message_id
                """,
                (channel.guild.id, panel_message.id),
            )
    else:
        await panel_message.edit(embed=ticket_panel_embed(enabled), view=SellerTicketPanelView(enabled))
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
        reason=reason,
    )


async def send_winner_vouch_reminder(channel: discord.TextChannel, winner_id: int):
    await channel.send(
        f"<@{winner_id}> Thanks for using Bid$. Please make sure to vouch for the auction manager/owner who helped you today in <#1487868025439916186> please and thank you :)"
    )


class PaymentSelect(discord.ui.Select):
    def __init__(self, auction_id: int):
        self.auction_id = auction_id
        super().__init__(
            placeholder="Choose payment method",
            custom_id=f"winner:{auction_id}:payment-method",
            options=[
                discord.SelectOption(label="PayPal", value="PayPal"),
                discord.SelectOption(label="Cash App", value="Cash App"),
                discord.SelectOption(label="Venmo", value="Venmo"),
                discord.SelectOption(label="Apple Pay", value="Apple Pay"),
                discord.SelectOption(label="Revolut", value="Revolut"),
                discord.SelectOption(label="Crypto", value="Crypto"),
            ],
        )

    async def callback(self, interaction: discord.Interaction):
        auction = fetch_auction(self.auction_id)
        if not auction or auction["winner_id"] != interaction.user.id:
            await interaction.response.send_message("Only the auction winner can choose the payment method.", ephemeral=True)
            return
        method = self.values[0]
        if method == "Cash App":
            await interaction.response.send_message(
                f"Hey {interaction.user.mention}, We no longer allow Cashapp, Please pick a different Payment method to use",
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
    def __init__(self, auction_id: int, include_payment_select: bool = True):
        super().__init__(timeout=None)
        self.auction_id = auction_id
        if include_payment_select:
            self.add_item(PaymentSelect(auction_id))

    @discord.ui.button(label="Mark paid", style=discord.ButtonStyle.success, custom_id="winner:mark-paid")
    async def mark_paid(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            await interaction.response.send_message("Only auction staff can mark tickets paid.", ephemeral=True)
            return
        auction = fetch_auction(self.auction_id)
        if not auction or not auction["winner_channel_id"]:
            await interaction.response.send_message("Winner ticket not found.", ephemeral=True)
            return
        set_transaction_status(self.auction_id, "paid_not_claimed")
        await move_winner_channel(auction, "paid_not_claimed")
        await interaction.response.send_message(
            "Payment recorded. Ticket moved to **paid-not-claimed**.",
            ephemeral=True,
        )

    @discord.ui.button(label="Mark claimed", style=discord.ButtonStyle.primary, custom_id="winner:mark-claimed")
    async def mark_claimed(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            await interaction.response.send_message("Only auction staff can mark tickets claimed.", ephemeral=True)
            return
        auction = fetch_auction(self.auction_id)
        if not auction or auction["transaction_status"] != "paid_not_claimed":
            await interaction.response.send_message("Mark the ticket paid before marking it claimed.", ephemeral=True)
            return
        set_transaction_status(self.auction_id, "paid_and_claimed")
        await move_winner_channel(auction, "paid_and_claimed")
        await send_winner_vouch_reminder(interaction.channel, auction["winner_id"])
        await interaction.response.send_message(
            "Ticket moved to **paid-and-claimed**.",
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
        prior_payment = connection.execute(
            "SELECT payment_method FROM auctions "
            "WHERE guild_id = ? AND winner_id = ? AND payment_method IS NOT NULL "
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
            await channel.send(
                content=f"{winner.mention}\n🎉 **Another auction win was added to this ticket.**",
                embed=embed,
                view=PaymentView(auction["id"], include_payment_select=prior_payment is None),
            )
            return channel.id

    overwrites = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False),
        winner: discord.PermissionOverwrite(
            view_channel=True,
            send_messages=False,
            read_message_history=True,
            attach_files=False,
        ),
    }
    config = get_config(guild.id)
    if config and config["manager_role_id"]:
        manager_role = guild.get_role(config["manager_role_id"])
        if manager_role:
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
        color=discord.Color.green(),
    )
    if auction["photo_url"]:
        embed.set_thumbnail(url=auction["photo_url"])
    manager_ping = ""
    if config and config["manager_role_id"]:
        manager_role = guild.get_role(config["manager_role_id"])
        if manager_role:
            manager_ping = f" {manager_role.mention}"
    await channel.send(
        content=(
            f"{winner.mention}{manager_ping}\n"
            f"🔔 **Auction win ticket added.** Staff, please assist the winner.\n\n"
            f"**How would you like to pay?**\n"
            f"In order to chat please press what payment you want."
        ),
        embed=embed,
        view=PaymentView(auction["id"]),
    )
    try:
        ticket_link = f"https://discord.com/channels/{guild.id}/{channel.id}"
        await winner.send(
            f"🏆 You won auction **#{auction['id']}**!\n"
            f"**Item:** {auction['item']}\n"
            f"**Winning amount:** {format_amount(auction['final_bid'])}\n\n"
            f"Please enter the server and complete your payment here: {ticket_link}"
        )
    except (discord.Forbidden, discord.HTTPException):
        await send_log(guild.id, f"Could not DM winner <@{winner.id}> for auction #{auction['id']}.")
    await send_log(guild.id, f"🔒 Created private winner channel {channel.mention} for auction **#{auction['id']}**.")
    return channel.id


def auction_embed(auction: sqlite3.Row) -> discord.Embed:
    status = auction["status"]
    colors = {
        "active": discord.Color.green(),
        "paused": discord.Color.orange(),
        "ended": discord.Color.gold(),
        "cancelled": discord.Color.red(),
    }
    embed = discord.Embed(
        title=f"Auction #{auction['id']} | {auction['item']}",
        description=auction["description"] or "Place your bid using the button below.",
        color=colors.get(status, discord.Color.blurple()),
    )
    embed.add_field(name="Status", value=status.title(), inline=True)
    embed.add_field(name="Auction ID", value=f"`{auction['id']}`", inline=True)
    embed.add_field(name="Starting amount", value=format_amount(auction["starting_bid"]), inline=True)
    embed.add_field(
        name="Reserve price",
        value=(format_amount(auction["reserve_price"]) if auction["reserve_price"] else "No reserve"),
        inline=True,
    )
    embed.add_field(name="Current bid", value=format_amount(auction["current_bid"]), inline=True)
    embed.add_field(
        name="Highest bidder",
        value=(f"<@{auction['highest_bidder_id']}>" if auction["highest_bidder_id"] else "No bids yet"),
        inline=True,
    )
    second_place = fetch_second_place(auction["id"], auction["highest_bidder_id"])
    embed.add_field(
        name="Second Place",
        value=(
            f"<@{second_place['bidder_id']}> - {format_amount(second_place['amount'])}"
            if second_place else "No second bidder yet"
        ),
        inline=True,
    )
    if status in ("active", "paused"):
        embed.add_field(name="Ends", value=f"<t:{int(auction['ends_at'])}:R>", inline=True)
    elif auction["winner_id"]:
        embed.add_field(name="Winner", value=f"<@{auction['winner_id']}>", inline=True)
        embed.add_field(name="Final bid", value=format_amount(auction["final_bid"]), inline=True)
    embed.add_field(name="Hosted by", value=f"<@{auction['host_id']}>", inline=True)
    if auction["photo_url"]:
        embed.set_image(url=auction["photo_url"])
    embed.set_footer(text="Bids are processed in server order. Staff controls are restricted.")
    return embed


async def refresh_auction_message(auction_id: int):
    auction = fetch_auction(auction_id)
    if not auction or not auction["message_id"]:
        return
    channel = bot.get_channel(auction["channel_id"])
    if not channel:
        return
    try:
        message = await channel.fetch_message(auction["message_id"])
        await message.edit(embed=auction_embed(auction), view=AuctionView(auction_id))
    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
        pass


async def require_server(interaction: discord.Interaction) -> bool:
    if interaction.guild is None:
        await interaction.response.send_message("This auction feature is only available in a server.", ephemeral=True)
        return False
    return True


async def require_staff(interaction: discord.Interaction) -> bool:
    if not await require_server(interaction):
        return False
    if not is_staff(interaction.user):
        await interaction.response.send_message("You need auction manager or server management permissions for that.", ephemeral=True)
        return False
    return True


async def finish_expired_auction(auction: sqlite3.Row):
    if auction["status"] != "active" or auction["ends_at"] > now():
        return False
    update_auction_status(auction["id"], "ended", bot.user.id if bot.user else 0)
    await refresh_auction_message(auction["id"])
    updated = fetch_auction(auction["id"])
    if updated and updated["winner_id"]:
        message = f"🏆 Auction **#{updated['id']}** ended! Winner: <@{updated['winner_id']}> with **{format_amount(updated['final_bid'])}**."
    else:
        message = f"🏁 Auction **#{auction['id']}** ended with no bids."
    channel = bot.get_channel(auction["channel_id"])
    if channel:
        await send_temporary_message(auction["id"], channel, message, "ended_announcement")
    if updated and updated["winner_id"]:
        await create_winner_channel(updated)
    await delete_temporary_messages(auction["id"])
    await send_log(auction["guild_id"], f"Auction #{auction['id']} automatically ended.")
    return True


async def place_bid(interaction: discord.Interaction, auction_id: int, amount: int):
    key = (auction_id, interaction.user.id)
    async with bid_lock:
        elapsed = time.monotonic() - last_bid_times.get(key, 0)
        if elapsed < BID_COOLDOWN_SECONDS:
            return False, f"Please wait {BID_COOLDOWN_SECONDS - elapsed:.1f} seconds before bidding again.", None, None

        with connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            auction = connection.execute(
                "SELECT * FROM auctions WHERE id = ?",
                (auction_id,),
            ).fetchone()
            if auction is None or auction["guild_id"] != interaction.guild.id:
                return False, "That auction was not found in this server.", None, None
            if auction["status"] != "active":
                return False, "That auction is not accepting bids.", None, None
            if auction["ends_at"] <= now():
                return False, "That auction has already ended.", None, None
            if auction["host_id"] == interaction.user.id:
                return False, "You cannot bid on your own auction.", None, None
            if amount <= auction["current_bid"]:
                return False, f"Your bid must be higher than {format_amount(auction['current_bid'])}.", None, None

            connection.execute(
                "INSERT INTO bids(auction_id, bidder_id, amount, created_at) VALUES (?, ?, ?, ?)",
                (auction_id, interaction.user.id, amount, now()),
            )
            remaining_seconds = max(0, auction["ends_at"] - now())
            extension = (
                min(ANTI_SNIPE_SECONDS, ANTI_SNIPE_WINDOW_SECONDS - remaining_seconds)
                if remaining_seconds <= ANTI_SNIPE_WINDOW_SECONDS
                else 0
            )
            connection.execute(
                """
                UPDATE auctions
                SET current_bid = ?, highest_bidder_id = ?,
                    ends_at = ends_at + ?
                WHERE id = ?
                """,
                (amount, interaction.user.id, extension, auction_id),
            )
            previous_bidder = auction["highest_bidder_id"]
            previous_amount = auction["current_bid"]
        last_bid_times[key] = time.monotonic()

    if extension:
        await send_log(interaction.guild.id, f"Auction #{auction_id} extended by {extension} seconds due to a last-second bid.")
    return True, "Bid accepted.", previous_bidder, previous_amount


async def submit_increment_bid(interaction: discord.Interaction, auction_id: int, increment: int):
    auction = fetch_auction(auction_id)
    if auction is None:
        await interaction.response.send_message("That auction was not found.", ephemeral=True)
        return
    proposed_amount = auction["current_bid"] + increment
    await interaction.response.send_message(
        bid_confirmation_message(auction["current_bid"], proposed_amount),
        view=ConfirmBidView(auction_id, interaction.user.id, proposed_amount, increment),
        ephemeral=True,
    )


class ConfirmBidView(discord.ui.View):
    def __init__(self, auction_id: int, bidder_id: int, amount: int, increment: int | None = None):
        super().__init__(timeout=60)
        self.auction_id = auction_id
        self.bidder_id = bidder_id
        self.amount = amount
        self.increment = increment

    @discord.ui.button(label="Confirm bid", style=discord.ButtonStyle.success)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.bidder_id:
            await interaction.response.send_message("Only the bidder who opened this confirmation can use it.", ephemeral=True)
            return
        auction = fetch_auction(self.auction_id)
        if auction is None:
            await interaction.response.edit_message(content="That auction was not found.", view=None)
            return
        bid_amount = auction["current_bid"] + self.increment if self.increment is not None else self.amount
        accepted, message, previous_bidder, previous_amount = await place_bid(
            interaction, self.auction_id, bid_amount
        )
        if not accepted:
            await interaction.response.edit_message(content=message, view=None)
            return
        await refresh_auction_message(self.auction_id)
        await interaction.response.edit_message(
            content=f"✅ Bid placed at **${format_amount(bid_amount)}**.", view=None
        )
        if previous_bidder and previous_bidder != interaction.user.id:
            await send_outbid_notification(
                self.auction_id, previous_bidder, previous_amount, interaction.user.id
            )

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id == self.bidder_id:
            await interaction.response.edit_message(content="Bid cancelled.", view=None)


class CreateAuctionModal(discord.ui.Modal, title="Create Auction"):
    item = discord.ui.TextInput(label="Item / Brainrot", max_length=256, placeholder="What are you auctioning?")
    description = discord.ui.TextInput(label="Description", required=False, style=discord.TextStyle.paragraph, max_length=1000)
    starting_bid = discord.ui.TextInput(label="Starting bid", placeholder="100")
    reserve_price = discord.ui.TextInput(label="Reserve price (optional)", required=False, placeholder="Leave blank for no reserve")
    duration_minutes = discord.ui.TextInput(label="Duration in minutes", placeholder="60")

    async def on_submit(self, interaction: discord.Interaction):
        if not is_staff(interaction.user):
            await interaction.response.send_message("Only auction staff can create auctions.", ephemeral=True)
            return
        await interaction.response.send_message(
            "Please use `/auction_create` for new auctions so you can attach the required image.",
            ephemeral=True,
        )
        return
        try:
            starting_bid = int(self.starting_bid.value)
            reserve_price = int(self.reserve_price.value or "0")
            duration = int(self.duration_minutes.value)
        except ValueError:
            await interaction.response.send_message("Starting bid, reserve price, and duration must be whole numbers.", ephemeral=True)
            return
        if not 1 <= starting_bid <= MAX_BID or not 0 <= reserve_price <= MAX_BID or not 1 <= duration <= 10080:
            await interaction.response.send_message("Use a starting bid from 1 to 2,147,483,647, a reserve of 0 or more, and a duration from 1 to 10,080 minutes.", ephemeral=True)
            return
        if reserve_price and reserve_price < starting_bid:
            await interaction.response.send_message("The reserve price must be at least the starting bid.", ephemeral=True)
            return
        draft = {
            "guild_id": interaction.guild.id,
            "channel_id": interaction.channel.id,
            "host_id": interaction.user.id,
            "item": self.item.value,
            "description": self.description.value,
            "starting_bid": starting_bid,
            "reserve_price": reserve_price,
            "duration": duration,
            "photo_url": None,
        }
        await interaction.response.send_message(
            "Review this auction, then publish it when ready.",
            embed=preview_embed(draft),
            view=PublishAuctionView(draft, interaction.user.id),
            ephemeral=True,
        )


def preview_embed(draft: dict) -> discord.Embed:
    embed = discord.Embed(
        title=f"Auction Preview | {draft['item']}",
        description=draft["description"] or "No description provided.",
        color=discord.Color.blurple(),
    )
    embed.add_field(name="Starting amount", value=format_amount(draft["starting_bid"]), inline=True)
    embed.add_field(name="Reserve price", value=(format_amount(draft["reserve_price"]) if draft["reserve_price"] else "No reserve"), inline=True)
    embed.add_field(name="Duration", value=f"{draft['duration']} minutes", inline=True)
    if draft.get("photo_url"):
        embed.set_image(url=draft["photo_url"])
    return embed


class PublishAuctionView(discord.ui.View):
    def __init__(self, draft: dict, actor_id: int):
        super().__init__(timeout=300)
        self.draft = draft
        self.actor_id = actor_id

    @discord.ui.button(label="Publish auction", style=discord.ButtonStyle.success)
    async def publish(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.actor_id:
            await interaction.response.send_message("Only the staff member who created this preview can publish it.", ephemeral=True)
            return
        auction_id = create_auction_record(
            self.draft["guild_id"],
            self.draft["channel_id"],
            self.draft["host_id"],
            self.draft["item"],
            self.draft["description"],
            self.draft["starting_bid"],
            self.draft["duration"],
            self.draft["reserve_price"],
            self.draft.get("photo_url"),
        )
        auction = fetch_auction(auction_id)
        channel = bot.get_channel(self.draft["channel_id"])
        if channel is None:
            await interaction.response.edit_message(content="The auction channel is no longer available.", view=None)
            return
        message = await channel.send(
            content=f"<@&{AUCTION_ALERT_ROLE_ID}>",
            embed=auction_embed(auction),
            view=AuctionView(auction_id),
            allowed_mentions=discord.AllowedMentions(roles=True),
        )
        set_message_id(auction_id, message.id)
        await interaction.response.edit_message(content=f"Auction **#{auction_id}** published in {channel.mention}.", embed=None, view=None)
        await send_log(interaction.guild.id, f"Auction #{auction_id} published by {interaction.user.mention}.")

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id == self.actor_id:
            await interaction.response.edit_message(content="Auction draft discarded.", embed=None, view=None)


class BidModal(discord.ui.Modal):
    amount = discord.ui.TextInput(label="Your bid", placeholder="Enter an amount")

    def __init__(self, auction_id: int):
        super().__init__(title=f"Bid on Auction #{auction_id}")
        self.auction_id = auction_id

    async def on_submit(self, interaction: discord.Interaction):
        try:
            amount = int(self.amount.value.replace(",", "").strip())
        except ValueError:
            await interaction.response.send_message("Your bid must be a whole number.", ephemeral=True)
            return
        if not 1 <= amount <= MAX_BID:
            await interaction.response.send_message("That bid amount is out of range.", ephemeral=True)
            return
        auction = fetch_auction(self.auction_id)
        if auction is None:
            await interaction.response.send_message("That auction was not found.", ephemeral=True)
            return
        await interaction.response.send_message(
            bid_confirmation_message(auction["current_bid"], amount),
            view=ConfirmBidView(self.auction_id, interaction.user.id, amount),
            ephemeral=True,
        )


class ConfirmView(discord.ui.View):
    def __init__(self, auction_id: int, action: str, actor_id: int):
        super().__init__(timeout=60)
        self.auction_id = auction_id
        self.action = action
        self.actor_id = actor_id

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.actor_id:
            await interaction.response.send_message("Only the person who opened this confirmation can use it.", ephemeral=True)
            return
        auction = fetch_auction(self.auction_id)
        if not auction or auction["status"] not in ("active", "paused"):
            await interaction.response.edit_message(content="That auction is no longer available for this action.", view=None)
            return
        update_auction_status(self.auction_id, self.action, interaction.user.id)
        await refresh_auction_message(self.auction_id)
        await interaction.response.edit_message(content=f"Auction #{self.auction_id} marked **{self.action}**.", view=None)
        if self.action == "ended":
            updated = fetch_auction(self.auction_id)
            winner = f" Winner: <@{updated['winner_id']}> for **{format_amount(updated['final_bid'])}**." if updated and updated["winner_id"] else (
                " The reserve price was not met." if updated and updated["highest_bidder_id"] else " No bids were placed."
            )
            channel = bot.get_channel(auction["channel_id"])
            if channel:
                await send_temporary_message(
                    self.auction_id,
                    channel,
                    f"🏁 Auction **#{self.auction_id}** ended.{winner}",
                    "ended_announcement",
                )
            if updated and updated["winner_id"]:
                await create_winner_channel(updated)
            await delete_temporary_messages(self.auction_id)

    @discord.ui.button(label="Keep open", style=discord.ButtonStyle.secondary)
    async def dismiss(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(content="Action cancelled.", view=None)


class StaffControlsView(discord.ui.View):
    def __init__(self, auction_id: int):
        super().__init__(timeout=120)
        self.auction_id = auction_id

    async def check(self, interaction: discord.Interaction) -> bool:
        if not is_staff(interaction.user):
            await interaction.response.send_message("Auction staff only.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Pause / Resume", style=discord.ButtonStyle.primary)
    async def pause_resume(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self.check(interaction):
            return
        auction = fetch_auction(self.auction_id)
        if not auction or auction["status"] not in ("active", "paused"):
            await interaction.response.send_message("That auction is no longer active.", ephemeral=True)
            return
        if auction["status"] == "active":
            remaining = max(0, auction["ends_at"] - now())
            update_auction_status(self.auction_id, "paused", interaction.user.id, remaining)
            result = "paused"
        else:
            with connect() as connection:
                connection.execute(
                    "UPDATE auctions SET status = 'active', ends_at = ?, paused_remaining = NULL WHERE id = ?",
                    (now() + (auction["paused_remaining"] or 60), self.auction_id),
                )
            log_action(auction["guild_id"], interaction.user.id, "resumed", self.auction_id)
            result = "resumed"
        await refresh_auction_message(self.auction_id)
        await interaction.response.send_message(f"Auction #{self.auction_id} {result}.", ephemeral=True)

    @discord.ui.button(label="Extend 5 min", style=discord.ButtonStyle.success)
    async def extend(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self.check(interaction):
            return
        auction = fetch_auction(self.auction_id)
        if not auction or auction["status"] != "active":
            await interaction.response.send_message("Only active auctions can be extended.", ephemeral=True)
            return
        extend_auction_record(self.auction_id, 300, interaction.user.id)
        await refresh_auction_message(self.auction_id)
        await interaction.response.send_message("Auction extended by 5 minutes.", ephemeral=True)

    @discord.ui.button(label="End", style=discord.ButtonStyle.danger)
    async def end(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self.check(interaction):
            return
        await interaction.response.send_message(
            f"Confirm ending auction #{self.auction_id}.",
            view=ConfirmView(self.auction_id, "ended", interaction.user.id),
            ephemeral=True,
        )

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self.check(interaction):
            return
        await interaction.response.send_message(
            f"Confirm cancelling auction #{self.auction_id}.",
            view=ConfirmView(self.auction_id, "cancelled", interaction.user.id),
            ephemeral=True,
        )


def auction_dashboard_embed(guild_id: int) -> discord.Embed:
    with connect() as connection:
        active = connection.execute(
            "SELECT id, item, current_bid, status FROM auctions "
            "WHERE guild_id = ? AND status IN ('active', 'paused') ORDER BY id",
            (guild_id,),
        ).fetchall()
        winners = connection.execute(
            "SELECT * FROM auction_winner_archive WHERE guild_id = ? "
            "ORDER BY archived_at DESC LIMIT 10",
            (guild_id,),
        ).fetchall()
        latest = connection.execute(
            "SELECT id, item, winner_id, final_bid, status FROM auctions "
            "WHERE guild_id = ? AND status IN ('ended', 'cancelled') "
            "ORDER BY id DESC LIMIT 10",
            (guild_id,),
        ).fetchall()

    embed = discord.Embed(title="Auction Dashboard", color=discord.Color.gold())
    active_text = (
        "\n".join(
            f"**#{row['id']}** {row['item']} | {format_amount(row['current_bid'])} | {row['status'].title()}"
            for row in active
        )
        if active
        else "No active auctions."
    )
    embed.add_field(name="Active auctions", value=active_text, inline=False)

    winner_rows = [*winners, *latest]
    winner_text = []
    seen = set()
    for row in winner_rows:
        key = (row["original_auction_id"] if "original_auction_id" in row.keys() else row["id"], row["item"])
        if key in seen:
            continue
        seen.add(key)
        result = (
            f"Winner: <@{row['winner_id']}> for **${format_amount(row['final_bid'])}**"
            if row["winner_id"] and row["final_bid"] is not None
            else row["status"].title()
        )
        number = row["original_auction_id"] if "original_auction_id" in row.keys() else row["id"]
        winner_text.append(f"**#{number}** {row['item']} | {result}")
    embed.add_field(
        name="Previous winners",
        value="\n".join(winner_text) if winner_text else "No previous winners yet.",
        inline=False,
    )
    embed.set_footer(text="Reset archives completed auctions and starts live numbering over at #1 when safe.")
    return embed


class AuctionDashboardView(discord.ui.View):
    def __init__(self, owner_id: int):
        super().__init__(timeout=300)
        self.owner_id = owner_id

    async def check_access(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.owner_id and not is_staff(interaction.user):
            await interaction.response.send_message("Auction staff only.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.secondary)
    async def refresh(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self.check_access(interaction):
            return
        await interaction.response.edit_message(embed=auction_dashboard_embed(interaction.guild.id), view=self)

    @discord.ui.button(label="Reset auction numbering", style=discord.ButtonStyle.danger)
    async def reset(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self.check_access(interaction):
            return
        success, message = reset_auction_numbering(interaction.guild.id, interaction.user.id)
        await interaction.response.edit_message(
            content=message,
            embed=auction_dashboard_embed(interaction.guild.id),
            view=self if success else self,
        )


class AuctionView(discord.ui.View):
    def __init__(self, auction_id: int):
        super().__init__(timeout=None)
        self.auction_id = auction_id
        auction = fetch_auction(auction_id)
        bidding_enabled = bool(auction and auction["status"] == "active")
        for child in self.children:
            if isinstance(child, discord.ui.Button):
                suffix = child.label.lower().replace(" ", "-").replace("+", "plus")
                child.custom_id = f"auction:{auction_id}:{suffix}"
                if child.label.startswith("Bid") or child.label == "Remove my bid":
                    child.disabled = not bidding_enabled

    @discord.ui.button(label="Bid +$1", style=discord.ButtonStyle.success, row=0)
    async def bid_one(self, interaction: discord.Interaction, button: discord.ui.Button):
        await submit_increment_bid(interaction, self.auction_id, 1)

    @discord.ui.button(label="Bid +$2", style=discord.ButtonStyle.success, row=0)
    async def bid_two(self, interaction: discord.Interaction, button: discord.ui.Button):
        await submit_increment_bid(interaction, self.auction_id, 2)

    @discord.ui.button(label="Bid +$3", style=discord.ButtonStyle.success, row=0)
    async def bid_three(self, interaction: discord.Interaction, button: discord.ui.Button):
        await submit_increment_bid(interaction, self.auction_id, 3)

    @discord.ui.button(label="Bid +$5", style=discord.ButtonStyle.success, row=0)
    async def bid_five(self, interaction: discord.Interaction, button: discord.ui.Button):
        await submit_increment_bid(interaction, self.auction_id, 5)

    @discord.ui.button(label="Bid +$10", style=discord.ButtonStyle.success, row=0)
    async def bid_ten(self, interaction: discord.Interaction, button: discord.ui.Button):
        await submit_increment_bid(interaction, self.auction_id, 10)

    @discord.ui.button(label="Remove my bid", style=discord.ButtonStyle.danger, row=1)
    async def remove_my_bid(self, interaction: discord.Interaction, button: discord.ui.Button):
        auction = fetch_auction(self.auction_id)
        if not auction or auction["guild_id"] != interaction.guild.id:
            await interaction.response.send_message("Auction not found.", ephemeral=True)
            return
        if auction["status"] != "active":
            await interaction.response.send_message("Only active-auction bids can be removed.", ephemeral=True)
            return
        with connect() as connection:
            bid_record = connection.execute(
                "SELECT id FROM bids WHERE auction_id = ? AND bidder_id = ? AND valid = 1 "
                "ORDER BY created_at DESC, id DESC LIMIT 1",
                (self.auction_id, interaction.user.id),
            ).fetchone()
        if not bid_record:
            await interaction.response.send_message("You do not have an active bid on this auction.", ephemeral=True)
            return
        remove_bid_record(
            self.auction_id,
            bid_record["id"],
            interaction.user.id,
            "Removed by bidder",
        )
        await refresh_auction_message(self.auction_id)
        await interaction.response.send_message(
            "Your latest bid was removed and the auction total was recalculated.",
            ephemeral=True,
        )

    @discord.ui.button(label="Staff controls", style=discord.ButtonStyle.secondary, row=1)
    async def staff_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            await interaction.response.send_message("Auction staff only.", ephemeral=True)
            return
        await interaction.response.send_message("Choose a staff action:", view=StaffControlsView(self.auction_id), ephemeral=True)


class AuctionPanelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Create auction", style=discord.ButtonStyle.primary, custom_id="auction:create")
    async def create_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            await interaction.response.send_message("Only auction staff can create auctions.", ephemeral=True)
            return
        await interaction.response.send_message(
            "Use `/auction_create` to start an auction immediately with the required image attachment.",
            ephemeral=True,
        )


@bot.event
async def on_ready():
    global sync_done
    if not sync_done:
        bot.add_view(AuctionPanelView())
        with connect() as connection:
            active = connection.execute(
                "SELECT id, message_id FROM auctions WHERE status IN ('active', 'paused') AND message_id IS NOT NULL"
            ).fetchall()
        for auction in active:
            bot.add_view(AuctionView(auction["id"]), message_id=auction["message_id"])
        for auction in active:
            await refresh_auction_message(auction["id"])
        with connect() as connection:
            winner_channels = connection.execute(
                "SELECT id FROM auctions WHERE winner_channel_id IS NOT NULL AND winner_id IS NOT NULL"
            ).fetchall()
        for auction in winner_channels:
            bot.add_view(PaymentView(auction["id"]))
        with connect() as connection:
            open_tickets = connection.execute(
                "SELECT id FROM seller_tickets WHERE status = 'open'"
            ).fetchall()
        for ticket in open_tickets:
            bot.add_view(SellerTicketView(ticket["id"]))
        await ensure_ticket_panel()
        if GUILD_ID and GUILD_ID.isdigit():
            development_guild = discord.Object(id=int(GUILD_ID))
            bot.tree.clear_commands(guild=development_guild)
            bot.tree.copy_global_to(guild=development_guild)
            await bot.tree.sync(guild=development_guild)
            bot.tree.clear_commands(guild=None)
            await bot.tree.sync()
            print(f"Synced commands instantly to development server {GUILD_ID}.")
        else:
            await bot.tree.sync()
        sync_done = True
    if not auction_worker.is_running():
        auction_worker.start()
    if not schedule_worker.is_running():
        schedule_worker.start()
    print(f"Logged in as {bot.user}")


@tasks.loop(seconds=AUCTION_EXPIRY_CHECK_SECONDS)
async def auction_worker():
    with connect() as connection:
        due = connection.execute(
            "SELECT * FROM auctions WHERE status = 'active' AND ends_at <= ?",
            (now(),),
        ).fetchall()
        soon = connection.execute(
            "SELECT * FROM auctions WHERE status = 'active' AND ending_announced = 0 AND ends_at <= ? AND ends_at > ?",
            (now() + 60, now()),
        ).fetchall()
        for auction in soon:
            connection.execute("UPDATE auctions SET ending_announced = 1 WHERE id = ?", (auction["id"],))
    
    for auction in soon:
        channel = bot.get_channel(auction["channel_id"])
        if channel:
            await send_temporary_message(
                auction["id"],
                channel,
                f"<@&{AUCTION_ALERT_ROLE_ID}> ⏰ Auction **#{auction['id']} — {auction['item']}** has **1 minute left**!",
                "ending_soon",
            )
    for auction in due:
        await finish_expired_auction(auction)


@auction_worker.before_loop
async def before_auction_worker():
    await bot.wait_until_ready()


@tasks.loop(seconds=15)
async def schedule_worker():
    current = datetime.now(timezone.utc)
    day = current.weekday()
    current_time = current.strftime("%H:%M")
    today = current.strftime("%Y-%m-%d")
    announcement_rows = []
    server_announcement_rows = []
    with connect() as connection:
        schedules_today = connection.execute(
            "SELECT * FROM schedules WHERE enabled = 1 AND day_of_week = ?",
            (day,),
        ).fetchall()
        for schedule in schedules_today:
            hour, minute = (int(part) for part in schedule["time_utc"].split(":"))
            scheduled_at = current.replace(hour=hour, minute=minute, second=0, microsecond=0)
            seconds_until = (scheduled_at - current).total_seconds()
            if 0 < seconds_until <= 900 and schedule["last_announcement_date"] != today:
                connection.execute(
                    "UPDATE schedules SET last_announcement_date = ? WHERE id = ?",
                    (today, schedule["id"]),
                )
                announcement_rows.append((schedule, scheduled_at))
        schedules = connection.execute(
            """
            SELECT * FROM schedules
            WHERE enabled = 1 AND day_of_week = ? AND time_utc = ?
              AND (last_run_date IS NULL OR last_run_date != ?)
            """,
            (day, current_time, today),
        ).fetchall()
        claimed_schedules = []
        for schedule in schedules:
            claimed = connection.execute(
                "UPDATE schedules SET last_run_date = ? WHERE id = ? AND (last_run_date IS NULL OR last_run_date != ?)",
                (today, schedule["id"], today),
            )
            if claimed.rowcount == 1:
                claimed_schedules.append(schedule)
        announcements_today = connection.execute(
            "SELECT * FROM scheduled_announcements WHERE enabled = 1 AND day_of_week = ?",
            (day,),
        ).fetchall()
        for scheduled_announcement in announcements_today:
            hour, minute = (int(part) for part in scheduled_announcement["time_utc"].split(":"))
            scheduled_at = current.replace(hour=hour, minute=minute, second=0, microsecond=0)
            seconds_until = (scheduled_at - current).total_seconds()
            if -15 <= seconds_until <= 0 and scheduled_announcement["last_sent_date"] != today:
                connection.execute(
                    "UPDATE scheduled_announcements SET last_sent_date = ? WHERE id = ?",
                    (today, scheduled_announcement["id"]),
                )
                server_announcement_rows.append((scheduled_announcement, scheduled_at))
    for schedule in claimed_schedules:
        auction_id = create_auction_record(
            schedule["guild_id"],
            schedule["channel_id"],
            schedule["created_by"],
            schedule["item"],
            schedule["description"],
            schedule["starting_bid"],
            schedule["duration_minutes"],
            photo_url=schedule["photo_url"],
        )
        auction = fetch_auction(auction_id)
        channel = bot.get_channel(schedule["channel_id"])
        if channel:
            message = await channel.send(embed=auction_embed(auction), view=AuctionView(auction_id))
            set_message_id(auction_id, message.id)
    for schedule, scheduled_at in announcement_rows:
        channel = bot.get_channel(schedule["channel_id"])
        if channel:
            await channel.send(
                f"📅 Upcoming auction **{schedule['item']}** starts <t:{int(scheduled_at.timestamp())}:R>."
            )
    for scheduled_announcement, scheduled_at in server_announcement_rows:
        channel = bot.get_channel(scheduled_announcement["channel_id"])
        if channel:
            role_mention = f"<@&{scheduled_announcement['role_id']}> " if scheduled_announcement["role_id"] else ""
            await channel.send(
                f"{role_mention}{scheduled_announcement['announcement']}\n"
                f"📅 Scheduled for <t:{int(scheduled_at.timestamp())}:F>.",
                allowed_mentions=discord.AllowedMentions(users=False, roles=True, everyone=False),
            )
    with connect() as connection:
        due_queue_items = connection.execute(
            "SELECT id, guild_id FROM queue_items WHERE scheduled_at IS NOT NULL AND scheduled_at <= ? "
            "ORDER BY scheduled_at, position, id",
            (now(),),
        ).fetchall()
    for row in due_queue_items:
        try:
            await start_queued_item(row["id"])
        except (discord.Forbidden, discord.HTTPException) as error:
            print(f"Could not automatically start queue item #{row['id']}: {error}")
    for guild_id in {row["guild_id"] for row in due_queue_items}:
        guild = bot.get_guild(guild_id)
        if guild:
            await refresh_queue_message(await ensure_queue_channel(guild))


@schedule_worker.before_loop
async def before_schedule_worker():
    await bot.wait_until_ready()


@bot.tree.command(name="auction_panel", description="Post the auction creation panel.")
async def auction_panel(interaction: discord.Interaction):
    if not await require_staff(interaction):
        return
    embed = discord.Embed(
        title="Auction House",
        description="Staff can create an auction with the button below. Members can bid from each auction message.",
        color=discord.Color.blurple(),
    )
    embed.add_field(name="Staff", value="Use the panel to create auctions and the staff controls on each auction to manage them.", inline=False)
    embed.add_field(name="Members", value="Click **Place bid** and enter your offer. The bot records bids in server order.", inline=False)
    await interaction.response.send_message(embed=embed, view=AuctionPanelView())


@bot.tree.command(name="ticket_dashboard", description="View ticket totals and manage seller auction tickets.")
async def ticket_dashboard(interaction: discord.Interaction):
    if not await require_staff(interaction):
        return
    await interaction.response.send_message(
        embed=ticket_dashboard_embed(interaction.guild.id),
        view=TicketDashboardView(),
        ephemeral=True,
    )


class TicketCloseConfirmView(discord.ui.View):
    def __init__(self, owner_id: int):
        super().__init__(timeout=60)
        self.owner_id = owner_id

    @discord.ui.button(label="Yes", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Only the staff member who started this confirmation can use it.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        seller_ticket = None
        winner_auction = None
        with connect() as connection:
            seller_ticket = connection.execute(
                "SELECT * FROM seller_tickets WHERE channel_id = ?",
                (interaction.channel.id,),
            ).fetchone()
            winner_auction = connection.execute(
                "SELECT * FROM auctions WHERE winner_channel_id = ?",
                (interaction.channel.id,),
            ).fetchone()
        if seller_ticket is None and winner_auction is None:
            await interaction.followup.send("This channel is no longer a bot ticket.", ephemeral=True)
            return
        ticket_label = (
            f"Seller Auction Request #{seller_ticket['id']}"
            if seller_ticket
            else f"Winner Auction #{winner_auction['id']}"
        )
        try:
            deleted, transcript_url = await close_ticket_channel(
                interaction.channel,
                interaction.guild.id,
                interaction.user,
                ticket_label,
                delete_channel=False,
            )
        except (discord.Forbidden, discord.HTTPException) as error:
            await interaction.followup.send(
                f"I could not close this ticket because Discord returned an error: {error}",
                ephemeral=True,
            )
            return
        if not deleted:
            await interaction.followup.send(
                "I could not send the transcript to the transcript channel, so the ticket was kept.",
                ephemeral=True,
            )
            return
        with connect() as connection:
            if seller_ticket:
                connection.execute(
                    "UPDATE seller_tickets SET status = 'closed', closed_at = ? WHERE id = ?",
                    (now(), seller_ticket["id"]),
                )
            if winner_auction:
                connection.execute(
                    "UPDATE auctions SET transaction_status = 'closed' WHERE id = ?",
                    (winner_auction["id"],),
                )
        if winner_auction:
            log_action(interaction.guild.id, interaction.user.id, "ticket_closed", winner_auction["id"])
        try:
            await interaction.channel.delete(reason=f"Ticket closed by {interaction.user}")
        except (discord.Forbidden, discord.HTTPException) as error:
            await interaction.followup.send(
                f"The ticket was marked closed and the transcript was saved, but I could not delete the channel: {error}",
                ephemeral=True,
            )
            return
        await interaction.followup.send(
            f"Transcript sent and ticket deleted. [View transcript]({transcript_url})",
            ephemeral=True,
        )

    @discord.ui.button(label="No", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id == self.owner_id:
            await interaction.response.edit_message(content="Ticket close cancelled.", view=None)


@bot.tree.command(name="ticket_close", description="Close the seller or winner ticket you are currently viewing.")
async def ticket_close(interaction: discord.Interaction):
    if not await require_staff(interaction):
        return
    if not isinstance(interaction.channel, discord.TextChannel):
        await interaction.response.send_message("This command must be used inside a ticket text channel.", ephemeral=True)
        return

    seller_ticket = None
    winner_auction = None
    with connect() as connection:
        seller_ticket = connection.execute(
            "SELECT * FROM seller_tickets WHERE channel_id = ?",
            (interaction.channel.id,),
        ).fetchone()
        winner_auction = connection.execute(
            "SELECT * FROM auctions WHERE winner_channel_id = ?",
            (interaction.channel.id,),
        ).fetchone()

    if seller_ticket is None and winner_auction is None:
        await interaction.response.send_message("This channel is not a bot ticket.", ephemeral=True)
        return

    await interaction.response.send_message(
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
        if values["reserve_price"] and values["reserve_price"] < values["starting_bid"]:
            await interaction.response.send_message("The reserve price must be at least the starting amount.", ephemeral=True)
            return
        connection.execute(
            "UPDATE queue_items SET channel_id = ?, item = ?, starting_bid = ?, duration_minutes = ?, photo_url = ?, reserve_price = ?, description = ?, scheduled_at = ? WHERE id = ?",
            (*values.values(), scheduled_at, queue_id),
        )
    await refresh_queue_message(await ensure_queue_channel(interaction.guild))
    await interaction.response.send_message(f"Updated queue item **#{queue_id}**.", ephemeral=True)


@bot.tree.command(name="queue_start", description="Start the first or selected queued auction publicly.")
@app_commands.describe(queue_id="Queue item ID")
async def queue_start(interaction: discord.Interaction, queue_id: int):
    if not await require_queue_staff(interaction):
        return
    config = get_config(interaction.guild.id)
    if not config:
        await interaction.response.send_message("Run `/auction_setup` before starting queue items.", ephemeral=True)
        return
    with connect() as connection:
        item = connection.execute("SELECT * FROM queue_items WHERE id = ? AND guild_id = ?", (queue_id, interaction.guild.id)).fetchone()
        if item:
            connection.execute("DELETE FROM queue_items WHERE id = ?", (queue_id,))
            connection.execute("UPDATE queue_items SET position = position - 1 WHERE guild_id = ? AND position > ?", (interaction.guild.id, item["position"]))
    if not item:
        await interaction.response.send_message("That queue item was not found.", ephemeral=True)
        return
    destination_channel_id = item["channel_id"] or config["auction_channel_id"]
    if not destination_channel_id:
        await interaction.response.send_message("This queue item has no destination channel. Edit it or run `/auction_setup` with an auction channel.", ephemeral=True)
        return
    channel = bot.get_channel(destination_channel_id)
    if channel is None:
        await interaction.response.send_message("The queued destination channel is no longer available.", ephemeral=True)
        return
    auction_id = create_auction_record(interaction.guild.id, destination_channel_id, interaction.user.id, item["item"], item["description"], item["starting_bid"], item["duration_minutes"], item["reserve_price"], item["photo_url"])
    auction = fetch_auction(auction_id)
    message = await channel.send(
        content=f"<@&{AUCTION_ALERT_ROLE_ID}>",
        embed=auction_embed(auction),
        view=AuctionView(auction_id),
        allowed_mentions=discord.AllowedMentions(roles=True),
    )
    set_message_id(auction_id, message.id)
    await refresh_queue_message(await ensure_queue_channel(interaction.guild))
    await interaction.response.send_message(f"Started auction **#{auction_id}** in {channel.mention}.", ephemeral=True)


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


@bot.tree.command(name="bid", description="Place a bid without using the auction button.")
@app_commands.describe(auction_id="Auction number", amount="Your bid amount")
async def bid(interaction: discord.Interaction, auction_id: int, amount: app_commands.Range[int, 1, MAX_BID]):
    if not await require_server(interaction):
        return
    auction = fetch_auction(auction_id)
    if not auction or auction["guild_id"] != interaction.guild.id:
        await interaction.response.send_message("That auction was not found.", ephemeral=True)
        return
    await interaction.response.send_message(
        bid_confirmation_message(auction["current_bid"], amount),
        view=ConfirmBidView(auction_id, interaction.user.id, amount),
        ephemeral=True,
    )


@bot.tree.command(name="auction_setup", description="Configure auction staff role, log channel, and auction channel.")
@app_commands.describe(manager_role="Role allowed to manage auctions", log_channel="Channel for auction logs", auction_channel="Default scheduled-auction channel")
async def auction_setup(interaction: discord.Interaction, manager_role: discord.Role | None = None, log_channel: discord.TextChannel | None = None, auction_channel: discord.TextChannel | None = None):
    if not await require_server(interaction):
        return
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("Administrator permission is required for setup.", ephemeral=True)
        return
    update_config(interaction.guild.id, manager_role.id if manager_role else None, log_channel.id if log_channel else None, auction_channel.id if auction_channel else None)
    await interaction.response.send_message("Auction settings updated.", ephemeral=True)


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


@bot.tree.command(name="auction_remove_bid", description="Invalidate a bid and recalculate the winner.")
@app_commands.describe(auction_id="Auction number", bid_id="Bid record ID", reason="Reason for removal")
async def auction_remove_bid(interaction: discord.Interaction, auction_id: int, bid_id: int, reason: str):
    if not await require_server(interaction):
        return
    if not is_moderator(interaction.user):
        await interaction.response.send_message("Moderator or auction manager permission is required.", ephemeral=True)
        return
    auction = fetch_auction(auction_id)
    if not auction or auction["guild_id"] != interaction.guild.id:
        await interaction.response.send_message("Auction not found.", ephemeral=True)
        return
    remove_bid_record(auction_id, bid_id, interaction.user.id, reason)
    await refresh_auction_message(auction_id)
    await send_log(interaction.guild.id, f"Bid **#{bid_id}** removed from auction **#{auction_id}** by {interaction.user.mention}: {reason}")
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
@app_commands.describe(auction_id="Auction number")
async def auction_staff(interaction: discord.Interaction, auction_id: int):
    if not await require_staff(interaction):
        return
    auction = fetch_auction(auction_id)
    if not auction or auction["guild_id"] != interaction.guild.id:
        await interaction.response.send_message("Auction not found.", ephemeral=True)
        return
    await interaction.response.send_message("Staff controls:", view=StaffControlsView(auction_id), ephemeral=True)


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
