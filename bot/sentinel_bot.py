# bot/sentinel_bot.py
"""
Crypto Pipeline Sentinel — Discord Bot with Slash Commands.

Provides real-time pipeline management and monitoring directly from Discord.
This is a TWO-WAY interactive bot (unlike webhooks which are one-way).

Architecture:
    Discord Server  <-->  Discord API (WebSocket)  <-->  This Python Bot
                                                           |
                                                    SQLite Database
                                                    Pipeline Scripts
                                                    Pytest Suite

Commands:
    /status     — Pipeline health overview (last run date, row counts, DB size)
    /market     — Latest market prices from the database
    /test       — Run the full pytest suite and report results
    /run        — Manually trigger the ETL pipeline
    /dbstats    — Detailed database table statistics
    /health     — Full system health check (DB, API, Docker, Alerts)
    /help_pipe  — Show all available commands with descriptions
"""

import os
import sys
import time
import subprocess
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import discord
from discord import app_commands
from dotenv import load_dotenv

from sqlalchemy import create_engine, text

# ── Resolve project root so imports and DB paths work correctly ──────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(PROJECT_ROOT)

load_dotenv(PROJECT_ROOT / ".env")

# ── Bot Configuration ────────────────────────────────────────────────────────
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN", "")
DB_PATH = PROJECT_ROOT / "crypto_pipeline.db"
VENV_PYTHON = PROJECT_ROOT / "venv" / "Scripts" / "python.exe"

# Database engine: automatically connects to Neon Cloud Postgres if configured, else SQLite
DB_CONNECTION_STRING = os.getenv("NEON_DB_URL") or os.getenv("DB_CONNECTION_STRING", f"sqlite:///{DB_PATH}")
bot_engine = create_engine(DB_CONNECTION_STRING)
is_postgres = "postgresql" in DB_CONNECTION_STRING

# ── Embed color constants ────────────────────────────────────────────────────
COLOR_SUCCESS = 0x2ECC71  # Emerald Green
COLOR_FAILURE = 0xE74C3C  # Crimson Red
COLOR_INFO    = 0x3498DB  # Sky Blue
COLOR_WARNING = 0xF1C40F  # Amber Yellow


# ══════════════════════════════════════════════════════════════════════════════
# Bot Setup
# ══════════════════════════════════════════════════════════════════════════════

intents = discord.Intents.default()
client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)


# ── Helper Functions ─────────────────────────────────────────────────────────

def query_db(sql: str, params: dict | None = None) -> list[dict]:
    """Execute a read-only SQL query via SQLAlchemy and return results as list of dicts."""
    try:
        with bot_engine.connect() as conn:
            result = conn.execute(text(sql), params or {})
            return [dict(row._mapping) for row in result.fetchall()]
    except Exception as exc:
        print(f"Database query error: {exc}")
        return []


def get_db_size_mb() -> float:
    """Get database size in megabytes (queries Postgres catalog or SQLite file)."""
    if is_postgres:
        try:
            with bot_engine.connect() as conn:
                size_bytes = conn.execute(text("SELECT pg_database_size(current_database())")).scalar()
                return float(size_bytes or 0) / (1024 * 1024)
        except Exception:
            return 0.0
    if not DB_PATH.exists():
        return 0.0
    return DB_PATH.stat().st_size / (1024 * 1024)


def format_price(value) -> str:
    """Format a number as a USD price string."""
    if value is None:
        return "N/A"
    return f"${value:,.2f}"


def format_pct(value) -> str:
    """Format a number as a percentage with sign."""
    if value is None:
        return "N/A"
    sign = "+" if value >= 0 else ""
    return f"{sign}{value:.2f}%"


# ══════════════════════════════════════════════════════════════════════════════
# Slash Commands
# ══════════════════════════════════════════════════════════════════════════════

@tree.command(name="status", description="📊 Pipeline health overview — last run, row counts, DB size")
async def cmd_status(interaction: discord.Interaction):
    """Show pipeline health overview."""
    await interaction.response.defer()

    # Get total rows
    fact_rows = query_db("SELECT COUNT(*) as cnt FROM fact_market_data")
    total_rows = fact_rows[0]["cnt"] if fact_rows else 0

    # Get date range
    date_range = query_db(
        "SELECT MIN(record_timestamp) as first_date, MAX(record_timestamp) as last_date FROM fact_market_data"
    )
    first_date = date_range[0]["first_date"] if date_range and date_range[0]["first_date"] else "N/A"
    last_date = date_range[0]["last_date"] if date_range and date_range[0]["last_date"] else "N/A"

    # Get unique days
    unique_days = query_db("SELECT COUNT(DISTINCT record_timestamp) as days FROM fact_market_data")
    days_count = unique_days[0]["days"] if unique_days else 0

    # Get symbol count
    symbols = query_db("SELECT COUNT(*) as cnt FROM dim_symbol")
    symbol_count = symbols[0]["cnt"] if symbols else 0

    db_size = get_db_size_mb()

    embed = discord.Embed(
        title="📊 Pipeline Status Report",
        description="Real-time health overview of the Crypto Data Pipeline.",
        color=COLOR_INFO,
        timestamp=datetime.now(timezone.utc),
    )
    embed.add_field(name="🗓️ Date Range", value=f"`{first_date}` → `{last_date}`", inline=False)
    embed.add_field(name="📈 Total Records", value=f"`{total_rows:,}` rows", inline=True)
    embed.add_field(name="📅 Unique Days", value=f"`{days_count}` days", inline=True)
    embed.add_field(name="🪙 Tracked Assets", value=f"`{symbol_count}` symbols", inline=True)
    embed.add_field(name="💾 Database Size", value=f"`{db_size:.3f} MB`", inline=True)
    embed.set_footer(text="Crypto Pipeline Sentinel")

    await interaction.followup.send(embed=embed)


@tree.command(name="market", description="💰 Latest market prices from the database")
async def cmd_market(interaction: discord.Interaction):
    """Show the latest market prices for each tracked asset."""
    await interaction.response.defer()

    try:
        # Get latest prices for each symbol
        rows = query_db("""
            SELECT ds.symbol_code, f.price_usd, f.change_24h, f.volume_24h,
                   f.market_cap, f.rolling_avg_7d, f.volatility_7d,
                   f.daily_return, f.record_timestamp
            FROM fact_market_data f
            JOIN dim_symbol ds ON f.symbol_id = ds.symbol_id
            WHERE f.record_timestamp = (SELECT MAX(record_timestamp) FROM fact_market_data)
            ORDER BY f.price_usd DESC
        """)

        if not rows:
            await interaction.followup.send("❌ No market data found in database.")
            return

        embed = discord.Embed(
            title="💰 Latest Market Snapshot",
            description=f"Data from: **{rows[0]['record_timestamp']}**",
            color=COLOR_SUCCESS,
            timestamp=datetime.now(timezone.utc),
        )

        for row in rows:
            symbol = row["symbol_code"].upper()
            price = format_price(row["price_usd"])
            change = format_pct(row["change_24h"])
            vol = f"${row['volume_24h']:,.0f}" if row["volume_24h"] else "N/A"
            mcap = f"${row['market_cap']:,.0f}" if row["market_cap"] else "N/A"
            avg7 = format_price(row["rolling_avg_7d"])
            volatility = format_pct(row["volatility_7d"])
            daily_ret = format_pct(row["daily_return"])

            field_value = (
                f"**Price:** `{price}`\n"
                f"**24h Change:** `{change}`\n"
                f"**Volume:** `{vol}` | **Mkt Cap:** `{mcap}`\n"
                f"**7d Avg:** `{avg7}` | **Volatility:** `{volatility}`\n"
                f"**Daily Return:** `{daily_ret}`"
            )
            embed.add_field(name=f"🪙 {symbol}", value=field_value, inline=False)

        embed.set_footer(text="Crypto Pipeline Sentinel")
        await interaction.followup.send(embed=embed)

    except Exception as e:
        await interaction.followup.send(
            embed=discord.Embed(
                title="❌ /market Error",
                description=f"```{str(e)[:500]}```",
                color=COLOR_FAILURE,
            )
        )


@tree.command(name="test", description="🧪 Run the full pytest suite and report results")
async def cmd_test(interaction: discord.Interaction):
    """Execute pytest and return results."""
    await interaction.response.defer()

    start = time.time()
    try:
        result = subprocess.run(
            [str(VENV_PYTHON), "-m", "pytest", "tests/", "-v", "--tb=short"],
            capture_output=True, text=True, timeout=120,
            cwd=str(PROJECT_ROOT),
        )
        duration = time.time() - start
        output = result.stdout[-1500:] if len(result.stdout) > 1500 else result.stdout

        # Parse pass/fail counts from pytest output
        passed = output.count(" PASSED")
        failed = output.count(" FAILED")
        errors = output.count(" ERROR")

        if result.returncode == 0:
            embed = discord.Embed(
                title="🧪 Test Suite: ALL PASSED ✅",
                description=f"**{passed}** tests passed in **{duration:.2f}s**",
                color=COLOR_SUCCESS,
            )
        else:
            embed = discord.Embed(
                title="🧪 Test Suite: FAILURES DETECTED ❌",
                description=f"**{passed}** passed, **{failed}** failed, **{errors}** errors in **{duration:.2f}s**",
                color=COLOR_FAILURE,
            )

        embed.add_field(name="📋 Output (last 1000 chars)", value=f"```\n{output[-1000:]}\n```", inline=False)
        embed.set_footer(text="Crypto Pipeline Sentinel")

    except subprocess.TimeoutExpired:
        embed = discord.Embed(
            title="🧪 Test Suite: TIMEOUT ⏰",
            description="Tests exceeded the 120-second time limit.",
            color=COLOR_WARNING,
        )

    await interaction.followup.send(embed=embed)


@tree.command(name="run", description="🚀 Manually trigger the ETL pipeline")
async def cmd_run(interaction: discord.Interaction):
    """Trigger a manual pipeline execution."""
    await interaction.response.defer()

    embed_start = discord.Embed(
        title="🚀 Pipeline Triggered",
        description="Executing: `Extract → Transform → Load → Refresh View`\nPlease wait...",
        color=COLOR_INFO,
    )
    await interaction.followup.send(embed=embed_start)

    start = time.time()
    try:
        result = subprocess.run(
            [str(VENV_PYTHON), "run_etl.py"],
            capture_output=True, text=True, timeout=300,
            cwd=str(PROJECT_ROOT),
        )
        duration = time.time() - start

        if result.returncode == 0:
            embed = discord.Embed(
                title="✅ Pipeline Execution Succeeded",
                description=f"Completed in **{duration:.2f}s**. Check your #general channel for the detailed alert.",
                color=COLOR_SUCCESS,
            )
        else:
            stderr_snippet = result.stderr[-800:] if result.stderr else "No error output captured."
            embed = discord.Embed(
                title="❌ Pipeline Execution Failed",
                description=f"Failed after **{duration:.2f}s**.",
                color=COLOR_FAILURE,
            )
            embed.add_field(name="🔍 Error Log", value=f"```\n{stderr_snippet}\n```", inline=False)

    except subprocess.TimeoutExpired:
        embed = discord.Embed(
            title="⏰ Pipeline Timeout",
            description="Pipeline exceeded the 5-minute time limit.",
            color=COLOR_WARNING,
        )

    embed.set_footer(text="Crypto Pipeline Sentinel")
    await interaction.channel.send(embed=embed)


@tree.command(name="dbstats", description="🗄️ Detailed database table statistics")
async def cmd_dbstats(interaction: discord.Interaction):
    """Show detailed statistics for each database table."""
    await interaction.response.defer()

    embed = discord.Embed(
        title="🗄️ Database Table Statistics",
        color=COLOR_INFO,
        timestamp=datetime.now(timezone.utc),
    )

    # dim_symbol
    symbols = query_db("SELECT symbol_code, asset_name FROM dim_symbol ORDER BY symbol_id")
    symbol_list = "\n".join([f"• `{s['symbol_code']}` — {s['asset_name']}" for s in symbols]) if symbols else "Empty"
    embed.add_field(name=f"📋 dim_symbol ({len(symbols)} rows)", value=symbol_list, inline=False)

    # fact_market_data per symbol
    per_symbol = query_db("""
        SELECT ds.symbol_code, COUNT(*) as rows,
               MIN(f.record_timestamp) as first, MAX(f.record_timestamp) as last
        FROM fact_market_data f
        JOIN dim_symbol ds ON f.symbol_id = ds.symbol_id
        GROUP BY ds.symbol_code
    """)
    for s in per_symbol:
        embed.add_field(
            name=f"📊 {s['symbol_code']}",
            value=f"`{s['rows']}` rows | `{s['first']}` → `{s['last']}`",
            inline=True,
        )

    # vw_weekly_trends
    weekly = query_db("SELECT COUNT(*) as cnt FROM vw_weekly_trends")
    weekly_cnt = weekly[0]["cnt"] if weekly else 0
    embed.add_field(name="📅 vw_weekly_trends", value=f"`{weekly_cnt}` aggregated rows", inline=False)

    # DB file size
    embed.add_field(name="💾 File Size", value=f"`{get_db_size_mb():.3f} MB`", inline=True)

    embed.set_footer(text="Crypto Pipeline Sentinel")
    await interaction.followup.send(embed=embed)


@tree.command(name="health", description="🏥 Full system health check — DB, API, Docker, Alerts")
async def cmd_health(interaction: discord.Interaction):
    """Comprehensive health check across all pipeline subsystems."""
    await interaction.response.defer()

    checks = []

    # Check 1: Database accessible
    try:
        rows = query_db("SELECT COUNT(*) as cnt FROM fact_market_data")
        checks.append(("💾 Database", "✅ Online", f"`{rows[0]['cnt']}` rows"))
    except Exception as e:
        checks.append(("💾 Database", "❌ Offline", str(e)[:100]))

    # Check 2: Python venv exists
    venv_ok = VENV_PYTHON.exists()
    checks.append(("🐍 Python venv", "✅ Found" if venv_ok else "❌ Missing", str(VENV_PYTHON)))

    # Check 3: .env file exists
    env_ok = (PROJECT_ROOT / ".env").exists()
    checks.append(("🔐 .env File", "✅ Present" if env_ok else "❌ Missing", "Secrets configured"))

    # Check 4: Discord Webhook configured
    webhook = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    checks.append(("📡 Webhook URL", "✅ Set" if webhook else "⚠️ Not Set", "Alerting active" if webhook else "No alerts"))

    # Check 5: CoinGecko API reachable
    try:
        import requests
        resp = requests.get("https://api.coingecko.com/api/v3/ping", timeout=5)
        if resp.status_code == 200:
            checks.append(("🌐 CoinGecko API", "✅ Reachable", "Status 200 OK"))
        else:
            checks.append(("🌐 CoinGecko API", f"⚠️ Status {resp.status_code}", "May be rate-limited"))
    except Exception as e:
        checks.append(("🌐 CoinGecko API", "❌ Unreachable", str(e)[:80]))

    # Check 6: Docker available
    try:
        docker_result = subprocess.run(["docker", "--version"], capture_output=True, text=True, timeout=5)
        if docker_result.returncode == 0:
            ver = docker_result.stdout.strip()[:50]
            checks.append(("🐳 Docker", "✅ Installed", ver))
        else:
            checks.append(("🐳 Docker", "⚠️ Issue", "Docker returned non-zero"))
    except FileNotFoundError:
        checks.append(("🐳 Docker", "⚠️ Not in PATH", "Install Docker Desktop"))
    except Exception:
        checks.append(("🐳 Docker", "⚠️ Unknown", "Could not check"))

    # Build embed
    all_ok = all("✅" in c[1] for c in checks)
    embed = discord.Embed(
        title="🏥 System Health Check" + (" — All Systems GO ✅" if all_ok else " — Issues Detected ⚠️"),
        color=COLOR_SUCCESS if all_ok else COLOR_WARNING,
        timestamp=datetime.now(timezone.utc),
    )

    for name, status, detail in checks:
        embed.add_field(name=f"{name}: {status}", value=f"```{detail}```", inline=False)

    embed.set_footer(text="Crypto Pipeline Sentinel")
    await interaction.followup.send(embed=embed)


@tree.command(name="help_pipe", description="📖 Show all available bot commands")
async def cmd_help_pipe(interaction: discord.Interaction):
    """Display help for all available commands."""
    embed = discord.Embed(
        title="📖 Crypto Pipeline Sentinel — Command Reference",
        description="All available slash commands for managing and monitoring your data pipeline.",
        color=COLOR_INFO,
    )

    commands_list = [
        ("/status", "📊 Pipeline health overview — last run date, row counts, DB size"),
        ("/market", "💰 Latest market prices for all tracked crypto assets"),
        ("/test", "🧪 Run the full pytest suite (25 tests) and see results"),
        ("/run", "🚀 Manually trigger the ETL pipeline (Extract → Transform → Load)"),
        ("/dbstats", "🗄️ Detailed database table statistics per symbol"),
        ("/health", "🏥 Full system health check — DB, API, Docker, Webhook"),
        ("/help_pipe", "📖 Show this help message"),
    ]

    for cmd, desc in commands_list:
        embed.add_field(name=f"`{cmd}`", value=desc, inline=False)

    embed.set_footer(text="Crypto Pipeline Sentinel • Built with discord.py")
    await interaction.response.send_message(embed=embed)


# ══════════════════════════════════════════════════════════════════════════════
# Bot Events
# ══════════════════════════════════════════════════════════════════════════════

@client.event
async def on_ready():
    """Sync slash commands with Discord when bot connects."""
    await tree.sync()
    print(f"{'='*60}")
    print(f"🤖 Sentinel Bot is ONLINE as: {client.user}")
    print(f"📡 Connected to {len(client.guilds)} server(s)")
    print(f"⚡ {len(tree.get_commands())} slash commands registered")
    print(f"{'='*60}")


@tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    """
    Global error handler — catches ANY unhandled exception in ANY slash command.
    This prevents the dreaded 'Bot is thinking...' infinite hang.
    When a command crashes, Discord gets a clean error embed instead of silence.
    """
    error_msg = str(error)[:500]
    embed = discord.Embed(
        title="❌ Command Error",
        description=f"An error occurred while processing your command.\n```{error_msg}```",
        color=COLOR_FAILURE,
        timestamp=datetime.now(timezone.utc),
    )
    embed.set_footer(text="Crypto Pipeline Sentinel")

    try:
        if interaction.response.is_done():
            await interaction.followup.send(embed=embed)
        else:
            await interaction.response.send_message(embed=embed, ephemeral=True)
    except Exception:
        pass  # Absolute last resort — silently fail rather than crash the bot


# ══════════════════════════════════════════════════════════════════════════════
# Entry Point
# ══════════════════════════════════════════════════════════════════════════════

def main():
    if not DISCORD_BOT_TOKEN:
        print("\n❌ Error: DISCORD_BOT_TOKEN is not set in your .env file!")
        print("Please add:  DISCORD_BOT_TOKEN=your_token_here")
        print("Get your token from: https://discord.com/developers/applications")
        sys.exit(1)

    print("🚀 Starting Crypto Pipeline Sentinel Bot...")
    client.run(DISCORD_BOT_TOKEN)


if __name__ == "__main__":
    main()
