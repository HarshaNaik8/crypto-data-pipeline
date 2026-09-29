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
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timezone
from pathlib import Path

import discord
from discord import app_commands
from dotenv import load_dotenv

from sqlalchemy import create_engine, text

# ── Lightweight HTTP health-check server for 24/7 cloud hosts (Render/Fly/Koyeb)
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status":"healthy","bot":"online","service":"crypto-sentinel-bot"}\n')

    def log_message(self, format, *args):
        pass  # Suppress HTTP access logging in console


def start_health_server():
    """Run non-blocking health check HTTP server on daemon thread."""
    port = int(os.getenv("PORT", "8080"))
    try:
        server = HTTPServer(("0.0.0.0", port), HealthCheckHandler)
        t = threading.Thread(target=server.serve_forever, daemon=True)
        t.start()
        print(f"📡 Cloud Health HTTP server online on port {port}")
    except Exception as exc:
        print(f"⚠️ Health HTTP server failed to bind port {port}: {exc}")


# ── Resolve project root so imports and DB paths work correctly ──────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent if (Path(__file__).resolve().parent.parent / "src").exists() else Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / ".env")
load_dotenv(".env")

# ── Bot Configuration ────────────────────────────────────────────────────────
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN", "")
DB_PATH = PROJECT_ROOT / "crypto_pipeline.db"
VENV_PYTHON = PROJECT_ROOT / "venv" / "Scripts" / "python.exe"
PYTHON_EXEC = str(VENV_PYTHON) if VENV_PYTHON.exists() else sys.executable

# ── Database Engine (Universal: SQLite, psycopg2, or pure-Python pg8000) ──────
import ssl
from urllib.parse import urlparse, urlunparse

def create_db_engine(connection_url: str):
    """
    Universal database engine creator.
    Supports SQLite, PostgreSQL with psycopg2, and PostgreSQL with pg8000.
    Automatically handles SSL contexts and strips incompatible driver arguments (like ?sslmode=require).
    """
    if not connection_url or "postgresql" not in connection_url:
        return create_engine(connection_url)

    is_pg8000 = False
    try:
        import psycopg2
    except ImportError:
        try:
            import pg8000
            is_pg8000 = True
        except ImportError:
            pass

    if is_pg8000:
        parsed = urlparse(connection_url)
        cleaned_url = urlunparse((
            "postgresql+pg8000",
            parsed.netloc,
            parsed.path,
            parsed.params,
            "",  # strip query parameters like sslmode, channel_binding that pg8000 rejects
            parsed.fragment
        ))
        ssl_ctx = ssl.create_default_context()
        return create_engine(cleaned_url, connect_args={"ssl_context": ssl_ctx})
    else:
        return create_engine(connection_url)

# Database engine: automatically connects to Neon Cloud Postgres if configured, else SQLite
DB_CONNECTION_STRING = os.getenv("NEON_DB_URL") or os.getenv("DB_CONNECTION_STRING", f"sqlite:///{DB_PATH}")
bot_engine = create_db_engine(DB_CONNECTION_STRING)
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

    test_dir = PROJECT_ROOT / "tests"
    if not test_dir.exists():
        test_dir = Path("tests")

    if not test_dir.exists():
        embed = discord.Embed(
            title="🧪 CI/CD Test Suite Notice",
            description="The full test suite (25/25 tests) runs automatically on **GitHub Actions CI/CD** on every git push.",
            color=COLOR_INFO,
        )
        await interaction.followup.send(embed=embed)
        return

    start = time.time()
    try:
        result = subprocess.run(
            [PYTHON_EXEC, "-m", "pytest", str(test_dir), "-v", "--tb=short"],
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

    run_file = PROJECT_ROOT / "run_etl.py"
    if not run_file.exists():
        run_file = Path("run_etl.py")

    if not run_file.exists():
        embed = discord.Embed(
            title="☁️ Cloud ETL Notice",
            description=(
                "The automated ETL pipeline runs autonomously every morning at **05:35 AM IST** on **GitHub Actions**.\n\n"
                "👉 **To trigger a cloud execution right now:**\n"
                "1. Open GitHub → **Actions**\n"
                "2. Select **Daily Scheduled ETL Pipeline**\n"
                "3. Click **Run workflow** 🚀"
            ),
            color=COLOR_INFO,
        )
        await interaction.followup.send(embed=embed)
        return

    embed_start = discord.Embed(
        title="🚀 Pipeline Triggered",
        description="Executing: `Extract → Transform → Load → Refresh View`\nPlease wait...",
        color=COLOR_INFO,
    )
    await interaction.followup.send(embed=embed_start)

    start = time.time()
    try:
        result = subprocess.run(
            [PYTHON_EXEC, str(run_file)],
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


@tree.command(name="cloud_db", description="☁️ Inspect live Neon Cloud PostgreSQL health, latency, and catalog")
async def cmd_cloud_db(interaction: discord.Interaction):
    """Deep-dive into Neon PostgreSQL health, latency, active connections, and table metrics."""
    await interaction.response.defer()

    t0 = time.time()
    try:
        with bot_engine.connect() as conn:
            pg_version = conn.execute(text("SELECT version();")).scalar()
            db_name = conn.execute(text("SELECT current_database();")).scalar()
            active_conns = conn.execute(
                text("SELECT count(*) FROM pg_stat_activity WHERE datname = current_database();")
            ).scalar()
            total_size_bytes = conn.execute(
                text("SELECT pg_database_size(current_database());")
            ).scalar() or 0

            # Count rows in each table
            fact_count = conn.execute(text("SELECT COUNT(*) FROM fact_market_data;")).scalar() or 0
            dim_count = conn.execute(text("SELECT COUNT(*) FROM dim_symbol;")).scalar() or 0
            view_count = conn.execute(text("SELECT COUNT(*) FROM vw_weekly_trends;")).scalar() or 0

        latency_ms = (time.time() - t0) * 1000
        size_mb = total_size_bytes / (1024 * 1024)

        embed = discord.Embed(
            title="☁️ Neon Cloud PostgreSQL Status",
            description="Live connection metrics and catalog telemetry from Neon AWS.",
            color=COLOR_SUCCESS,
            timestamp=datetime.now(timezone.utc),
        )
        embed.add_field(name="🔌 Cloud Status", value="`🟢 ONLINE`", inline=True)
        embed.add_field(name="⚡ Round-trip Latency", value=f"`{latency_ms:.1f} ms`", inline=True)
        embed.add_field(name="🗄️ Database Name", value=f"`{db_name}`", inline=True)
        embed.add_field(name="👥 Active Connections", value=f"`{active_conns}`", inline=True)
        embed.add_field(name="💾 Storage Used", value=f"`{size_mb:.2f} MB / 512 MB`", inline=True)
        embed.add_field(name="📊 Fact Rows", value=f"`{fact_count}` records", inline=True)
        embed.add_field(name="🪙 Dim Symbols", value=f"`{dim_count}` symbols", inline=True)
        embed.add_field(name="📈 Weekly View Rows", value=f"`{view_count}` aggregates", inline=True)
        embed.add_field(name="⚙️ Engine Version", value=f"```{pg_version[:80]}...```", inline=False)
        embed.set_footer(text="Neon Serverless PostgreSQL • AWS Singapore Region")
        await interaction.followup.send(embed=embed)

    except Exception as exc:
        latency_ms = (time.time() - t0) * 1000
        embed = discord.Embed(
            title="☁️ Neon Cloud PostgreSQL Status",
            description=f"❌ Failed to reach cloud database ({latency_ms:.1f}ms)\n```{str(exc)[:500]}```",
            color=COLOR_FAILURE,
            timestamp=datetime.now(timezone.utc),
        )
        await interaction.followup.send(embed=embed)


@tree.command(name="history", description="📈 Historical price & volatility trend for a tracked asset")
@app_commands.describe(symbol="The cryptocurrency to inspect (Bitcoin, Ethereum, or Solana)")
@app_commands.choices(symbol=[
    app_commands.Choice(name="Bitcoin (BTC)", value="BITCOIN"),
    app_commands.Choice(name="Ethereum (ETH)", value="ETHEREUM"),
    app_commands.Choice(name="Solana (SOL)", value="SOLANA"),
])
async def cmd_history(interaction: discord.Interaction, symbol: app_commands.Choice[str]):
    """Show the last 7 recorded days of data for an asset from Neon PostgreSQL."""
    await interaction.response.defer()

    sym_code = symbol.value
    rows = query_db("""
        SELECT f.record_timestamp, f.price_usd, f.daily_return, f.volatility_7d, f.volume_24h
        FROM fact_market_data f
        JOIN dim_symbol d ON f.symbol_id = d.symbol_id
        WHERE d.symbol_code = :sym
        ORDER BY f.record_timestamp DESC
        LIMIT 7
    """, {"sym": sym_code})

    if not rows:
        await interaction.followup.send(f"❌ No historical data found for `{sym_code}`.")
        return

    embed = discord.Embed(
        title=f"📈 {symbol.name} — Historical Trend",
        description=f"Showing last **{len(rows)}** recorded day(s) from Neon PostgreSQL.",
        color=COLOR_INFO,
        timestamp=datetime.now(timezone.utc),
    )

    for r in rows:
        date_str = r["record_timestamp"]
        price = format_price(r["price_usd"])
        daily_ret = format_pct(r["daily_return"])
        vol = format_pct(r["volatility_7d"])
        embed.add_field(
            name=f"📅 {date_str}",
            value=f"Price: **{price}** | Return: `{daily_ret}` | 7d Vol: `{vol}`",
            inline=False,
        )

    embed.set_footer(text="Crypto Pipeline Sentinel • Neon PostgreSQL")
    await interaction.followup.send(embed=embed)


@tree.command(name="verify_etl", description="🔍 Verify if today's scheduled ETL execution is completed")
async def cmd_verify_etl(interaction: discord.Interaction):
    """Check whether today's ETL execution has run and loaded data."""
    await interaction.response.defer()

    today_str = datetime.now().strftime("%Y-%m-%d")
    rows = query_db("""
        SELECT d.symbol_code, f.price_usd, f.record_timestamp
        FROM fact_market_data f
        JOIN dim_symbol d ON f.symbol_id = d.symbol_id
        WHERE f.record_timestamp = :today
        ORDER BY d.symbol_code
    """, {"today": today_str})

    if rows:
        embed = discord.Embed(
            title="✅ Today's ETL Execution Verified",
            description=f"All records for today (**{today_str}**) are safely written in Neon PostgreSQL!",
            color=COLOR_SUCCESS,
            timestamp=datetime.now(timezone.utc),
        )
        for r in rows:
            embed.add_field(
                name=f"🪙 {r['symbol_code']}",
                value=f"Price: `{format_price(r['price_usd'])}`",
                inline=True,
            )
        embed.add_field(name="⏰ Next Scheduled Run", value="`Tomorrow at 05:35 AM IST` (00:05 UTC)", inline=False)
    else:
        last_date_row = query_db("SELECT MAX(record_timestamp) as last_date FROM fact_market_data")
        last_date = last_date_row[0]["last_date"] if last_date_row and last_date_row[0]["last_date"] else "None"
        embed = discord.Embed(
            title="⏳ Today's ETL Pending",
            description=f"No rows recorded for today (**{today_str}**) yet.\nLatest available data in DB is from: **{last_date}**.",
            color=COLOR_WARNING,
            timestamp=datetime.now(timezone.utc),
        )
        embed.add_field(name="⏰ Next Cloud Run", value="Scheduled for `05:35 AM IST` via GitHub Actions", inline=False)
        embed.add_field(name="💡 Manual Run", value="You can run `/run` right here or trigger GitHub Actions!", inline=False)

    embed.set_footer(text="Crypto Pipeline Sentinel")
    await interaction.followup.send(embed=embed)


@tree.command(name="help_pipe", description="📖 Show all available bot commands")
async def cmd_help_pipe(interaction: discord.Interaction):
    """Display help for all available commands."""
    embed = discord.Embed(
        title="📖 Crypto Pipeline Sentinel — Command Reference",
        description="All 10 available slash commands for monitoring and managing your cloud pipeline.",
        color=COLOR_INFO,
    )

    commands_list = [
        ("/status", "📊 Pipeline health overview — date range, row counts, DB size"),
        ("/market", "💰 Latest market prices for all tracked crypto assets"),
        ("/history", "📈 Historical price & volatility trends for a chosen asset"),
        ("/cloud_db", "☁️ Deep-dive into Neon PostgreSQL health, latency, & catalog"),
        ("/verify_etl", "🔍 Verify whether today's scheduled ETL execution has completed"),
        ("/test", "🧪 Run the full pytest suite (25 tests) and see results"),
        ("/run", "🚀 Manually trigger the ETL pipeline (Extract → Transform → Load)"),
        ("/dbstats", "🗄️ Detailed database table statistics per symbol"),
        ("/health", "🏥 Full system health check — DB, API, Docker, Webhook"),
        ("/help_pipe", "📖 Show this help message"),
    ]

    for cmd, desc in commands_list:
        embed.add_field(name=f"`{cmd}`", value=desc, inline=False)

    embed.set_footer(text="Crypto Pipeline Sentinel • Built with discord.py & Neon PostgreSQL")
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

    start_health_server()
    print("🚀 Starting Crypto Pipeline Sentinel Bot...")
    client.run(DISCORD_BOT_TOKEN)


if __name__ == "__main__":
    main()
