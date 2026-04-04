"""
DeFi Executor — Phase 3
Executes live swaps on Polygon via 1inch aggregator.

Features:
  - Web3 transaction signing + broadcast
  - 1inch v6 for optimal routing
  - SQLite trade log with cost basis tracking
  - Prometheus /metrics endpoint (port 8091)
  - Post-trade email notification
  - Safety rails: gas limits, daily spend cap, position sizing
  - Falls back gracefully to SIMULATION_MODE if env vars are missing

Set SIMULATION_MODE=false in .env to enable live trading.
NEVER commit your WALLET_PRIVATE_KEY to git.
"""

import os, json, time, hmac, hashlib, logging, sqlite3, smtplib, threading
import requests, redis, yaml
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from flask import Flask, Response

# ── Try web3 import ──────────────────────────────────────────────────────────
try:
    from web3 import Web3
    from web3.middleware import ExtraDataToPOAMiddleware
    WEB3_AVAILABLE = True
except ImportError:
    WEB3_AVAILABLE = False
    Web3 = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── Environment ──────────────────────────────────────────────────────────────
SIMULATION_MODE   = os.environ.get("SIMULATION_MODE", "true").lower() == "true"
WALLET_ADDRESS    = os.environ.get("WALLET_ADDRESS", "")
_PRIVATE_KEY      = os.environ.get("WALLET_PRIVATE_KEY", "")   # never logged
POLYGON_RPC_URL   = os.environ.get("POLYGON_RPC_URL", "")
ONEINCH_API_KEY   = os.environ.get("ONEINCH_API_KEY", "")
REDIS_URL         = os.environ.get("REDIS_URL", "redis://redis:6379")
SMTP_HOST         = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT         = int(os.environ.get("SMTP_PORT", 587))
SMTP_USER         = os.environ.get("SMTP_USER", "")
SMTP_PASSWORD     = os.environ.get("SMTP_PASSWORD", "")
NOTIFY_RECIPIENT  = os.environ.get("DIGEST_RECIPIENT", SMTP_USER)
CHAIN_ID          = 137   # Polygon

# ── Paths ────────────────────────────────────────────────────────────────────
RISK_CONFIG     = Path("/app/config/risk_config.yaml")
STRATEGY_CONFIG = Path("/app/config/strategy_config.yaml")
DB_PATH         = Path("/app/data/trades.db")

# ── 1inch ────────────────────────────────────────────────────────────────────
ONEINCH_BASE   = f"https://api.1inch.dev/swap/v6.0/{CHAIN_ID}"
ONEINCH_ROUTER = "0x111111125421cA6dc452d289314280a0f8842A65"

# ── Token registry — Polygon mainnet addresses ───────────────────────────────
# All lowercase; normalise on use. Decimals included for amount conversion.
TOKENS = {
    "USDC":  {"address": "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359", "decimals": 6},
    "USDCE": {"address": "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174", "decimals": 6},  # bridged fallback
    "WETH":  {"address": "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619", "decimals": 18},
    "ETH":   {"address": "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619", "decimals": 18},
    "WBTC":  {"address": "0x1BFD67037B42Cf73acF2047067bd4F2C47D9BfD6", "decimals": 8},
    "BTC":   {"address": "0x1BFD67037B42Cf73acF2047067bd4F2C47D9BfD6", "decimals": 8},
    "WMATIC":{"address": "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270", "decimals": 18},
    "MATIC": {"address": "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270", "decimals": 18},
    "POL":   {"address": "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270", "decimals": 18},
    "LINK":  {"address": "0x53E0bca35eC356BD5ddDFebbD1Fc0fD03FaBad39", "decimals": 18},
    "AAVE":  {"address": "0xD6DF932A45C0f255f85145f286eA0b292B21C90B", "decimals": 18},
    "UNI":   {"address": "0xb33EaAd8d922B1083446DC23f610c2567fB5180f", "decimals": 18},
    "CRV":   {"address": "0x172370d5Cd63279eFa6d502DAB29171933a610AF", "decimals": 18},
    "BAL":   {"address": "0x9a71012B13CA4d3D0Cdc72A177DF3ef03b0E76A7", "decimals": 18},
    "SAND":  {"address": "0xBbba073C31bF03b8ACf7c28EF0738DeCF3695683", "decimals": 18},
    # ARB and OP are not on Polygon — omitted intentionally
}

ERC20_ABI = [
    {"inputs": [{"name": "spender", "type": "address"}, {"name": "amount", "type": "uint256"}],
     "name": "approve", "outputs": [{"name": "", "type": "bool"}], "type": "function"},
    {"inputs": [{"name": "owner", "type": "address"}, {"name": "spender", "type": "address"}],
     "name": "allowance", "outputs": [{"name": "", "type": "uint256"}], "type": "function"},
    {"inputs": [{"name": "account", "type": "address"}],
     "name": "balanceOf", "outputs": [{"name": "", "type": "uint256"}], "type": "function"},
]

# ── Redis ────────────────────────────────────────────────────────────────────
r = redis.from_url(REDIS_URL, decode_responses=True)

# ── Metrics state (updated by executor, read by /metrics endpoint) ───────────
_metrics: dict = {}
_metrics_lock = threading.Lock()


# ════════════════════════════════════════════════════════════════════════════
#  DATABASE
# ════════════════════════════════════════════════════════════════════════════

def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp           TEXT    NOT NULL,
            symbol              TEXT    NOT NULL,
            side                TEXT    NOT NULL,
            amount_usd          REAL    NOT NULL,
            token_amount        REAL    NOT NULL,
            price_usd           REAL    NOT NULL,
            gas_cost_usd        REAL    DEFAULT 0,
            tx_hash             TEXT,
            status              TEXT    DEFAULT 'pending',
            signal_source       TEXT,
            signal_confidence   REAL,
            signal_strength     REAL,
            cost_basis_usd      REAL,
            realized_pnl_usd    REAL,
            chain               TEXT    DEFAULT 'polygon'
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS positions (
            symbol              TEXT    PRIMARY KEY,
            token_amount        REAL    NOT NULL DEFAULT 0,
            avg_cost_usd        REAL    NOT NULL DEFAULT 0,
            total_invested_usd  REAL    NOT NULL DEFAULT 0,
            last_updated        TEXT    NOT NULL
        )
    """)
    # Tax lot table — tracks individual purchase lots for FIFO cost basis and tax reporting
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tax_lots (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol              TEXT    NOT NULL,
            quantity            REAL    NOT NULL,
            cost_basis_usd      REAL    NOT NULL,
            purchase_date       TEXT    NOT NULL,
            source              TEXT    DEFAULT 'executor',
            closed_at           TEXT,
            sale_price_usd      REAL,
            realized_gain_usd   REAL
        )
    """)
    conn.commit()
    conn.close()
    logger.info(f"Trade database initialised at {DB_PATH}")


def log_trade(trade: dict) -> int:
    """Insert trade record. Returns row id."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.execute("""
        INSERT INTO trades
            (timestamp, symbol, side, amount_usd, token_amount, price_usd,
             gas_cost_usd, tx_hash, status, signal_source, signal_confidence,
             signal_strength, cost_basis_usd, realized_pnl_usd, chain)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        trade["timestamp"], trade["symbol"], trade["side"],
        trade["amount_usd"], trade["token_amount"], trade["price_usd"],
        trade.get("gas_cost_usd", 0), trade.get("tx_hash"),
        trade.get("status", "confirmed"), trade.get("signal_source"),
        trade.get("signal_confidence"), trade.get("signal_strength"),
        trade.get("cost_basis_usd"), trade.get("realized_pnl_usd"),
        trade.get("chain", "polygon"),
    ))
    row_id = cur.lastrowid
    conn.commit()
    conn.close()
    return row_id


def update_position(symbol: str, side: str, token_amount: float, price_usd: float) -> dict:
    """Update position table and return updated position with cost basis."""
    conn = sqlite3.connect(DB_PATH)
    now = datetime.now(timezone.utc).isoformat()

    row = conn.execute("SELECT * FROM positions WHERE symbol=?", (symbol,)).fetchone()

    if side == "buy":
        if row:
            existing_qty   = row[1]
            existing_cost  = row[2]
            existing_total = row[3]
            new_qty   = existing_qty + token_amount
            new_total = existing_total + (token_amount * price_usd)
            new_avg   = new_total / new_qty if new_qty else price_usd
            conn.execute("""
                UPDATE positions SET token_amount=?, avg_cost_usd=?,
                total_invested_usd=?, last_updated=? WHERE symbol=?
            """, (new_qty, new_avg, new_total, now, symbol))
        else:
            conn.execute("""
                INSERT INTO positions (symbol, token_amount, avg_cost_usd,
                total_invested_usd, last_updated)
                VALUES (?,?,?,?,?)
            """, (symbol, token_amount, price_usd, token_amount * price_usd, now))
        position = {"avg_cost_usd": price_usd if not row else new_avg}

    else:  # sell
        if row:
            avg_cost = row[2]
            new_qty  = max(0, row[1] - token_amount)
            new_total = new_qty * avg_cost
            conn.execute("""
                UPDATE positions SET token_amount=?, total_invested_usd=?,
                last_updated=? WHERE symbol=?
            """, (new_qty, new_total, now, symbol))
            position = {"avg_cost_usd": avg_cost}
        else:
            position = {"avg_cost_usd": price_usd}

    conn.commit()
    conn.close()
    return position


def get_all_positions() -> list[dict]:
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("SELECT * FROM positions WHERE token_amount > 0").fetchall()
    conn.close()
    return [{"symbol": r[0], "token_amount": r[1], "avg_cost_usd": r[2],
             "total_invested_usd": r[3], "last_updated": r[4]} for r in rows]


def get_trade_stats() -> dict:
    conn = sqlite3.connect(DB_PATH)
    total     = conn.execute("SELECT COUNT(*) FROM trades WHERE status='confirmed'").fetchone()[0]
    wins      = conn.execute("SELECT COUNT(*) FROM trades WHERE side='sell' AND realized_pnl_usd > 0").fetchone()[0]
    losses    = conn.execute("SELECT COUNT(*) FROM trades WHERE side='sell' AND realized_pnl_usd <= 0").fetchone()[0]
    total_pnl = conn.execute("SELECT COALESCE(SUM(realized_pnl_usd),0) FROM trades WHERE side='sell'").fetchone()[0]
    gas_total = conn.execute("SELECT COALESCE(SUM(gas_cost_usd),0) FROM trades").fetchone()[0]
    conn.close()
    return {"total_trades": total, "wins": wins, "losses": losses,
            "total_pnl_usd": total_pnl, "gas_spent_usd": gas_total}


def record_tax_lot_buy(symbol: str, quantity: float, cost_basis_usd: float,
                       purchase_date: str, source: str = "executor"):
    """Record a new tax lot when a buy trade executes."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        INSERT INTO tax_lots (symbol, quantity, cost_basis_usd, purchase_date, source)
        VALUES (?, ?, ?, ?, ?)
    """, (symbol, quantity, cost_basis_usd, purchase_date, source))
    conn.commit()
    conn.close()


def close_tax_lot_fifo(symbol: str, quantity_sold: float, sale_price_usd: float,
                       sale_date: str) -> float:
    """
    Close tax lots FIFO when a sell occurs. Returns total realized gain/loss.
    """
    conn = sqlite3.connect(DB_PATH)
    open_lots = conn.execute("""
        SELECT id, quantity, cost_basis_usd FROM tax_lots
        WHERE symbol = ? AND closed_at IS NULL
        ORDER BY purchase_date ASC
    """, (symbol,)).fetchall()

    remaining = quantity_sold
    total_gain = 0.0

    for lot_id, lot_qty, lot_cost in open_lots:
        if remaining <= 0:
            break
        used = min(remaining, lot_qty)
        gain = (sale_price_usd - lot_cost) * used
        total_gain += gain
        remaining -= used

        if used >= lot_qty:
            conn.execute("""
                UPDATE tax_lots SET closed_at=?, sale_price_usd=?, realized_gain_usd=?
                WHERE id=?
            """, (sale_date, sale_price_usd, gain, lot_id))
        else:
            # Partial close: split the lot
            conn.execute("UPDATE tax_lots SET quantity=? WHERE id=?",
                         (lot_qty - used, lot_id))
            conn.execute("""
                INSERT INTO tax_lots (symbol, quantity, cost_basis_usd, purchase_date,
                source, closed_at, sale_price_usd, realized_gain_usd)
                VALUES (?, ?, ?, (SELECT purchase_date FROM tax_lots WHERE id=?),
                (SELECT source FROM tax_lots WHERE id=?), ?, ?, ?)
            """, (symbol, used, lot_cost, lot_id, lot_id, sale_date, sale_price_usd, gain))

    conn.commit()
    conn.close()
    return total_gain


def get_open_tax_lots() -> list[dict]:
    """Return all open tax lots with holding period info for Grafana."""
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("""
        SELECT id, symbol, quantity, cost_basis_usd, purchase_date, source
        FROM tax_lots WHERE closed_at IS NULL ORDER BY symbol, purchase_date
    """).fetchall()
    conn.close()

    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    lots = []
    for row in rows:
        lot_id, symbol, qty, cost, purchase_date, source = row
        try:
            pd = datetime.fromisoformat(purchase_date.replace("Z", "+00:00"))
            if pd.tzinfo is None:
                pd = pd.replace(tzinfo=timezone.utc)
            holding_days = (now - pd).days
        except Exception:
            holding_days = 0
        lots.append({
            "id": lot_id,
            "symbol": symbol,
            "quantity": qty,
            "cost_basis_usd": cost,
            "purchase_date": purchase_date,
            "source": source,
            "holding_days": holding_days,
            "long_term": holding_days >= 365,
        })
    return lots


# ════════════════════════════════════════════════════════════════════════════
#  WEB3 + CHAIN
# ════════════════════════════════════════════════════════════════════════════

def get_w3() -> Optional["Web3"]:
    if not WEB3_AVAILABLE or not POLYGON_RPC_URL:
        return None
    w3 = Web3(Web3.HTTPProvider(POLYGON_RPC_URL))
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
    return w3 if w3.is_connected() else None


def get_usdc_balance(w3) -> float:
    """Return USDC balance of the wallet in USD (6 decimals)."""
    try:
        token_info = TOKENS["USDC"]
        contract   = w3.eth.contract(
            address=Web3.to_checksum_address(token_info["address"]),
            abi=ERC20_ABI,
        )
        raw = contract.functions.balanceOf(
            Web3.to_checksum_address(WALLET_ADDRESS)
        ).call()
        return raw / (10 ** token_info["decimals"])
    except Exception as e:
        logger.error(f"Could not fetch USDC balance: {e}")
        return 0.0


def get_matic_balance_usd(w3) -> float:
    """Return MATIC balance in USD (for gas monitoring)."""
    try:
        wei = w3.eth.get_balance(Web3.to_checksum_address(WALLET_ADDRESS))
        matic = wei / 1e18
        # Rough MATIC price from Redis if available
        raw = r.get("claude:signals:MATIC")
        price = 0.8  # fallback
        if raw:
            price = json.loads(raw).get("price_usd", 0.8) or 0.8
        return matic * price
    except Exception:
        return 0.0


def ensure_approval(w3, token_symbol: str, amount_wei: int) -> bool:
    """Ensure 1inch router is approved to spend the token. Returns True if ok."""
    try:
        token = TOKENS[token_symbol]
        contract = w3.eth.contract(
            address=Web3.to_checksum_address(token["address"]),
            abi=ERC20_ABI,
        )
        allowance = contract.functions.allowance(
            Web3.to_checksum_address(WALLET_ADDRESS),
            Web3.to_checksum_address(ONEINCH_ROUTER),
        ).call()

        if allowance >= amount_wei:
            return True

        logger.info(f"Approving 1inch router to spend {token_symbol}...")
        MAX_UINT256 = 2**256 - 1
        tx = contract.functions.approve(
            Web3.to_checksum_address(ONEINCH_ROUTER),
            MAX_UINT256,
        ).build_transaction({
            "from":  Web3.to_checksum_address(WALLET_ADDRESS),
            "nonce": w3.eth.get_transaction_count(Web3.to_checksum_address(WALLET_ADDRESS)),
            "chainId": CHAIN_ID,
        })
        signed = w3.eth.account.sign_transaction(tx, _PRIVATE_KEY)
        tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
        if receipt["status"] == 1:
            logger.info(f"{token_symbol} approval confirmed: {tx_hash.hex()}")
            return True
        else:
            logger.error(f"{token_symbol} approval failed")
            return False
    except Exception as e:
        logger.error(f"Approval error for {token_symbol}: {e}")
        return False


# ════════════════════════════════════════════════════════════════════════════
#  1INCH INTEGRATION
# ════════════════════════════════════════════════════════════════════════════

def oneinch_quote(src_symbol: str, dst_symbol: str, amount_usd: float) -> Optional[dict]:
    """Get a price quote from 1inch. Returns dict with dst_amount and price_impact."""
    if src_symbol not in TOKENS or dst_symbol not in TOKENS:
        logger.warning(f"Token not on Polygon: {src_symbol} or {dst_symbol}")
        return None

    src   = TOKENS[src_symbol]
    dst   = TOKENS[dst_symbol]
    # Convert USD amount to source token wei
    # For USDC (src on buys): 1 USD = 1 USDC = 1_000_000 wei
    src_amount_wei = int(amount_usd * (10 ** src["decimals"]))

    headers = {"Authorization": f"Bearer {ONEINCH_API_KEY}", "Accept": "application/json"}
    params  = {
        "src":    src["address"],
        "dst":    dst["address"],
        "amount": str(src_amount_wei),
    }
    try:
        resp = requests.get(f"{ONEINCH_BASE}/quote", params=params, headers=headers, timeout=10)
        if resp.status_code != 200:
            logger.error(f"1inch quote failed {resp.status_code}: {resp.text[:200]}")
            return None
        data = resp.json()
        dst_amount  = int(data["dstAmount"]) / (10 ** dst["decimals"])
        price_usd   = amount_usd / dst_amount if dst_amount else 0
        return {
            "src_amount_usd":  amount_usd,
            "src_amount_wei":  src_amount_wei,
            "dst_amount":      dst_amount,
            "dst_amount_wei":  int(data["dstAmount"]),
            "price_usd":       price_usd,
            "gas_estimate":    data.get("gas", 200000),
        }
    except Exception as e:
        logger.error(f"1inch quote error: {e}")
        return None


def oneinch_swap(w3, src_symbol: str, dst_symbol: str,
                 src_amount_wei: int, slippage: float = 1.0) -> Optional[dict]:
    """Build, sign, and broadcast a swap. Returns tx details or None on failure."""
    src = TOKENS[src_symbol]
    dst = TOKENS[dst_symbol]

    headers = {"Authorization": f"Bearer {ONEINCH_API_KEY}", "Accept": "application/json"}
    params  = {
        "src":              src["address"],
        "dst":              dst["address"],
        "amount":           str(src_amount_wei),
        "from":             WALLET_ADDRESS,
        "slippage":         slippage,
        "disableEstimate":  "false",
        "allowPartialFill": "false",
    }
    try:
        resp = requests.get(f"{ONEINCH_BASE}/swap", params=params, headers=headers, timeout=15)
        if resp.status_code != 200:
            logger.error(f"1inch swap build failed {resp.status_code}: {resp.text[:300]}")
            return None

        swap_data = resp.json()
        tx_raw    = swap_data["tx"]

        # Get current gas price (add 10% buffer for faster inclusion)
        gas_price = int(w3.eth.gas_price * 1.1)

        tx = {
            "to":       Web3.to_checksum_address(tx_raw["to"]),
            "data":     tx_raw["data"],
            "value":    int(tx_raw["value"]),
            "gas":      int(tx_raw["gas"]),
            "gasPrice": gas_price,
            "nonce":    w3.eth.get_transaction_count(Web3.to_checksum_address(WALLET_ADDRESS)),
            "chainId":  CHAIN_ID,
        }

        signed   = w3.eth.account.sign_transaction(tx, _PRIVATE_KEY)
        tx_hash  = w3.eth.send_raw_transaction(signed.raw_transaction)
        logger.info(f"Swap broadcast: {tx_hash.hex()}")

        receipt  = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
        gas_used = receipt["gasUsed"] * gas_price / 1e18   # in MATIC
        # Rough MATIC → USD (use saved price or 0.8 fallback)
        matic_px  = 0.8
        raw = r.get("claude:signals:MATIC")
        if raw:
            matic_px = json.loads(raw).get("price_usd", 0.8) or 0.8
        gas_usd   = gas_used * matic_px

        dst_amount = int(swap_data.get("dstAmount", "0")) / (10 ** dst["decimals"])

        return {
            "tx_hash":      tx_hash.hex(),
            "status":       "confirmed" if receipt["status"] == 1 else "failed",
            "dst_amount":   dst_amount,
            "gas_cost_usd": round(gas_usd, 4),
        }
    except Exception as e:
        logger.error(f"1inch swap execution error: {e}")
        return None


# ════════════════════════════════════════════════════════════════════════════
#  RISK + SIGNAL EVALUATION
# ════════════════════════════════════════════════════════════════════════════

def load_risk_config() -> dict:
    try:
        with open(RISK_CONFIG) as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        logger.error(f"Could not load risk config: {e}")
        return {}


def check_asset_allowed(symbol: str) -> tuple[bool, str]:
    try:
        resp = requests.get("http://asset_governor:8090/check",
                            params={"symbol": symbol}, timeout=5)
        data = resp.json()
        return data.get("allowed", False), data.get("reason", "unknown")
    except Exception as e:
        logger.error(f"Asset Governor check failed: {e}")
        return False, "governor_unavailable"


def check_daily_spend_limit(risk: dict) -> bool:
    """Block trading if daily circuit breaker has been tripped."""
    if r.get("system:trading_paused") == "true":
        logger.warning("Trading is paused (circuit breaker). Skipping execution.")
        return False
    return True


def evaluate_signal(symbol: str, claude_signal: dict,
                    momentum_signal: dict, risk: dict) -> Optional[dict]:
    """Return a trade decision dict or None if thresholds not met."""
    try:
        with open(STRATEGY_CONFIG) as f:
            cfg = yaml.safe_load(f) or {}
        strategy_cfg = cfg.get("strategies", {}).get("claude_alpha", {})
    except Exception:
        strategy_cfg = {}

    min_confidence = strategy_cfg.get("min_confidence", 0.70)
    min_strength   = strategy_cfg.get("min_signal_strength", 0.40)
    require_confirm= strategy_cfg.get("require_technical_confirmation", True)

    cs     = claude_signal.get("signal_strength", 0)
    cc     = claude_signal.get("confidence", 0)
    action = claude_signal.get("suggested_action", "hold")

    if action in ("hold", "avoid"):
        return None
    if cc < min_confidence:
        return None
    if abs(cs) < min_strength:
        return None

    if require_confirm and momentum_signal:
        m_enter = momentum_signal.get("enter_long", 0)
        m_exit  = momentum_signal.get("exit_long", 0)
        if cs > 0 and not m_enter:
            return None
        if cs < 0 and not m_exit:
            return None

    max_alloc = risk.get("position_limits", {}).get("max_per_asset", 0.15)
    max_gas   = risk.get("defi_specific", {}).get("max_gas_per_trade_usd", 5)
    min_size  = risk.get("defi_specific", {}).get("min_trade_size_usd", 25)

    return {
        "symbol":       symbol,
        "action":       action,
        "side":         "buy" if action in ("buy", "accumulate") else "sell",
        "signal_strength": cs,
        "confidence":   cc,
        "max_allocation": max_alloc,
        "max_gas_usd":  max_gas,
        "min_size_usd": min_size,
        "reasoning":    claude_signal.get("reasoning", ""),
        "decided_at":   datetime.now(timezone.utc).isoformat(),
    }


# ════════════════════════════════════════════════════════════════════════════
#  EXECUTION
# ════════════════════════════════════════════════════════════════════════════

def compute_trade_size(usdc_balance: float, decision: dict) -> float:
    """Calculate how many USD to trade, respecting position limits."""
    max_pct  = decision["max_allocation"]       # e.g. 0.15
    min_size = decision["min_size_usd"]         # e.g. 25
    max_size = usdc_balance * max_pct
    # Never more than 15% of balance or what's available
    size = min(max_size, usdc_balance * 0.95)   # keep 5% reserve for gas
    if size < min_size:
        logger.info(f"Trade size ${size:.2f} below minimum ${min_size}. Skipping.")
        return 0.0
    return round(size, 2)


def execute_live_trade(decision: dict, w3) -> Optional[dict]:
    """Execute a live swap. Returns completed trade record or None."""
    symbol  = decision["symbol"]
    side    = decision["side"]

    if symbol not in TOKENS:
        logger.warning(f"{symbol} not in Polygon token registry. Skipping.")
        return None

    usdc_balance = get_usdc_balance(w3)
    logger.info(f"USDC balance: ${usdc_balance:.2f}")

    if side == "buy":
        trade_size = compute_trade_size(usdc_balance, decision)
        if trade_size == 0:
            return None

        # Validate gas
        quote = oneinch_quote("USDC", symbol, trade_size)
        if not quote:
            return None
        # Rough gas estimate check
        matic_px = 0.8
        raw = r.get("claude:signals:MATIC")
        if raw:
            matic_px = json.loads(raw).get("price_usd", 0.8) or 0.8
        est_gas_usd = (quote["gas_estimate"] * 50e9 / 1e18) * matic_px  # 50 gwei estimate
        if est_gas_usd > decision["max_gas_usd"]:
            logger.warning(f"Estimated gas ${est_gas_usd:.3f} exceeds limit ${decision['max_gas_usd']}. Skipping.")
            return None

        # Approve USDC for 1inch router
        if not ensure_approval(w3, "USDC", quote["src_amount_wei"]):
            return None

        result = oneinch_swap(w3, "USDC", symbol, quote["src_amount_wei"])
        if not result or result["status"] != "confirmed":
            return None

        price_usd   = trade_size / result["dst_amount"] if result["dst_amount"] else 0
        position    = update_position(symbol, "buy", result["dst_amount"], price_usd)
        trade = {
            "timestamp":         datetime.now(timezone.utc).isoformat(),
            "symbol":            symbol,
            "side":              "buy",
            "amount_usd":        trade_size,
            "token_amount":      result["dst_amount"],
            "price_usd":         price_usd,
            "gas_cost_usd":      result["gas_cost_usd"],
            "tx_hash":           result["tx_hash"],
            "status":            "confirmed",
            "signal_source":     "claude_analyst",
            "signal_confidence": decision["confidence"],
            "signal_strength":   decision["signal_strength"],
            "cost_basis_usd":    price_usd,
            "realized_pnl_usd":  None,
        }

    else:  # sell
        positions = {p["symbol"]: p for p in get_all_positions()}
        if symbol not in positions:
            logger.info(f"No position in {symbol} to sell.")
            return None
        pos         = positions[symbol]
        token_qty   = pos["token_amount"]
        src_amt_wei = int(token_qty * (10 ** TOKENS[symbol]["decimals"]))

        if not ensure_approval(w3, symbol, src_amt_wei):
            return None

        result = oneinch_swap(w3, symbol, "USDC", src_amt_wei)
        if not result or result["status"] != "confirmed":
            return None

        cost_basis    = pos["avg_cost_usd"]
        current_price = result["dst_amount"] / token_qty if token_qty else 0
        realized_pnl  = (current_price - cost_basis) * token_qty
        update_position(symbol, "sell", token_qty, current_price)
        trade = {
            "timestamp":         datetime.now(timezone.utc).isoformat(),
            "symbol":            symbol,
            "side":              "sell",
            "amount_usd":        result["dst_amount"],
            "token_amount":      token_qty,
            "price_usd":         current_price,
            "gas_cost_usd":      result["gas_cost_usd"],
            "tx_hash":           result["tx_hash"],
            "status":            "confirmed",
            "signal_source":     "claude_analyst",
            "signal_confidence": decision["confidence"],
            "signal_strength":   decision["signal_strength"],
            "cost_basis_usd":    cost_basis,
            "realized_pnl_usd":  round(realized_pnl, 4),
        }

    trade_id = log_trade(trade)
    trade["id"] = trade_id
    # Also push to Redis for digest/dashboard
    r.lpush("executor:live_trades", json.dumps(trade))
    r.ltrim("executor:live_trades", 0, 499)
    update_metrics()
    send_trade_notification(trade)
    return trade


def simulate_execution(decision: dict):
    """Log what WOULD happen. No chain interaction."""
    log_entry = {
        "mode":      "SIMULATION",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "decision":  decision,
        "note":      "Set SIMULATION_MODE=false to execute on-chain.",
    }
    logger.info(f"[SIM] {decision['symbol']} {decision['side'].upper()} "
                f"(conf={decision['confidence']:.0%} str={decision['signal_strength']:+.2f})")
    r.lpush("executor:simulated_trades", json.dumps(log_entry))
    r.ltrim("executor:simulated_trades", 0, 499)


# ════════════════════════════════════════════════════════════════════════════
#  NOTIFICATIONS
# ════════════════════════════════════════════════════════════════════════════

def send_trade_notification(trade: dict):
    """Send immediate post-trade email."""
    if not SMTP_USER or not SMTP_PASSWORD:
        logger.warning("SMTP not configured — skipping trade notification email.")
        return
    try:
        symbol = trade["symbol"]
        side   = trade["side"].upper()
        pnl    = trade.get("realized_pnl_usd")
        pnl_str = f"<br><strong>Realized P&L:</strong> ${pnl:+.2f}" if pnl is not None else ""
        subject = f"[Crypto] Trade Executed: {side} {symbol}"
        html = f"""
<h2>{'🟢 BUY' if side=='BUY' else '🔴 SELL'} — {symbol}</h2>
<p>
  <strong>Amount:</strong> ${trade['amount_usd']:.2f}<br>
  <strong>Price:</strong> ${trade['price_usd']:.4f}<br>
  <strong>Tokens:</strong> {trade['token_amount']:.6f} {symbol}<br>
  <strong>Gas cost:</strong> ${trade['gas_cost_usd']:.4f}{pnl_str}<br>
  <strong>Tx:</strong> <a href="https://polygonscan.com/tx/{trade['tx_hash']}">{trade['tx_hash'][:18]}...</a><br>
  <strong>Signal confidence:</strong> {trade['signal_confidence']:.0%}<br>
  <strong>Time:</strong> {trade['timestamp']}
</p>
<p><em>Chain: Polygon | Mode: LIVE</em></p>
"""
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = SMTP_USER
        msg["To"]      = NOTIFY_RECIPIENT
        msg.attach(MIMEText(html, "html"))
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_USER, NOTIFY_RECIPIENT, msg.as_string())
        logger.info(f"Trade notification sent for {side} {symbol}")
    except Exception as e:
        logger.error(f"Failed to send trade notification: {e}")


# ════════════════════════════════════════════════════════════════════════════
#  PROMETHEUS METRICS
# ════════════════════════════════════════════════════════════════════════════

def update_metrics():
    """Refresh in-memory metrics dict from DB."""
    stats     = get_trade_stats()
    positions = get_all_positions()
    with _metrics_lock:
        _metrics["stats"]     = stats
        _metrics["positions"] = positions
        _metrics["updated"]   = time.time()


def render_prometheus_metrics() -> str:
    with _metrics_lock:
        stats = _metrics.get("stats", {})
        positions = _metrics.get("positions", [])

    lines = []
    def g(name, value, labels="", help_text="", mtype="gauge"):
        lbl = f"{{{labels}}}" if labels else ""
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} {mtype}")
        lines.append(f"{name}{lbl} {value}")

    g("crypto_trades_total",    stats.get("total_trades", 0),
      help_text="Total confirmed trades", mtype="counter")
    g("crypto_winning_trades",  stats.get("wins", 0),
      help_text="Number of profitable sell trades")
    g("crypto_losing_trades",   stats.get("losses", 0),
      help_text="Number of unprofitable sell trades")
    g("crypto_realized_pnl_usd_total", round(stats.get("total_pnl_usd", 0), 4),
      help_text="Total realized P&L in USD")
    g("crypto_gas_spent_usd_total", round(stats.get("gas_spent_usd", 0), 4),
      help_text="Total gas spent in USD", mtype="counter")

    lines.append("# HELP crypto_position_value_usd Current position size in tokens")
    lines.append("# TYPE crypto_position_value_usd gauge")
    for pos in positions:
        sym = pos["symbol"].replace("/", "_")
        lines.append(f'crypto_position_token_amount{{symbol="{sym}"}} {pos["token_amount"]}')
        lines.append(f'crypto_position_avg_cost_usd{{symbol="{sym}"}} {pos["avg_cost_usd"]}')
        lines.append(f'crypto_position_invested_usd{{symbol="{sym}"}} {pos["total_invested_usd"]}')

    # ── Tax lot metrics ───────────────────────────────────────────────────────
    try:
        tax_lots = get_open_tax_lots()
        if tax_lots:
            # Current prices from Redis for unrealized gain calculation
            current_prices: dict[str, float] = {}
            for lot in tax_lots:
                sym = lot["symbol"]
                if sym not in current_prices:
                    raw = r.get(f"claude:signals:{sym}")
                    if raw:
                        current_prices[sym] = json.loads(raw).get("price_usd", 0) or 0

            lines.append("# HELP tax_cost_basis_usd Average cost basis per open lot in USD")
            lines.append("# TYPE tax_cost_basis_usd gauge")
            lines.append("# HELP tax_unrealized_gain_usd Unrealized gain/loss per open lot in USD")
            lines.append("# TYPE tax_unrealized_gain_usd gauge")
            lines.append("# HELP tax_holding_days Days held per open tax lot")
            lines.append("# TYPE tax_holding_days gauge")

            for lot in tax_lots:
                sym = lot["symbol"].replace("/", "_")
                lot_id = lot["id"]
                cost = lot["cost_basis_usd"]
                qty = lot["quantity"]
                holding = lot["holding_days"]
                current_price = current_prices.get(lot["symbol"], 0)
                unrealized = (current_price - cost) * qty if current_price else 0

                labels = f'symbol="{sym}",lot_id="{lot_id}"'
                lines.append(f'tax_cost_basis_usd{{{labels}}} {cost}')
                lines.append(f'tax_unrealized_gain_usd{{{labels}}} {round(unrealized, 4)}')
                lines.append(f'tax_holding_days{{{labels}}} {holding}')
    except Exception as e:
        logger.warning(f"Tax metrics error: {e}")

    lines.append(f"crypto_last_metrics_update {_metrics.get('updated', 0)}")
    return "\n".join(lines) + "\n"


# Flask app for /metrics endpoint
metrics_app = Flask("executor_metrics")

@metrics_app.route("/metrics")
def metrics_endpoint():
    return Response(render_prometheus_metrics(), mimetype="text/plain; version=0.0.4")

@metrics_app.route("/health")
def health():
    return {"status": "ok", "mode": "simulation" if SIMULATION_MODE else "live"}

@metrics_app.route("/positions")
def positions_endpoint():
    import json as _json
    return _json.dumps(get_all_positions()), 200, {"Content-Type": "application/json"}

@metrics_app.route("/trades")
def trades_endpoint():
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT * FROM trades ORDER BY id DESC LIMIT 50"
    ).fetchall()
    conn.close()
    cols = ["id","timestamp","symbol","side","amount_usd","token_amount",
            "price_usd","gas_cost_usd","tx_hash","status","signal_source",
            "signal_confidence","signal_strength","cost_basis_usd",
            "realized_pnl_usd","chain"]
    import json as _json
    return _json.dumps([dict(zip(cols, r)) for r in rows]), 200, {"Content-Type": "application/json"}


def start_metrics_server():
    """Run Flask metrics server in a background thread."""
    import logging as _log
    _log.getLogger("werkzeug").setLevel(_log.ERROR)
    metrics_app.run(host="0.0.0.0", port=8091, debug=False)


# ════════════════════════════════════════════════════════════════════════════
#  MAIN LOOP
# ════════════════════════════════════════════════════════════════════════════

def process_signals(w3=None):
    risk = load_risk_config()

    if not check_daily_spend_limit(risk):
        return

    for key in r.scan_iter("claude:signals:*"):
        try:
            claude_signal = json.loads(r.get(key))
            symbol = claude_signal.get("symbol", "").upper()
            if not symbol:
                continue

            allowed, reason = check_asset_allowed(symbol)
            if not allowed:
                continue

            # Skip if no Polygon token address available
            if not SIMULATION_MODE and symbol not in TOKENS:
                logger.debug(f"{symbol}: not in Polygon token registry, skipping live execution")
                continue

            momentum_key    = f"signals:momentum:{symbol}_USDC"
            raw             = r.get(momentum_key)
            momentum_signal = json.loads(raw) if raw else {}

            decision = evaluate_signal(symbol, claude_signal, momentum_signal, risk)
            if decision is None:
                continue

            if SIMULATION_MODE:
                simulate_execution(decision)
            else:
                execute_live_trade(decision, w3)

        except Exception as e:
            logger.error(f"Error processing {key}: {e}", exc_info=True)


if __name__ == "__main__":
    mode = "SIMULATION" if SIMULATION_MODE else "LIVE — POLYGON"
    logger.info(f"DeFi Executor starting in {mode} mode")

    init_db()
    update_metrics()

    # Start metrics/API server in background thread
    t = threading.Thread(target=start_metrics_server, daemon=True)
    t.start()
    logger.info("Metrics server started on port 8091 (/metrics, /positions, /trades)")

    if not SIMULATION_MODE:
        if not WEB3_AVAILABLE:
            logger.error("web3 package not installed. Cannot run in live mode.")
            exit(1)
        if not _PRIVATE_KEY or not WALLET_ADDRESS:
            logger.error("WALLET_ADDRESS and WALLET_PRIVATE_KEY required for live mode.")
            exit(1)
        if not ONEINCH_API_KEY:
            logger.error("ONEINCH_API_KEY required for live mode.")
            exit(1)
        w3 = get_w3()
        if not w3:
            logger.error(f"Could not connect to Polygon RPC: {POLYGON_RPC_URL}")
            exit(1)
        bal = get_usdc_balance(w3)
        logger.info(f"Connected to Polygon. Wallet USDC balance: ${bal:.2f}")
    else:
        logger.info("SIMULATION mode — no real transactions will occur.")
        w3 = None

    time.sleep(20)

    while True:
        try:
            process_signals(w3)
            if not SIMULATION_MODE and w3:
                update_metrics()
        except Exception as e:
            logger.error(f"Signal loop error: {e}", exc_info=True)
        time.sleep(60)
