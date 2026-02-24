#!/usr/bin/env python3
"""
WEATHER BOT — Copy-Trade Top PnL Leaders on Weather Markets
============================================================

Strategy:
  1. Continuously scan Polymarket for weather/temperature/climate markets
  2. When found, identify top PnL traders on those markets
  3. Copy their YES/NO positions with fixed $5 bets
  4. Auto-resolve and redeem winning positions

Runs idle when no weather markets exist. Scans every 5 minutes.
"""

import time
import csv
import os
import sys
import json
import traceback
import tempfile
from datetime import datetime, timezone, timedelta

import requests
from dotenv import load_dotenv
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds, OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY

# ═══════════════════════════════════════════════════════════════
# CREDENTIALS
# ═══════════════════════════════════════════════════════════════
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

clob_client = ClobClient(
    "https://clob.polymarket.com",
    key=os.environ["POLYMARKET_PRIVATE_KEY"],
    chain_id=137,
    creds=ApiCreds(
        api_key=os.environ["POLYMARKET_API_KEY"],
        api_secret=os.environ["POLYMARKET_API_SECRET"],
        api_passphrase=os.environ["POLYMARKET_PASSPHRASE"],
    ),
)

# ═══════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════
GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"
DATA_API = "https://data-api.polymarket.com"

# Weather keywords to match market questions
WEATHER_KEYWORDS = [
    "temperature", "weather", "degrees", "°f", "°c",
    "fahrenheit", "celsius", "snow", "rainfall", "precipitation",
    "tornado", "flood", "drought", "heat wave", "polar vortex",
    "el nino", "la nina", "noaa", "hottest", "coldest", "warmest",
    "record high", "record low", "winter storm", "blizzard",
    "tropical storm", "hurricane season", "atlantic hurricane",
    "climate", "wildfire", "wind speed",
]

# Scanning
SCAN_INTERVAL = 300       # 5 minutes between scans
IDLE_SCAN_INTERVAL = 600  # 10 minutes when no markets found (save API calls)
TOP_TRADERS_COUNT = 10    # Copy top N PnL traders

# Trading
BET_SIZE = 5.00           # Fixed $5 per trade
MIN_ASK = 0.10            # Min price (avoid tiny-chance bets)
MAX_ASK = 0.95            # Max price (avoid near-certain bets with thin ROI)
MAX_SPREAD = 0.10         # Max bid-ask spread

# Risk
STARTING_BANKROLL = 30.00
KILL_SWITCH_MIN = 5.00
MAX_PENDING = 20

# Files
STATE_FILE = "data/weather_state.json"
LOG_FILE = "data/weather_trades.csv"

LOG_FIELDS = [
    "timestamp", "question", "outcome", "leader_address",
    "leader_pnl", "leader_position",
    "clob_ask", "clob_bid", "spread",
    "bet_size", "shares", "potential_profit",
    "token_id", "condition_id", "order_id",
    "resolved", "won", "pnl", "bankroll_after",
]


# ═══════════════════════════════════════════════════════════════
# STATE & LOGGING
# ═══════════════════════════════════════════════════════════════

def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                data = json.load(f)
            if data.get("version") == 1:
                return data
        except Exception:
            pass
    return {
        "version": 1,
        "bankroll": STARTING_BANKROLL,
        "pnl": 0.0,
        "wins": 0,
        "losses": 0,
        "trades": 0,
        "pending": [],
        "traded_tokens": [],
        "markets_seen": 0,
        "last_market_found": None,
    }


def save_state(state):
    """Atomic write: tmpfile + rename."""
    tmp_fd, tmp_path = tempfile.mkstemp(
        dir=os.path.dirname(STATE_FILE) or ".", suffix=".tmp"
    )
    try:
        with os.fdopen(tmp_fd, "w") as f:
            json.dump(state, f, indent=2, default=str)
        os.replace(tmp_path, STATE_FILE)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def init_log():
    os.makedirs("data", exist_ok=True)
    if not os.path.exists(LOG_FILE):
        with open(LOG_FILE, "w", newline="") as f:
            csv.DictWriter(f, fieldnames=LOG_FIELDS).writeheader()


def log_trade(trade: dict):
    with open(LOG_FILE, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=LOG_FIELDS)
        writer.writerow({k: trade.get(k, "") for k in LOG_FIELDS})


# ═══════════════════════════════════════════════════════════════
# MARKET SCANNER
# ═══════════════════════════════════════════════════════════════

def is_weather_market(market: dict) -> bool:
    """Check if a market is weather-related based on question + description."""
    q = (market.get("question", "") or "").lower()
    desc = (market.get("description", "") or "").lower()
    text = q + " " + desc
    return any(kw in text for kw in WEATHER_KEYWORDS)


def scan_weather_markets() -> list:
    """
    Scan all active Polymarket markets for weather-related ones.
    Returns list of weather market dicts sorted by liquidity (highest first).
    """
    all_markets = []
    offset = 0

    while offset < 5000:
        try:
            resp = requests.get(
                f"{GAMMA_API}/markets",
                params={
                    "active": True,
                    "closed": False,
                    "limit": 100,
                    "offset": offset,
                },
                timeout=15,
            )
            if resp.status_code != 200:
                break
            batch = resp.json()
            if not batch:
                break
            all_markets.extend(batch)
            if len(batch) < 100:
                break
            offset += 100
            time.sleep(0.1)  # rate limit
        except Exception as e:
            print(f"  [WARN] Gamma scan error at offset {offset}: {e}")
            break

    weather = [m for m in all_markets if is_weather_market(m)]

    # Sort by liquidity (most liquid first)
    weather.sort(key=lambda x: float(x.get("liquidityNum", 0) or 0), reverse=True)
    return weather


# ═══════════════════════════════════════════════════════════════
# TOP PNL LEADER IDENTIFICATION
# ═══════════════════════════════════════════════════════════════

def get_top_traders(condition_id: str) -> list:
    """
    Get top PnL traders for a specific market condition.
    Uses Polymarket's data API to find traders with highest profit.
    Returns list of {"address": str, "pnl": float, "position": "YES"/"NO", "size": float}
    """
    try:
        # Get market activity/positions
        resp = requests.get(
            f"{DATA_API}/activity",
            params={
                "market": condition_id,
                "limit": 50,
            },
            timeout=15,
        )
        if resp.status_code != 200:
            return []
        activities = resp.json()
        if not activities:
            return []

        # Aggregate PnL by trader address
        trader_pnl = {}
        for a in activities:
            addr = a.get("proxyWalletAddress", "") or a.get("address", "")
            if not addr:
                continue
            pnl = float(a.get("usdcSize", 0) or 0)
            side = a.get("side", "")
            if addr not in trader_pnl:
                trader_pnl[addr] = {"address": addr, "pnl": 0, "buys": 0, "sells": 0}
            if side == "BUY":
                trader_pnl[addr]["buys"] += pnl
            elif side == "SELL":
                trader_pnl[addr]["sells"] += pnl
                trader_pnl[addr]["pnl"] += pnl  # profit from selling

        # Sort by volume (proxy for engagement/confidence)
        traders = sorted(trader_pnl.values(),
                         key=lambda x: x["buys"] + x["sells"], reverse=True)
        return traders[:TOP_TRADERS_COUNT]

    except Exception as e:
        print(f"  [WARN] Failed to get traders for {condition_id[:16]}: {e}")
        return []


def get_leader_positions(market: dict) -> list:
    """
    For a weather market, find what the top PnL traders are betting on.
    Returns list of {"outcome": str, "confidence": float, "leaders": int}
    """
    condition_id = market.get("conditionId", "")
    if not condition_id:
        return []

    outcomes = json.loads(market.get("outcomes", "[]"))
    tokens = json.loads(market.get("clobTokenIds", "[]"))
    prices = json.loads(market.get("outcomePrices", "[]"))

    if not outcomes or not tokens:
        return []

    # Get top traders
    traders = get_top_traders(condition_id)
    if not traders:
        return []

    # Try to determine leader consensus from their positions
    try:
        wallet = os.environ.get("POLYMARKET_WALLET", "")
        positions_data = []
        for t in traders[:5]:  # Check top 5
            try:
                resp = requests.get(
                    f"{DATA_API}/positions",
                    params={"user": t["address"]},
                    timeout=10,
                )
                if resp.status_code == 200:
                    positions = resp.json()
                    for p in positions:
                        if p.get("conditionId") == condition_id:
                            positions_data.append({
                                "address": t["address"],
                                "outcome": p.get("outcome", ""),
                                "size": float(p.get("size", 0)),
                                "pnl": t["pnl"],
                            })
            except Exception:
                pass
            time.sleep(0.2)  # rate limit

        return positions_data
    except Exception as e:
        print(f"  [WARN] Failed to get leader positions: {e}")
        return []


# ═══════════════════════════════════════════════════════════════
# TRADE EXECUTION
# ═══════════════════════════════════════════════════════════════

def get_clob_prices(token_id: str) -> dict:
    """Get bid/ask from CLOB."""
    result = {"bid": 0.0, "ask": 0.0}
    try:
        ask_r = requests.get(
            f"{CLOB_API}/price?token_id={token_id}&side=SELL", timeout=3
        )
        result["ask"] = float(ask_r.json().get("price", 0))
        bid_r = requests.get(
            f"{CLOB_API}/price?token_id={token_id}&side=BUY", timeout=3
        )
        result["bid"] = float(bid_r.json().get("price", 0))
    except Exception:
        pass
    return result


def execute_trade(state, market, outcome_idx, leader_info):
    """Place a real FOK buy order copying the leader's position."""
    tokens = json.loads(market.get("clobTokenIds", "[]"))
    outcomes = json.loads(market.get("outcomes", "[]"))

    if outcome_idx >= len(tokens):
        return False

    token_id = tokens[outcome_idx]
    outcome = outcomes[outcome_idx] if outcome_idx < len(outcomes) else "?"

    prices = get_clob_prices(token_id)
    ask = prices["ask"]
    bid = prices["bid"]

    if ask <= 0 or ask >= 1:
        return False
    if ask < MIN_ASK or ask > MAX_ASK:
        print(f"  [SKIP] {outcome} ask=${ask:.2f} outside range "
              f"${MIN_ASK:.2f}-${MAX_ASK:.2f}")
        return False

    spread = ask - bid
    if spread > MAX_SPREAD:
        print(f"  [SKIP] {outcome} spread=${spread:.3f} > max ${MAX_SPREAD:.2f}")
        return False

    shares = int(BET_SIZE / ask)
    if shares < 1:
        return False

    actual_cost = round(shares * ask, 2)

    try:
        order_args = OrderArgs(
            token_id=token_id,
            price=round(ask, 2),
            size=shares,
            side=BUY,
        )
        signed_order = clob_client.create_order(order_args)
        resp = clob_client.post_order(signed_order, OrderType.FOK)

        if not resp or not resp.get("success"):
            print(f"  [NOFILL] {outcome} @ ${ask:.2f}")
            return False
    except Exception as e:
        print(f"  [ERROR] Order failed: {e}")
        return False

    order_id = resp.get("orderID", "")

    trade = {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "question": market.get("question", "")[:100],
        "outcome": outcome,
        "leader_address": leader_info.get("address", "")[:16] + "...",
        "leader_pnl": leader_info.get("pnl", 0),
        "leader_position": leader_info.get("outcome", ""),
        "clob_ask": ask,
        "clob_bid": bid,
        "spread": round(spread, 3),
        "bet_size": actual_cost,
        "shares": shares,
        "potential_profit": round(shares - actual_cost, 4),
        "token_id": token_id,
        "condition_id": market.get("conditionId", ""),
        "order_id": order_id,
        "resolved": False,
    }

    state["pending"].append(trade)
    state["trades"] += 1
    state["bankroll"] -= actual_cost
    state["traded_tokens"].append(token_id)

    save_state(state)
    log_trade(trade)

    roi = (1 - ask) / ask * 100
    print(f"\n  >>> COPY TRADE: {outcome} @ ${ask:.2f} ({roi:.1f}% ROI)")
    print(f"      {market.get('question', '')[:60]}")
    print(f"      Copying leader: {leader_info.get('address', '')[:16]}... "
          f"(PnL: ${leader_info.get('pnl', 0):,.2f})")
    print(f"      Order: {order_id[:16]}... | "
          f"${actual_cost:.2f} for {shares} shares")
    print(f"      Bankroll: ${state['bankroll']:.2f}")

    return True


# ═══════════════════════════════════════════════════════════════
# RESOLUTION
# ═══════════════════════════════════════════════════════════════

def resolve_trades(state):
    """Check pending trades for resolution via data API."""
    if not state["pending"]:
        return

    try:
        wallet = os.environ["POLYMARKET_WALLET"]
        positions = requests.get(
            f"{DATA_API}/positions?user={wallet}",
            timeout=15,
        ).json()
    except Exception as e:
        print(f"  [WARN] Resolution check failed: {e}")
        return

    pos_map = {}
    for p in positions:
        tid = p.get("asset", "") or p.get("tokenId", "")
        if tid:
            pos_map[tid] = p

    still_pending = []
    for t in state["pending"]:
        tid = t.get("token_id", "")
        pos = pos_map.get(tid)

        if pos and pos.get("redeemable"):
            won = float(pos.get("curValue", 0)) > 0
            if won:
                payout = t["shares"] * 1.0
                pnl = payout - t["bet_size"]
                state["bankroll"] += payout
                state["wins"] += 1
            else:
                pnl = -t["bet_size"]
                state["losses"] += 1

            state["pnl"] += pnl
            t["resolved"] = True
            t["won"] = won
            t["pnl"] = round(pnl, 4)
            t["bankroll_after"] = round(state["bankroll"], 2)
            log_trade(t)

            mark = "WIN" if won else "LOSS"
            w, l = state["wins"], state["losses"]
            wr = w / max(w + l, 1)
            print(f"\n  {'>>>' if won else 'XXX'} RESOLVED: {t['outcome']} -> {mark}")
            print(f"      {t['question'][:50]}")
            print(f"      PnL: ${pnl:+.4f} | Bank: ${state['bankroll']:.2f} | "
                  f"{w}W-{l}L ({wr:.1%})")
        else:
            # Check for stale positions (>72h past expected end)
            try:
                ts = datetime.fromisoformat(t["timestamp"].replace("Z", "+00:00"))
                age = (datetime.now(timezone.utc) - ts).total_seconds()
                if age > 259200:  # >72h
                    print(f"  [STALE] Marking as loss (>72h): {t['question'][:50]}")
                    pnl = -t["bet_size"]
                    state["losses"] += 1
                    state["pnl"] += pnl
                    t["resolved"] = True
                    t["won"] = False
                    t["pnl"] = round(pnl, 4)
                    t["bankroll_after"] = round(state["bankroll"], 2)
                    log_trade(t)
                    continue
            except Exception:
                pass
            still_pending.append(t)

    state["pending"] = still_pending


# ═══════════════════════════════════════════════════════════════
# DISPLAY
# ═══════════════════════════════════════════════════════════════

def print_banner():
    print("=" * 70)
    print("  WEATHER BOT — Copy-Trade Top PnL Leaders")
    print(f"  Scan: every {SCAN_INTERVAL}s | "
          f"Idle: every {IDLE_SCAN_INTERVAL}s")
    print(f"  Bet: ${BET_SIZE:.2f} fixed | "
          f"Ask range: ${MIN_ASK:.2f}-${MAX_ASK:.2f}")
    print(f"  Bankroll: ${STARTING_BANKROLL:.2f} | "
          f"Keywords: {len(WEATHER_KEYWORDS)} weather terms")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 70)


def print_dashboard(state):
    w, l = state["wins"], state["losses"]
    wr = w / max(w + l, 1)
    pending = len(state["pending"])

    print(f"\n{'=' * 70}")
    print(f"  WEATHER DASHBOARD | "
          f"{datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}")
    print(f"{'=' * 70}")
    print(f"  Bankroll:  ${state['bankroll']:>10,.2f}  |  "
          f"PnL: ${state['pnl']:>+10,.2f}")
    print(f"  Record:    {w}W-{l}L ({wr:.1%})  |  "
          f"Trades: {state['trades']}")
    print(f"  Pending:   {pending} positions")
    print(f"  Markets seen: {state['markets_seen']}  |  "
          f"Last found: {state.get('last_market_found', 'never')}")
    print(f"{'=' * 70}")


# ═══════════════════════════════════════════════════════════════
# MAIN LOOP
# ═══════════════════════════════════════════════════════════════

def run():
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    init_log()
    state = load_state()
    print_banner()

    print(f"\n  Loaded state: {state['trades']} trades, "
          f"${state['bankroll']:.2f} bankroll, "
          f"{len(state['pending'])} pending")

    scan_count = 0
    consecutive_empty = 0

    while True:
        try:
            scan_count += 1
            now_utc = datetime.now(timezone.utc)

            # Resolve any pending trades
            resolve_trades(state)

            # Safety checks
            if state["bankroll"] < KILL_SWITCH_MIN:
                print(f"\n  [KILL SWITCH] Bankroll ${state['bankroll']:.2f} "
                      f"< ${KILL_SWITCH_MIN:.2f}")
                print_dashboard(state)
                save_state(state)
                time.sleep(IDLE_SCAN_INTERVAL)
                continue

            if len(state["pending"]) >= MAX_PENDING:
                print(f"  [MAX PENDING] {len(state['pending'])} positions")
                save_state(state)
                time.sleep(SCAN_INTERVAL)
                continue

            # Scan for weather markets
            use_interval = (IDLE_SCAN_INTERVAL if consecutive_empty > 3
                            else SCAN_INTERVAL)

            print(f"\n  [SCAN #{scan_count}] "
                  f"{now_utc.strftime('%H:%M:%S UTC')} — "
                  f"scanning for weather markets...")

            weather_markets = scan_weather_markets()
            traded_set = set(state["traded_tokens"])

            if not weather_markets:
                consecutive_empty += 1
                if consecutive_empty <= 3 or consecutive_empty % 10 == 0:
                    print(f"  [SCAN] No weather markets found "
                          f"(scan #{scan_count}, "
                          f"{consecutive_empty} consecutive empties)")
                else:
                    print(f"  [IDLE] No weather markets. "
                          f"Next scan in {use_interval}s...", end="\r",
                          flush=True)
                save_state(state)
                time.sleep(use_interval)
                continue

            # Weather markets found!
            consecutive_empty = 0
            state["markets_seen"] += len(weather_markets)
            state["last_market_found"] = now_utc.strftime("%Y-%m-%d %H:%M:%S")

            print(f"\n  [WEATHER FOUND] {len(weather_markets)} weather markets!")
            for m in weather_markets[:5]:
                liq = float(m.get("liquidityNum", 0) or 0)
                print(f"    - {m.get('question', '')[:70]}  "
                      f"Liq: ${liq:,.0f}")

            # Process each weather market
            trades_this_cycle = 0
            for market in weather_markets:
                if state["bankroll"] < BET_SIZE:
                    break
                if len(state["pending"]) >= MAX_PENDING:
                    break

                tokens = json.loads(market.get("clobTokenIds", "[]"))
                outcomes = json.loads(market.get("outcomes", "[]"))

                # Skip already-traded tokens
                if all(t in traded_set for t in tokens):
                    continue

                # Get leader positions
                print(f"\n  [LEADERS] Analyzing: {market.get('question', '')[:60]}")
                positions = get_leader_positions(market)

                if not positions:
                    print(f"    No leader positions found")
                    continue

                # Find consensus outcome
                outcome_votes = {}
                for p in positions:
                    outcome = p.get("outcome", "")
                    if outcome not in outcome_votes:
                        outcome_votes[outcome] = {
                            "count": 0, "total_size": 0,
                            "best_leader": None
                        }
                    outcome_votes[outcome]["count"] += 1
                    outcome_votes[outcome]["total_size"] += p.get("size", 0)
                    if (outcome_votes[outcome]["best_leader"] is None or
                            p.get("size", 0) > outcome_votes[outcome]
                            ["best_leader"].get("size", 0)):
                        outcome_votes[outcome]["best_leader"] = p

                # Pick the outcome with most leader support
                best_outcome = max(outcome_votes.items(),
                                   key=lambda x: x[1]["total_size"])
                outcome_name = best_outcome[0]
                leader = best_outcome[1]["best_leader"]

                print(f"    Leaders favor: {outcome_name} "
                      f"({best_outcome[1]['count']} traders, "
                      f"${best_outcome[1]['total_size']:,.0f} size)")

                # Find matching token index
                outcome_idx = None
                for i, o in enumerate(outcomes):
                    if o == outcome_name:
                        outcome_idx = i
                        break

                if outcome_idx is None:
                    print(f"    Could not match outcome '{outcome_name}' "
                          f"to tokens")
                    continue

                if tokens[outcome_idx] in traded_set:
                    print(f"    Already traded this outcome")
                    continue

                # Execute copy trade
                success = execute_trade(state, market, outcome_idx,
                                        leader or {})
                if success:
                    trades_this_cycle += 1
                    time.sleep(0.5)

            # Dashboard
            print_dashboard(state)
            save_state(state)

            if trades_this_cycle:
                print(f"\n  Placed {trades_this_cycle} copy trades this cycle")

            print(f"\n  Next scan in {SCAN_INTERVAL}s...")
            time.sleep(SCAN_INTERVAL)

        except KeyboardInterrupt:
            print("\n\n[SHUTDOWN] Saving state...")
            save_state(state)
            print_dashboard(state)
            break
        except Exception as e:
            print(f"\n[ERROR] {e}")
            traceback.print_exc()
            save_state(state)
            time.sleep(30)


if __name__ == "__main__":
    run()
