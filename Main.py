import asyncio
import json
import os
import time
from datetime import datetime
from collections import deque, defaultdict
import websockets
import requests
import numpy as np

# ====================== CONFIG ======================
LICENSE_KEY     = os.getenv("LICENSE_KEY", "")
ACCOUNT_ID      = os.getenv("ACCOUNT_ID", "scalper")
DERIV_TOKEN     = os.getenv("DERIV_TOKEN", "pat_b263937b653fa685defbe47917ae6771c25ebf64431aa3fd0a86afbf5ce94bd8")
DERIV_APP_ID    = os.getenv("DERIV_APP_ID", "1089")

# === AGGRESSIVE RISK SETTINGS ===
RISK_PERCENT    = 2.5              # % of balance per trade
MIN_STAKE       = 0.5
MAX_STAKE       = 50.0
MAX_OPEN        = 6                # allow more concurrent trades

# Multiplier settings (powerful)
MULTIPLIER      = 100              # higher = more aggressive
TAKE_PROFIT_USD = 3.0              # take profit in $
STOP_LOSS_USD   = 2.0              # hard stop loss in $

# Strategy
FAST_EMA        = 5
SLOW_EMA        = 11
RSI_PERIOD      = 6
RSI_OB          = 78
RSI_OS          = 22

# Expanded symbol list (high opportunity)
SYMBOLS = [
    "R_75", "R_100", "R_50", "R_25",
    "1HZ75V", "1HZ100V", "1HZ50V",
    "frxXAUUSD", "frxEURUSD", "frxGBPUSD",
    "frxUSDJPY", "frxAUDUSD", "frxUSDCAD", "frxNZDUSD"
]

LICENSE_CHECK_EVERY = 3600
# ====================================================

class MoneyMakerScalper:
    def __init__(self):
        self.ws = None
        self.prices = {s: deque(maxlen=120) for s in SYMBOLS}
        self.scores = defaultdict(float)
        self.open_count = 0
        self.balance = 1000.0
        self.last_license = 0
        self.running = True
        self.active_contracts = {}

    def log(self, msg):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

    def verify_license(self):
        url = "https://aypfqqzuhutjgxzyzqmx.supabase.co/functions/v1/ea-verify"
        payload = {"license_key": LICENSE_KEY, "mt_account": ACCOUNT_ID}
        try:
            r = requests.post(url, json=payload, headers={"Content-Type": "application/json"}, timeout=10)
            if '"valid":true' in r.text:
                self.log("License VALID ✓")
                return True
            self.log("License INVALID ✗")
            return False
        except Exception as e:
            self.log(f"License error: {e}")
            return False

    def ema(self, data, period):
        if len(data) < period: return None
        weights = np.exp(np.linspace(-1., 0., period))
        weights /= weights.sum()
        return float(np.convolve(list(data)[-period:], weights, mode='valid')[0])

    def rsi(self, data, period):
        if len(data) < period + 1: return 50.0
        deltas = np.diff(data)
        avg_gain = np.mean(np.where(deltas > 0, deltas, 0)[-period:])
        avg_loss = np.mean(np.where(deltas < 0, -deltas, 0)[-period:])
        if avg_loss == 0: return 100.0
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    def calc_stake(self):
        stake = self.balance * (RISK_PERCENT / 100)
        return round(max(MIN_STAKE, min(MAX_STAKE, stake)), 2)

    async def connect(self):
        uri = f"wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}"
        self.ws = await websockets.connect(uri)
        self.log("Connected to Deriv")

        await self.ws.send(json.dumps({"authorize": DERIV_TOKEN}))
        resp = json.loads(await self.ws.recv())
        if "error" in resp:
            self.log(f"Auth failed: {resp['error']['message']}")
            return False

        self.balance = float(resp["authorize"].get("balance", 1000))
        self.log(f"Authorized | Balance: ${self.balance:.2f}")

        for s in SYMBOLS:
            await self.ws.send(json.dumps({"ticks": s, "subscribe": 1}))
        self.log(f"Subscribed to {len(SYMBOLS)} symbols")
        return True

    async def open_multiplier(self, symbol, direction):
        if self.open_count >= MAX_OPEN:
            return

        stake = self.calc_stake()
        # direction: "CALL" → buy, "PUT" → sell
        contract_type = "MULTUP" if direction == "CALL" else "MULTDOWN"

        proposal = {
            "proposal": 1,
            "amount": stake,
            "basis": "stake",
            "contract_type": contract_type,
            "currency": "USD",
            "symbol": symbol,
            "multiplier": MULTIPLIER,
            "limit_order": {
                "take_profit": TAKE_PROFIT_USD,
                "stop_loss": STOP_LOSS_USD
            }
        }

        await self.ws.send(json.dumps(proposal))
        resp = json.loads(await self.ws.recv())

        if "error" in resp:
            # fallback to short tick contract if multiplier not available
            await self.open_tick_contract(symbol, direction, stake)
            return

        pid = resp["proposal"]["id"]
        price = resp["proposal"]["ask_price"]

        await self.ws.send(json.dumps({"buy": pid, "price": price}))
        buy = json.loads(await self.ws.recv())

        if "buy" in buy:
            self.open_count += 1
            contract_id = buy["buy"]["contract_id"]
            self.active_contracts[contract_id] = symbol
            self.log(f"★ MULTIPLIER {direction} {symbol} | Stake ${stake} | x{MULTIPLIER} | TP ${TAKE_PROFIT_USD} / SL ${STOP_LOSS_USD}")

    async def open_tick_contract(self, symbol, direction, stake):
        """Fallback fast tick contract"""
        proposal = {
            "proposal": 1,
            "amount": stake,
            "basis": "stake",
            "contract_type": direction,
            "currency": "USD",
            "duration": 3,
            "duration_unit": "t",
            "symbol": symbol
        }
        await self.ws.send(json.dumps(proposal))
        resp = json.loads(await self.ws.recv())
        if "error" in resp:
            return
        pid = resp["proposal"]["id"]
        price = resp["proposal"]["ask_price"]
        await self.ws.send(json.dumps({"buy": pid, "price": price}))
        buy = json.loads(await self.ws.recv())
        if "buy" in buy:
            self.open_count += 1
            self.log(f"★ TICK {direction} {symbol} | Stake ${stake}")

    async def on_tick(self, data):
        symbol = data["tick"]["symbol"]
        price = float(data["tick"]["quote"])
        self.prices[symbol].append(price)
        prices = list(self.prices[symbol])

        if len(prices) < 30:
            return

        fast = self.ema(prices, FAST_EMA)
        slow = self.ema(prices, SLOW_EMA)
        rsi = self.rsi(prices, RSI_PERIOD)

        if fast is None or slow is None:
            return

        # Volatility score for scanner
        vol = np.std(prices[-20:])
        self.scores[symbol] = vol

        # Only trade the currently hottest symbols
        ranked = sorted(self.scores.items(), key=lambda x: x[1], reverse=True)[:7]
        if symbol not in [x[0] for x in ranked]:
            return

        prev_fast = self.ema(prices[:-1], FAST_EMA)
        prev_slow = self.ema(prices[:-1], SLOW_EMA)
        if prev_fast is None or prev_slow is None:
            return

        # ===== AGGRESSIVE MULTI-STRATEGY =====
        buy_signal = False
        sell_signal = False

        # 1. EMA Cross + RSI
        if prev_fast <= prev_slow and fast > slow and 38 < rsi < RSI_OB:
            buy_signal = True
        if prev_fast >= prev_slow and fast < slow and RSI_OS < rsi < 62:
            sell_signal = True

        # 2. Strong momentum (price running)
        recent_move = (prices[-1] - prices[-8]) / prices[-8] * 100
        if recent_move > 0.12 and rsi > 55:
            buy_signal = True
        if recent_move < -0.12 and rsi < 45:
            sell_signal = True

        # 3. RSI extreme reversal (aggressive fade)
        if rsi < 18:
            buy_signal = True
        if rsi > 82:
            sell_signal = True

        if buy_signal:
            await self.open_multiplier(symbol, "CALL")
        elif sell_signal:
            await self.open_multiplier(symbol, "PUT")

    async def run(self):
        self.log("=" * 65)
        self.log("POWERFUL MONEY-MAKER SCALPER  |  Multipliers + Aggressive")
        self.log("=" * 65)

        if not LICENSE_KEY or not DERIV_TOKEN:
            self.log("ERROR: Set LICENSE_KEY and DERIV_TOKEN environment variables")
            return

        if not self.verify_license():
            self.log("License failed. Exiting.")
            return

        self.last_license = time.time()

        while self.running:
            try:
                if not await self.connect():
                    await asyncio.sleep(10)
                    continue

                while self.running:
                    if time.time() - self.last_license > LICENSE_CHECK_EVERY:
                        if not self.verify_license():
                            self.running = False
                            break
                        self.last_license = time.time()

                    try:
                        msg = await asyncio.wait_for(self.ws.recv(), timeout=25)
                        data = json.loads(msg)

                        if "tick" in data:
                            await self.on_tick(data)
                        elif "buy" in data:
                            pass
                        elif "proposal_open_contract" in data:
                            # can be used later for trailing management
                            pass

                    except asyncio.TimeoutError:
                        await self.ws.send(json.dumps({"ping": 1}))

            except Exception as e:
                self.log(f"Reconnecting... ({e})")
                self.open_count = 0
                await asyncio.sleep(8)

if __name__ == "__main__":
    bot = MoneyMakerScalper()
    asyncio.run(bot.run())
