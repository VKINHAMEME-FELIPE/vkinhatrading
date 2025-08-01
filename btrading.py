import pandas as pd
import asyncio
import json
import os
import logging
from datetime import datetime
from typing import Dict, List, Optional

# Configurações do bot (mantidas exatamente como no código original)
SYMBOLS = ["SOLUSDT"]  # Atualizar com os pares dos CSVs
INTERVAL = "1m"
LEVERAGE = 10
POSITION_SIZE = 0.1
TP_PCT = 0.08
SL_PCT = 0.013
RSI_PERIOD = 14
RSI_OVERBOUGHT = 0
RSI_OVERSOLD = 0
EMA7_PERIOD = 7
EMA21_PERIOD = 21
MIN_ORDER_SIZE = 10
CONSISTENCY_THRESHOLD = 0
VOLUME_THRESHOLD = 0

# Variáveis globais do bot
sim_balance = 500.0  # Saldo inicial de 500 USDT
sim_daily_gain = 0.0
total_gain = 0.0
trade_count = 0
total_loss_count = 0
latest_prices: Dict[str, float] = {}
positions: Dict[str, Dict] = {}
ema7: Dict[str, List[float]] = {}
ema21: Dict[str, List[float]] = {}
rsi_values: Dict[str, List[float]] = {}
trend_consistency: Dict[str, int] = {}
last_candle: Dict[str, Dict] = {}
trade_history = []
logging.basicConfig(filename='trading_bot.log', level=logging.INFO)

# Função para carregar CSV da pasta Time
def load_historical_data(file_path: str) -> pd.DataFrame:
    df = pd.read_csv(file_path)
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    return df[['timestamp', 'close', 'volume']]

# Funções do bot (mantidas idênticas ao código original)
def calculate_ema(prices: List[float], period: int) -> float:
    if len(prices) < period:
        return 0.0
    alpha = 2 / (period + 1)
    ema = prices[0]
    for price in prices[1:]:
        ema = alpha * price + (1 - alpha) * ema
    return ema

def calculate_rsi(prices: List[float], period: int) -> float:
    if len(prices) < period:
        return 50.0
    gains = []
    losses = []
    for i in range(1, len(prices)):
        diff = prices[i] - prices[i-1]
        if diff > 0:
            gains.append(diff)
            losses.append(0)
        else:
            gains.append(0)
            losses.append(-diff)
    avg_gain = sum(gains[-period:]) / period if gains else 0
    avg_loss = sum(losses[-period:]) / period if losses else 0
    rs = avg_gain / avg_loss if avg_loss != 0 else 0
    return 100 - (100 / (1 + rs)) if rs != 0 else 50.0

async def place_order(symbol: str, side: str, price: float, quantity: float, position_size: float):
    global sim_balance, trade_count, sim_daily_gain, total_gain
    notional = price * quantity
    if notional < MIN_ORDER_SIZE:
        logging.info(f"Ordem para {symbol} ignorada: tamanho mínimo não atingido ({notional:.2f} < {MIN_ORDER_SIZE})")
        return
    if sim_balance < notional:
        logging.info(f"Saldo insuficiente para {symbol}: {sim_balance:.2f} < {notional:.2f}")
        return
    sim_balance -= notional
    positions[symbol] = {
        'side': side,
        'entry_price': price,
        'quantity': quantity,
        'position_size': position_size,
        'tp_price': price * (1 + TP_PCT) if side == 'BUY' else price * (1 - TP_PCT),
        'sl_price': price * (1 - SL_PCT) if side == 'BUY' else price * (1 + SL_PCT)
    }
    trade_count += 1
    logging.info(f"Ordem colocada: {side} {quantity:.4f} {symbol} a {price:.2f}")

async def verificar_tp(symbol: str) -> List[str]:
    global sim_balance, sim_daily_gain, total_gain, total_loss_count
    messages = []
    if symbol not in positions:
        return messages
    pos = positions[symbol]
    current_price = latest_prices.get(symbol, pos['entry_price'])
    side = pos['side']
    tp_price = pos['tp_price']
    sl_price = pos['sl_price']
    quantity = pos['quantity']
    notional = current_price * quantity
    profit = 0
    is_loss = False

    if side == 'BUY' and current_price >= tp_price:
        profit = (current_price - pos['entry_price']) * quantity * LEVERAGE
    elif side == 'BUY' and current_price <= sl_price:
        profit = (current_price - pos['entry_price']) * quantity * LEVERAGE
        is_loss = True
    elif side == 'SELL' and current_price <= tp_price:
        profit = (pos['entry_price'] - current_price) * quantity * LEVERAGE
    elif side == 'SELL' and current_price >= sl_price:
        profit = (pos['entry_price'] - current_price) * quantity * LEVERAGE
        is_loss = True
    else:
        return messages

    sim_balance += notional + profit
    sim_daily_gain += profit
    total_gain += profit
    if is_loss:
        total_loss_count += 1
    messages.append(f"Posição fechada: {side} {quantity:.4f} {symbol} a {current_price:.2f}, Lucro/Prejuízo: {profit:.2f}")
    trade_history.append({
        'symbol': symbol,
        'side': side,
        'entry_price': pos['entry_price'],
        'exit_price': current_price,
        'quantity': quantity,
        'profit': profit,
        'timestamp': datetime.now().isoformat()
    })
    del positions[symbol]
    with open('trade_history.json', 'w') as f:
        json.dump(trade_history, f, indent=4)
    return messages

async def handle_kline_async(candle: Dict):
    symbol = candle['s'].lower()
    kline = candle['k']
    close_price = float(kline['c'])
    volume = float(kline['v'])
    is_candle_closed = kline['x']
    if not is_candle_closed:
        return
    latest_prices[symbol] = close_price
    if symbol not in ema7:
        ema7[symbol] = []
        ema21[symbol] = []
        rsi_values[symbol] = []
        trend_consistency[symbol] = 0
    ema7[symbol].append(close_price)
    ema21[symbol].append(close_price)
    rsi_values[symbol].append(close_price)
    if len(ema7[symbol]) < max(EMA7_PERIOD, EMA21_PERIOD, RSI_PERIOD):
        return
    ema7[symbol] = ema7[symbol][-max(EMA7_PERIOD, EMA21_PERIOD, RSI_PERIOD):]
    ema21[symbol] = ema21[symbol][-max(EMA7_PERIOD, EMA21_PERIOD, RSI_PERIOD):]
    rsi_values[symbol] = rsi_values[symbol][-RSI_PERIOD:]
    current_ema7 = calculate_ema(ema7[symbol], EMA7_PERIOD)
    current_ema21 = calculate_ema(ema21[symbol], EMA21_PERIOD)
    current_rsi = calculate_rsi(rsi_values[symbol], RSI_PERIOD)
    last_candle[symbol] = kline
    prev_ema7 = calculate_ema(ema7[symbol][:-1], EMA7_PERIOD) if len(ema7[symbol]) > 1 else current_ema7
    prev_ema21 = calculate_ema(ema21[symbol][:-1], EMA21_PERIOD) if len(ema21[symbol]) > 1 else current_ema21
    avg_volume = sum(float(k['v']) for k in [last_candle[symbol]] if symbol in last_candle) / max(1, len([last_candle[symbol]]))
    is_volume_valid = volume > avg_volume * VOLUME_THRESHOLD
    if current_ema7 > current_ema21 and prev_ema7 <= prev_ema21 and current_rsi < RSI_OVERBOUGHT and is_volume_valid:
        trend_consistency[symbol] = min(trend_consistency.get(symbol, 0) + 1, CONSISTENCY_THRESHOLD)
        if trend_consistency[symbol] >= CONSISTENCY_THRESHOLD and symbol not in positions:
            quantity = (sim_balance * POSITION_SIZE) / close_price
            await place_order(symbol, 'BUY', close_price, quantity, POSITION_SIZE)
    elif current_ema7 < current_ema21 and prev_ema7 >= prev_ema21 and current_rsi > RSI_OVERSOLD and is_volume_valid:
        trend_consistency[symbol] = min(trend_consistency.get(symbol, 0) + 1, CONSISTENCY_THRESHOLD)
        if trend_consistency[symbol] >= CONSISTENCY_THRESHOLD and symbol not in positions:
            quantity = (sim_balance * POSITION_SIZE) / close_price
            await place_order(symbol, 'SELL', close_price, quantity, POSITION_SIZE)
    else:
        trend_consistency[symbol] = 0

# Função principal do backtest
async def backtest(symbol: str, file_path: str):
    global sim_balance, sim_daily_gain, total_gain, trade_count, total_loss_count
    balance_history = []
    data = load_historical_data(file_path)
    for index, row in data.iterrows():
        candle = {
            's': symbol.upper(),
            'k': {
                't': int(row['timestamp'].timestamp() * 1000),
                'c': row['close'],
                'v': row['volume'],
                'x': True
            }
        }
        await handle_kline_async(candle)
        messages = await verificar_tp(symbol)
        for msg in messages:
            logging.info(msg)
        balance_history.append({'timestamp': row['timestamp'], 'balance': sim_balance})
    return balance_history

async def run_backtest():
    # Lista de CSVs na pasta Time (atualizar com os arquivos reais)
    csv_files = {
        'BNBUSDT': 'BNBUSDT_1m_2025-07-25_to_2025-07-30.csv',
            'SOLUSDT': 'SOLUSDT_1m_2025-07-25_to_2025-07-30.csv',
 'ETHUSDT': 'ETHUSDT_1m_2025-07-25_to_2025-07-30.csv',
     'BTCUSDT': 'BTCUSDT_1m_2025-07-25_to_2025-07-30.csv',
         'NEARUSDT': 'NEARUSDT_1m_2025-07-25_to_2025-07-30.csv',
                 'ENAUSDT': 'ENAUSDT_1m_2025-07-25_to_2025-07-30.csv',
            'CHZUSDT': 'CHZUSDT_1m_2025-07-25_to_2025-07-30.csv',
 'TRXUSDT': 'TRXUSDT_1m_2025-07-25_to_2025-07-30.csv',
     'VINEUSDT': 'VINEUSDT_1m_2025-07-25_to_2025-07-30.csv',
         'XRPUSDT': 'XRPUSDT_1m_2025-07-25_to_2025-07-30.csv',
               }
    balance_histories = {}
    global sim_balance, trade_count, total_loss_count, total_gain
    for symbol in csv_files:
        sim_balance = 500.0  # Resetar saldo para cada par
        trade_count = 0
        total_loss_count = 0
        total_gain = 0.0
        file_path = csv_files[symbol]
        if os.path.exists(file_path):
            print(f"Executando backtest para {symbol}...")
            balance_histories[symbol] = await backtest(symbol, file_path)
            print(f"\nResultados para {symbol}:")
            print(f"Saldo inicial: 500.00 USDT")
            print(f"Saldo final: {sim_balance:.2f} USDT")
            print(f"Total de trades: {trade_count}")
            print(f"Total de perdas: {total_loss_count}")
            print(f"Ganho total: {total_gain:.2f} USDT")
            print(f"Taxa de acertos: {(trade_count - total_loss_count) / trade_count * 100:.2f}%" if trade_count > 0 else "N/A")
            # Salvar histórico de saldo
            balance_df = pd.DataFrame(balance_histories[symbol])
            balance_df.to_csv(f'balance_history_{symbol}.csv', index=False)
        else:
            print(f"Arquivo {file_path} não encontrado.")

if __name__ == "__main__":
    asyncio.run(run_backtest())