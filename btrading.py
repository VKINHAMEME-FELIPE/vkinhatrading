import pandas as pd
import os
import json

# ===== CONFIGURAÇÕES DO BOT - altere conforme desejar ===== #
PAR_CONFIG = {
    # 'PAR': 'arquivo.csv',
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

# PARÂMETROS
INITIAL_BALANCE = 500.0
LEVERAGE = 20
TP_PCT = 0.015
SL_PCT = 0.013
POSITION_MARGIN = 15
RSI_PERIOD = 14
MIN_ORDER_SIZE = 10

def calc_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

def calc_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def backtest_symbol(symbol, file, params):
    if not os.path.exists(file):
        print(f"Arquivo {file} não encontrado.")
        return None

    df = pd.read_csv(file)
    if 'timestamp' not in df.columns:
        df['timestamp'] = pd.to_datetime(df.iloc[:, 0])
    else:
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', errors='coerce').fillna(pd.to_datetime(df['timestamp']))

    df['close'] = df['close'].astype(float)
    df = df.reset_index(drop=True)
    df['EMA7'] = calc_ema(df['close'], 7)
    df['EMA21'] = calc_ema(df['close'], 21)
    df['RSI'] = calc_rsi(df['close'], RSI_PERIOD)

    balance = INITIAL_BALANCE
    trade_history = []
    pos = None
    balance_curve = [balance]
    max_balance = balance
    drawdown = 0

    for i in range(22, len(df)):
        row = df.iloc[i]
        prev = df.iloc[i-1]
        price = row['close']

        # Cruzamento de EMA
        open_long = prev['EMA7'] <= prev['EMA21'] and row['EMA7'] > row['EMA21'] and row['RSI'] > 50
        open_short = prev['EMA7'] >= prev['EMA21'] and row['EMA7'] < row['EMA21'] and row['RSI'] > 50

        # Abre posição nova se não há posição
        if pos is None:
            if open_long or open_short:
                side = 'LONG' if open_long else 'SHORT'
                margin = POSITION_MARGIN
                qty = (margin * LEVERAGE) / price
                if balance >= margin and qty * price >= MIN_ORDER_SIZE:
                    pos = {
                        'side': side,
                        'entry_price': price,
                        'qty': qty,
                        'margin': margin,
                        'open_idx': i
                    }
                    balance -= margin  # Reserva margem

        # Gerencia posição aberta
        if pos is not None:
            change = (price - pos['entry_price']) / pos['entry_price'] if pos['side'] == 'LONG' else (pos['entry_price'] - price) / pos['entry_price']
            take_profit = change >= TP_PCT
            stop_loss = change <= -SL_PCT
            close_trade = False
            result = 0
            reason = ''
            if take_profit:
                close_trade = True
                reason = 'TP'
            elif stop_loss:
                close_trade = True
                reason = 'SL'
            if close_trade:
                # Calcula PnL da operação
                pnl = pos['qty'] * (price - pos['entry_price']) if pos['side'] == 'LONG' else pos['qty'] * (pos['entry_price'] - price)
                pnl *= (1 - 0.0004)  # FEE
                balance += pos['margin'] + pnl  # Libera margem + lucro/prejuízo
                trade_history.append({
                    'entry_time': df.iloc[pos['open_idx']]['timestamp'],
                    'exit_time': row['timestamp'],
                    'side': pos['side'],
                    'entry': pos['entry_price'],
                    'exit': price,
                    'qty': pos['qty'],
                    'pnl': pnl,
                    'reason': reason,
                })
                pos = None
        balance_curve.append(balance)
        max_balance = max(balance, max_balance)
        drawdown = max(drawdown, (max_balance - balance) / max_balance)

    # Fim - fecha posição a mercado se restou aberta
    if pos is not None:
        price = df.iloc[-1]['close']
        pnl = pos['qty'] * (price - pos['entry_price']) if pos['side'] == 'LONG' else pos['qty'] * (pos['entry_price'] - price)
        pnl *= (1 - 0.0004)
        balance += pos['margin'] + pnl
        trade_history.append({
            'entry_time': df.iloc[pos['open_idx']]['timestamp'],
            'exit_time': df.iloc[-1]['timestamp'],
            'side': pos['side'],
            'entry': pos['entry_price'],
            'exit': price,
            'qty': pos['qty'],
            'pnl': pnl,
            'reason': 'FORCE_EXIT'
        })
        balance_curve.append(balance)
        drawdown = max(drawdown, (max_balance - balance) / max_balance)

    result = {
        'par': symbol,
        'trades': len(trade_history),
        'saldo_final': balance,
        'lucro': balance - INITIAL_BALANCE,
        'rentabilidade_pct': (balance / INITIAL_BALANCE - 1) * 100,
        'max_drawdown_pct': drawdown * 100,
        'trade_history': trade_history,
        'balance_curve': balance_curve,
    }
    return result

# Rodar para todos os pares configurados
for par, arquivo in PAR_CONFIG.items():
    res = backtest_symbol(par, arquivo, {})
    if res:
        print(f"\n=== {par} ===")
        print(f"Trades: {res['trades']}")
        print(f"Saldo final: {res['saldo_final']:.2f}")
        print(f"Lucro: {res['lucro']:.2f}")
        print(f"Rentabilidade: {res['rentabilidade_pct']:.1f}%")
        print(f"Drawdown máximo: {res['max_drawdown_pct']:.2f}%")
        print(f"Primeiros 3 trades:")
        for tr in res['trade_history'][:3]:
            print(tr)
        print(f"Últimos 3 trades:")
        for tr in res['trade_history'][-3:]:
            print(tr)
