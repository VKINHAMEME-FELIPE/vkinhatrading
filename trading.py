import asyncio
from telethon import TelegramClient
from dotenv import load_dotenv
import os
import pandas as pd
import time
import datetime
import logging
import json
from binance.um_futures import UMFutures
from binance.error import ClientError
from binance.websocket.um_futures.websocket_client import UMFuturesWebsocketClient
import urllib.request
import numpy as np

# Configurar logging no início
logging.basicConfig(
    filename='trading.log',
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Carregar variáveis de ambiente
load_dotenv()

# ======================== CONFIG ========================
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
PHONE_NUMBER = os.getenv("PHONE_NUMBER")
BINANCE_API_KEY = os.getenv("API_KEY")
BINANCE_API_SECRET = os.getenv("API_SECRET")
SIMULATED = os.getenv("SIMULATED", "true").lower() == "true"
TELEGRAM_IMAGE_URL_LONG = os.getenv("TELEGRAM_IMAGE_URL_LONG")
TELEGRAM_IMAGE_URL_SHORT = os.getenv("TELEGRAM_IMAGE_URL_SHORT")
TELEGRAM_IMAGE_URL_INF = os.getenv("TELEGRAM_IMAGE_URL_INF")

# Verificar variáveis de ambiente
if not all([API_ID, API_HASH, PHONE_NUMBER]):
    raise ValueError("Erro: API_ID, API_HASH ou PHONE_NUMBER não encontrados no .env")
if not SIMULATED and not all([BINANCE_API_KEY, BINANCE_API_SECRET]):
    raise ValueError("Erro: BINANCE_API_KEY ou BINANCE_API_SECRET não encontrados no .env")
if not TELEGRAM_IMAGE_URL_LONG:
    logger.warning("TELEGRAM_IMAGE_URL_LONG não encontrado no .env, enviando mensagem sem imagem")
    print("⚠️ TELEGRAM_IMAGE_URL_LONG não encontrado no .env, enviando mensagem sem imagem")
if not TELEGRAM_IMAGE_URL_SHORT:
    logger.warning("TELEGRAM_IMAGE_URL_SHORT não encontrado no .env, enviando mensagem sem imagem")
    print("⚠️ TELEGRAM_IMAGE_URL_SHORT não encontrado no .env, enviando mensagem sem imagem")
if not TELEGRAM_IMAGE_URL_INF:
    logger.warning("TELEGRAM_IMAGE_URL_INF não encontrado no .env, enviando mensagem sem imagem")
    print("⚠️ TELEGRAM_IMAGE_URL_INF não encontrado no .env, enviando mensagem sem imagem")

SYMBOLS = ['btcusdc', 'ethusdc', 'solusdc', 'nearusdc', 'suiusdc', 'xrpusdc']
LEVERAGE = 20
TOTAL_MARGIN = 6.67  # Margem total por trade
TP_PCT = 0.008  # Take-profit
SL_PCT = 0.02   # Stop-loss
FEE_RATE = 0.0004  # Taxa de transação
LAYER_PCTS = [0.2, 0.3, 0.5]  # Percentuais de margem por camada
LAYER_OFFSETS = [0.001, 0.003, 0.006]  # Offsets de preço por camada
EMA_DIFF_THRESHOLD = 0.002  # Diferença mínima entre EMAs
TRADE_HISTORY_FILE = "trade_history.json"  # Arquivo para histórico de ordens

# ======================== BINANCE ========================
try:
    binance_client = UMFutures(key=BINANCE_API_KEY, secret=BINANCE_API_SECRET)
    for symbol in SYMBOLS:
        binance_client.change_leverage(symbol=symbol.upper(), leverage=LEVERAGE)
except ClientError as e:
    logger.error(f"Erro ao inicializar cliente Binance: {e}")
    raise ValueError(f"Erro ao inicializar cliente Binance: {e}")

orders = {symbol.upper(): [] for symbol in SYMBOLS}
latest_prices = {symbol: None for symbol in SYMBOLS}
last_telegram_time = time.time()
data = {symbol: [] for symbol in SYMBOLS}

# ======================== TELEGRAM ========================
async def connect_telegram():
    """Conecta ao Telegram e gerencia a autenticação."""
    client = TelegramClient('trading_session', API_ID, API_HASH)
    try:
        await client.connect()
        logger.info("Conexão com Telegram estabelecida")
        print("🔒 Conectando com o Telegram...")

        if not await client.is_user_authorized():
            logger.error("❌ Sessão não autorizada. Verifique se o arquivo 'trading_session.session' é válido.")
            raise Exception("Sessão do Telegram inválida. Autentique localmente primeiro.")
        else:
            logger.info("✅ Sessão autorizada com sucesso.")
            print("✅ Sessão autorizada com sucesso!")
        return client

    except Exception as e:
        logger.error(f"Erro ao conectar ao Telegram: {e}")
        print(f"Erro ao conectar ao Telegram: {e}")
        raise


async def get_all_groups(client):
    """Obtém todos os grupos onde o bot está presente."""
    groups = []
    try:
        async for dialog in client.iter_dialogs():
            if dialog.is_group or dialog.is_channel:
                try:
                    participant = await client.get_permissions(dialog.id, 'me')
                    if participant.is_admin or (dialog.is_group and not dialog.entity.broadcast):
                        groups.append(dialog.id)
                        logger.info(f"Grupo encontrado: ID={dialog.id}, Nome={dialog.title}")
                        print(f"Grupo encontrado: ID={dialog.id}, Nome={dialog.title}")
                    else:
                        logger.warning(f"Sem permissão para enviar mensagens no grupo: ID={dialog.id}, Nome={dialog.title}")
                        print(f"Sem permissão no grupo: ID={dialog.id}, Nome={dialog.title}")
                except Exception as e:
                    logger.warning(f"Erro ao verificar permissões no grupo {dialog.id} ({dialog.title}): {e}")
                    print(f"Erro ao verificar grupo {dialog.id} ({dialog.title}): {e}")
        logger.info(f"Encontrados {len(groups)} grupos para envio de mensagens")
        print(f"Encontrados {len(groups)} grupos para envio de mensagens")
        return groups or ['me']
    except Exception as e:
        logger.error(f"Erro ao obter grupos: {e}")
        print(f"Erro ao obter grupos: {e}")
        return ['me']

async def send_telegram(client, message, groups, image_type='inf', is_initial=False):
    """Envia mensagem a todos os grupos com controle de flood, com imagem específica."""
    global last_telegram_time
    logger.info(f"Tentando enviar mensagem: {message}, Tipo de imagem: {image_type}, Inicial: {is_initial}")
    logger.info(f"last_telegram_time: {last_telegram_time}, Tempo atual: {time.time()}")

    if not is_initial and time.time() - last_telegram_time < 60:
        logger.warning(f"Evitando flood no Telegram, mensagem não enviada: {message}")
        print(f"⚠️ Evitando flood no Telegram, mensagem não enviada: {message}")
        return

    image_url = {
        'long': TELEGRAM_IMAGE_URL_LONG,
        'short': TELEGRAM_IMAGE_URL_SHORT,
        'inf': TELEGRAM_IMAGE_URL_INF
    }.get(image_type, TELEGRAM_IMAGE_URL_INF)

    for group_id in groups:
        try:
            if image_url:
                try:
                    urllib.request.urlopen(image_url).close()
                    logger.info(f"URL da imagem válida: {image_url}")
                except Exception as e:
                    logger.error(f"Erro ao validar URL da imagem {image_url}: {e}")
                    print(f"⚠️ Erro ao validar URL da imagem: {e}")
                    await client.send_message(group_id, message)
                    logger.info(f"Mensagem de texto enviada ao grupo {group_id} (sem imagem devido a erro)")
                    print(f"Mensagem enviada ao grupo {group_id} (sem imagem)")
                else:
                    await client.send_file(group_id, image_url, caption=message)
                    logger.info(f"Mensagem com imagem enviada ao grupo {group_id}")
                    print(f"Mensagem com imagem enviada ao grupo {group_id}")
            else:
                await client.send_message(group_id, message)
                logger.info(f"Mensagem enviada ao grupo {group_id}")
                print(f"Mensagem enviada ao grupo {group_id}")
        except Exception as e:
            logger.error(f"Erro ao enviar mensagem ao grupo {group_id}: {e}")
            print(f"Erro ao enviar mensagem ao grupo {group_id}: {e}")

    last_telegram_time = time.time()
    logger.info(f"last_telegram_time atualizado para: {last_telegram_time}")

# ======================== BINANCE ========================
def save_trade_history(entry):
    """Salva o histórico de ordens em um arquivo JSON."""
    try:
        if os.path.exists(TRADE_HISTORY_FILE):
            with open(TRADE_HISTORY_FILE, "r") as f:
                data = json.load(f)
        else:
            data = []
        data.append(entry)
        with open(TRADE_HISTORY_FILE, "w") as f:
            json.dump(data, f, indent=4)
        logger.info(f"Histórico salvo: {entry}")
    except Exception as e:
        logger.error(f"Erro ao salvar histórico: {e}")
        print(f"Erro ao salvar histórico: {e}")

def get_account_balance():
    """Obtém o saldo da conta (real ou simulado)."""
    if SIMULATED:
        return 200
    try:
        balances = binance_client.get_balance()
        usdc_balance = next((item for item in balances if item["asset"] == "USDC"), None)
        if usdc_balance:
            return float(usdc_balance["balance"])
        return 0
    except Exception as e:
        logger.error(f"Erro ao obter saldo da conta: {e}")
        print(f"Erro ao obter saldo da conta: {e}")
        return 200

async def get_futures_summary(max_retries=3, retry_delay=5):
    """Obtém resumo da conta de futuros com retries, multiplicando saldos por 10."""
    if SIMULATED:
        print("⚠️ Modo simulado ativo, retornando saldo simulado")
        return {
            "Total Equity": 200.0 * 10,  # Multiplicado por 10
            "Margin Balance": 200.0 * 10,  # Multiplicado por 10
            "Floating P&L": 0.0,
            "Futures Wallet Balance": 200.0 * 10  # Multiplicado por 10
        }
    
    for attempt in range(max_retries):
        try:
            balances = binance_client.get_balance()
            usdc_balance = next((item for item in balances if item["asset"] == "USDC"), None)
            if not usdc_balance:
                logger.error("USDC não encontrado na lista de saldos")
                print("⚠️ USDC não encontrado na lista de saldos")
                return {}

            wallet_balance = float(usdc_balance["balance"]) * 10  # Multiplicado por 10
            margin_balance = float(usdc_balance.get("crossWalletBalance", 0.0)) * 10  # Multiplicado por 10
            pnl = float(usdc_balance.get("crossUnPnl", 0.0)) * 10  # Multiplicado por 10

            summary = {
                "Total Equity": wallet_balance + pnl,
                "Margin Balance": margin_balance,
                "Floating P&L": pnl,
                "Futures Wallet Balance": wallet_balance
            }
            print(f"Resumo da conta obtido: {summary}")
            return summary
        except ClientError as e:
            logger.error(f"Tentativa {attempt + 1}/{max_retries} - Erro ao obter resumo da conta de futuros (ClientError): {e}")
            print(f"Tentativa {attempt + 1}/{max_retries} - Erro ao obter resumo da conta de futuros: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
            continue
        except Exception as e:
            logger.error(f"Tentativa {attempt + 1}/{max_retries} - Erro inesperado ao obter resumo da conta de futuros: {e}")
            print(f"Tentativa {attempt + 1}/{max_retries} - Erro inesperado ao obter resumo da conta de futuros: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
            continue
    logger.error("Falha ao obter resumo da conta após todas as tentativas")
    print("⚠️ Falha ao obter resumo da conta após todas as tentativas")
    return {}

def format_summary(summary):
    """Formata o resumo da conta para exibição."""
    return f"""=== Binance Futures Summary ===
💼 Total Equity: {summary['Total Equity']:.2f} USDC
📈 Margin Balance: {summary['Margin Balance']:.2f} USDC
📉 Floating P&L: {summary['Floating P&L']:.2f} USDC
💰 Wallet Balance: {summary['Futures Wallet Balance']:.2f} USDC
=============================="""

def get_open_positions(symbol):
    """Verifica posições abertas para um símbolo."""
    if SIMULATED:
        return len(orders[symbol.upper()])
    try:
        positions = binance_client.get_position_risk(symbol=symbol.upper())
        for pos in positions:
            if float(pos['positionAmt']) != 0:
                return abs(float(pos['positionAmt']))
        return 0
    except Exception as e:
        logger.error(f"Erro ao verificar posições abertas para {symbol}: {e}")
        print(f"Erro ao verificar posições abertas para {symbol}: {e}")
        return 0

def get_price_rest(symbol):
    """Obtém o preço atual do símbolo via REST API."""
    try:
        ticker = binance_client.get_symbol_ticker(symbol=symbol.upper())
        return float(ticker['price'])
    except Exception as e:
        logger.error(f"Erro ao obter preço via REST para {symbol}: {e}")
        print(f"Erro ao obter preço via REST para {symbol}: {e}")
        return None

async def get_kline_data(symbol, interval='1m', limit=22):
    """Obtém dados de klines para volume e tendência."""
    try:
        klines = binance_client.get_klines(symbol=symbol.upper(), interval=interval, limit=limit)
        df = pd.DataFrame(klines, columns=[
            'timestamp', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_asset_volume', 'trades', 'taker_buy_base',
            'taker_buy_quote', 'ignored'
        ])
        df['close'] = df['close'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['volume'] = df['volume'].astype(float)
        return df
    except Exception as e:
        logger.error(f"Erro ao obter dados de klines para {symbol}: {e}")
        print(f"Erro ao obter dados de klines para {symbol}: {e}")
        return None

async def check_trading_conditions(symbol, close_price):
    """Verifica condições para entrada: volume e tendência."""
    df = await get_kline_data(symbol, interval='1m', limit=22)
    if df is None or len(df) < 22:
        logger.warning(f"Dados insuficientes para {symbol}")
        return False

    # Verificar volume mínimo (acima da média dos últimos 20 candles)
    avg_volume = df['volume'].mean()
    recent_volume = df['volume'].iloc[-1]
    if recent_volume < avg_volume:
        logger.info(f"Volume insuficiente para {symbol}: {recent_volume:.2f} < Média {avg_volume:.2f}")
        return False

    # Verificar tendência consistente (EMA7 > EMA21 ou EMA7 < EMA21 nos últimos 3 fechamentos)
    df['ema7'] = df['close'].ewm(span=7).mean()
    df['ema21'] = df['close'].ewm(span=21).mean()
    diff = abs(df['ema7'].iloc[-1] - df['ema21'].iloc[-1]) / df['ema21'].iloc[-1]
    if diff < EMA_DIFF_THRESHOLD:
        logger.info(f"Diferença EMA insuficiente para {symbol}: {diff:.4f} < {EMA_DIFF_THRESHOLD}")
        return False
    trend_consistent = False
    if df['ema7'].iloc[-1] > df['ema21'].iloc[-1]:
        trend_consistent = all(df['ema7'].tail(3) > df['ema21'].tail(3))
    elif df['ema7'].iloc[-1] < df['ema21'].iloc[-1]:
        trend_consistent = all(df['ema7'].tail(3) < df['ema21'].tail(3))
    if not trend_consistent:
        logger.info(f"Tendência não consistente para {symbol}")
        return False

    return True

def place_order(order_type, entry_price, symbol):
    """Coloca uma nova ordem (simulada ou real)."""
    symbol = symbol.upper()
    if get_open_positions(symbol) >= len(LAYER_PCTS):
        logger.warning(f"Limite de ordens atingido para {symbol}")
        return f"⚠️ Limite de ordens atingido para {symbol}"

    messages = []
    for i, (pct, offset) in enumerate(zip(LAYER_PCTS, LAYER_OFFSETS), 1):
        entry = entry_price * (1 - offset) if order_type == 'long' else entry_price * (1 + offset)
        margin = TOTAL_MARGIN * pct
        qty = round((margin * LEVERAGE) / entry, 3)
        if SIMULATED:
            order_data = {
                'type': order_type,
                'entry': entry,
                'amount': qty,
                'cost': margin,
                'open_time': datetime.datetime.now(),
                'layer': i
            }
            orders[symbol].append(order_data)
            msg = f"✅ ORDEM EXECUTADA: {symbol} {order_type.upper()}\nCamada {i}/{len(LAYER_PCTS)}\nQTD: {qty}"
            messages.append(msg)
        else:
            side = 'BUY' if order_type == 'long' else 'SELL'
            try:
                binance_client.new_order(
                    symbol=symbol,
                    side=side,
                    type='MARKET',
                    quantity=qty
                )
                msg = f"✅ ORDEM EXECUTADA: {symbol} {order_type.upper()}\nCamada {i}/{len(LAYER_PCTS)}\nQTD: {qty}"
                messages.append(msg)
            except ClientError as e:
                msg = f"❌ Erro camada {i}/{len(LAYER_PCTS)}: {e}"
                messages.append(msg)

        trade_log = {
            "timestamp": str(datetime.datetime.utcnow()),
            "symbol": symbol,
            "type": order_type,
            "layer": i,
            "qty": qty,
            "price": entry,
            "simulated": SIMULATED
        }
        save_trade_history(trade_log)

    logger.info(f"Ordem colocada: {symbol} {order_type.upper()} Camada Inicial")
    return "\n".join(messages)

def close_order(order, current_price, symbol):
    """Fecha uma ordem simulada."""
    global total_gain, trade_count, total_loss_count, sim_balance, sim_daily_gain
    gain = order['amount'] * (current_price - order['entry']) if order['type'] == 'long' else order['amount'] * (order['entry'] - current_price)
    gain *= (1 - FEE_RATE)  # Desconta taxa
    total_gain += gain
    sim_daily_gain += gain
    sim_balance += order['cost'] + gain
    trade_count += 1
    if gain < 0:
        total_loss_count += 1
    orders[symbol.upper()].remove(order)

    percentual = (gain / sim_balance) * 100
    msg = f"""❌ <b>Ordem FECHADA</b>
<b>Par:</b> {symbol.upper()}
<b>Tipo:</b> {order['type'].upper()}
<b>Camada:</b> {order['layer']}
<b>Ganho:</b> {gain:.2f} USDC ({percentual:.2f}%)
<b>Saldo Atual:</b> {sim_balance:.2f} USDC
📈 <i>Operação realizada pelo bot VKINHA Trading</i>"""
    logger.info(f"Ordem fechada: {symbol.upper()} {order['type'].upper()}, Camada: {order['layer']}, Ganho: {gain:.2f} USDC")

    trade_log = {
        "timestamp": str(datetime.datetime.utcnow()),
        "symbol": symbol.upper(),
        "type": order['type'],
        "layer": order['layer'],
        "gain": gain,
        "percentual": percentual,
        "balance": sim_balance,
        "simulated": SIMULATED
    }
    save_trade_history(trade_log)

    return msg

async def initial_test_operations(client, groups):
    """Realiza operação inicial de teste (uma ordem LONG) com preço real."""
    if not SIMULATED:
        logger.info("Modo REAL ativo, operação de teste inicial não executada")
        return

    symbol = 'BTCUSDC'
    entry_price = None
    logger.info("Iniciando operação de teste")

    print("Aguardando preço real do BTCUSDC...")
    for _ in range(20):
        if symbol.lower() in latest_prices and latest_prices[symbol.lower()] is not None:
            entry_price = latest_prices[symbol.lower()]
            break
        await asyncio.sleep(1)

    if not entry_price:
        entry_price = get_price_rest(symbol)
        if not entry_price:
            msg = "⚠️ Erro: Não foi possível obter o preço real do BTCUSDC"
            logger.error(msg)
            print(msg)
            await send_telegram(client, msg, groups, image_type='inf', is_initial=True)
            return

    if not await check_trading_conditions(symbol, entry_price):
        msg = f"⚠️ Condições de trading não atendidas para teste em {symbol.upper()}"
        logger.warning(msg)
        print(msg)
        await send_telegram(client, msg, groups, image_type='inf', is_initial=True)
        return

    msg_long = place_order('long', entry_price, symbol)
    if msg_long:
        await send_telegram(client, f"🧪 Ordem TESTE LONG em {symbol.upper()}\n{msg_long}", groups, image_type='long')

    await asyncio.sleep(60)

    close_price = latest_prices.get(symbol.lower(), entry_price)
    messages = []
    for order in orders[symbol][:]:
        msg = close_order(order, close_price, symbol)
        messages.append(msg)

    for msg in messages:
        await send_telegram(client, msg, groups, image_type=order['type'])

def verificar_tp(symbol):
    """Verifica take-profit e stop-loss."""
    price = latest_prices.get(symbol.lower())
    ativos = orders[symbol.upper()]
    if not ativos or not price:
        return []
    messages = []
    for order in ativos[:]:
        change = (price - order['entry']) / order['entry'] if order['type'] == 'long' else (order['entry'] - price) / order['entry']
        if change >= TP_PCT or change <= -SL_PCT:
            msg = close_order(order, price, symbol)
            messages.append(msg)
    return messages

def handle_kline(msg, client, groups):
    """Processa mensagens do WebSocket da Binance a cada candle de 1 minuto."""
    global sim_day, sim_daily_gain
    if msg['e'] != 'kline' or not msg['k']['x']:  # Processar apenas candles fechados
        return
    symbol = msg['s'].lower()
    close_price = float(msg['k']['c'])
    timestamp = int(msg['k']['t'])
    dt = datetime.datetime.fromtimestamp(timestamp / 1000.0)
    
    data[symbol].append({'time': dt, 'close': close_price})
    if len(data[symbol]) < 22:
        return
    data[symbol] = data[symbol][-22:]

    latest_prices[symbol] = close_price
    df = pd.DataFrame(data[symbol])
    df = df[['time', 'close']]
    df['ema7'] = df['close'].ewm(span=7).mean()
    df['ema21'] = df['close'].ewm(span=21).mean()

    ema7 = df['ema7'].iloc[-1]
    ema21 = df['ema21'].iloc[-1]

    messages = verificar_tp(symbol)
    if not orders[symbol.upper()]:
        if asyncio.run_coroutine_threadsafe(check_trading_conditions(symbol, close_price), asyncio.get_event_loop()).result():
            diff = abs(ema7 - ema21) / ema21
            if diff >= EMA_DIFF_THRESHOLD:
                if ema7 > ema21:
                    msg = place_order('long', close_price, symbol)
                    if msg:
                        messages.append(f"📈 SINAL LONG {symbol.upper()}\n{msg}")
                elif ema7 < ema21:
                    msg = place_order('short', close_price, symbol)
                    if msg:
                        messages.append(f"📉 SINAL SHORT {symbol.upper()}\n{msg}")

    for msg in messages:
        image_type = 'long' if 'SINAL LONG' in msg else 'short' if 'SINAL SHORT' in msg else 'inf'
        if 'Ordem FECHADA' in msg:
            order_type = 'long' if 'LONG' in msg else 'short'
            image_type = order_type
        asyncio.create_task(send_telegram(client, msg, groups, image_type=image_type))

    now = datetime.datetime.now()
    if now.hour == 21 and datetime.date.today() != sim_day:
        sim_day = datetime.date.today()
        msg = f"""📆 <b>Relatório Diário</b>
📈 Rentabilidade: {sim_daily_gain:.2f} USDC
💰 Saldo Atual: {sim_balance:.2f} USDC
⚠️ <i>Modo {'simulado' if SIMULATED else 'real'} ativo</i>"""
        asyncio.create_task(send_telegram(client, msg, groups, image_type='inf'))
        sim_daily_gain = 0

def start_websocket(client, groups):
    """Inicia o WebSocket da Binance."""
    print("🔗 Conectando ao WebSocket da Binance...")
    logger.info("Iniciando WebSocket da Binance")
    ws_client = UMFuturesWebsocketClient()
    def make_callback(symbol):
        def callback(msg):
            asyncio.create_task(handle_kline_async(msg, client, groups))
        return callback
    for symbol in SYMBOLS:
        ws_client.kline(symbol=symbol.lower(), interval="1m", callback=make_callback(symbol))
    print("✅ WebSocket iniciado.")
    logger.info("WebSocket iniciado")

async def handle_kline_async(msg, client, groups):
    """Wrapper assíncrono para handle_kline."""
    handle_kline(msg, client, groups)

async def monitor_account(client, groups):
    """Monitora a conta de futuros a cada 2 horas."""
    while True:
        summary = await get_futures_summary()
        if summary:
            msg = format_summary(summary)
            print(msg)
            logger.info(msg)
            await send_telegram(client, msg, groups, image_type='inf')
        else:
            msg = "⚠️ Não foi possível obter o resumo da conta durante o monitoramento. Verifique a conexão com a Binance."
            print(msg)
            logger.error(msg)
            await send_telegram(client, msg, groups, image_type='inf')
        await asyncio.sleep(7200)  # 2 horas

async def log_status():
    """Log recorrente a cada 10 minutos indicando que o bot está ativo."""
    while True:
        print("🔍 Monitorando gráficos para sinais de trade...")
        logger.info("Monitorando gráficos para sinais de trade...")
        await asyncio.sleep(600)  # 10 minutos

async def main():
    """Função principal."""
    global sim_balance, sim_daily_gain, total_gain, trade_count, total_loss_count, sim_day
    sim_balance = 200
    sim_daily_gain = 0
    total_gain = 0
    trade_count = 0
    total_loss_count = 0
    sim_day = datetime.date.today()

    try:
        client = await connect_telegram()
        async with client:
            groups = await get_all_groups(client)
            modo = "SIMULATED" if SIMULATED else "REAL"
            await send_telegram(client, f"✅ VKINHA Trading iniciado em modo {modo} 🚀", groups, image_type='inf', is_initial=True)
            
            print("Loading dados da conta...")
            # Verificar conexão com a Binance
            try:
                binance_client.ping()
                print("✅ Conexão com Binance estabelecida")
                logger.info("Conexão com Binance estabelecida")
            except Exception as e:
                msg = f"⚠️ Erro ao conectar com a Binance: {e}"
                print(msg)
                logger.error(msg)
                await send_telegram(client, msg, groups, image_type='inf', is_initial=True)
                raise

            # Obter e exibir resumo da conta
            summary = await get_futures_summary()
            if not summary and not SIMULATED:
                msg = "❌ Falha crítica: Não foi possível obter o resumo da conta após várias tentativas. Verifique a API Key, permissões de futuros ou conexão com a Binance."
                print(msg)
                logger.error(msg)
                await send_telegram(client, msg, groups, image_type='inf', is_initial=True)
                raise ValueError(msg)
            
            msg = format_summary(summary)
            print(msg)
            logger.info(msg)
            await send_telegram(client, msg, groups, image_type='inf', is_initial=True)
            
            print("Sincronizado...")

            start_websocket(client, groups)
            await initial_test_operations(client, groups)
            asyncio.create_task(monitor_account(client, groups))
            asyncio.create_task(log_status())
            await client.run_until_disconnected()
    except Exception as e:
        logger.error(f"Erro no main: {e}")
        print(f"Erro no main: {e}")
        await send_telegram(client, f"❌ Erro no bot: {e}", groups, image_type='inf', is_initial=True)

if __name__ == '__main__':
    asyncio.run(main())
    print("✅ VKINHA Trading finalizado 🚀")