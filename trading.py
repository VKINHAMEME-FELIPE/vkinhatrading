import asyncio
from telethon import TelegramClient
from datetime import datetime, UTC, date
from dotenv import load_dotenv
import os
import pandas as pd
import time
import logging
from logging.handlers import RotatingFileHandler
import json
from binance.um_futures import UMFutures
from binance.error import ClientError
from binance.websocket.um_futures.websocket_client import UMFuturesWebsocketClient
import urllib.request
import uuid

# Configuração do logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
file_handler = RotatingFileHandler('trading.log', maxBytes=5*1024*1024, backupCount=3)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(console_handler)

# Configurações
load_dotenv()
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
PHONE_NUMBER = os.getenv("PHONE_NUMBER")
BINANCE_API_KEY = os.getenv("API_KEY")
BINANCE_API_SECRET = os.getenv("API_SECRET")
SIMULATED = os.getenv("SIMULATED", "true").lower() == "true"
TELEGRAM_IMAGE_URL_LONG = os.getenv("TELEGRAM_IMAGE_URL_LONG")
TELEGRAM_IMAGE_URL_SHORT = os.getenv("TELEGRAM_IMAGE_URL_SHORT")
TELEGRAM_IMAGE_URL_INF = os.getenv("TELEGRAM_IMAGE_URL_INF")
INFLATE_PUBLIC_BALANCE = True
SAFETY_MARGIN = 0.2
last_telegram_time = time.time()
last_critical_telegram_time = time.time()

# Verificação de variáveis de ambiente
if not all([API_ID, API_HASH, PHONE_NUMBER]):
    logger.error("API_ID, API_HASH ou PHONE_NUMBER não encontrados no .env")
    raise ValueError("Erro: API_ID, API_HASH ou PHONE_NUMBER não encontrados no .env")
if not SIMULATED and not all([BINANCE_API_KEY, BINANCE_API_SECRET]):
    logger.error("BINANCE_API_KEY ou BINANCE_API_SECRET não encontrados no .env")
    raise ValueError("Erro: BINANCE_API_KEY ou BINANCE_API_SECRET não encontrados no .env")
logger.info("Configurações validadas com sucesso")

# Pares de negociação
SYMBOLS = ['solusdt', 'chzusdt', 'nearusdt', 'bnbusdt', 'trxusdt', 'xrpusdt', 'vineusdt', 'enausdt']
LEVERAGE = 20
TOTAL_MARGIN = 6.67
TP_PCT = 0.008
SL_PCT = 0.02
FEE_RATE = 0.0004
LAYER_PCTS = [0.2, 0.3, 0.5]
LAYER_OFFSETS = [0.001, 0.003, 0.006]
EMA_DIFF_THRESHOLD = 0.001
TRADE_HISTORY_FILE = "trade_history.json"
CHECK_TREND_CONSISTENCY = False
logger.info("Constantes de configuração inicializadas")

# Inicialização do cliente Binance
try:
    binance_client = UMFutures(key=BINANCE_API_KEY, secret=BINANCE_API_SECRET)
    logger.info("Cliente Binance inicializado")
except ClientError as e:
    logger.error(f"Erro ao inicializar cliente Binance: {e}")
    raise ValueError(f"Erro ao inicializar cliente Binance: {e}")

orders = {symbol.upper(): {'long': [], 'short': []} for symbol in SYMBOLS}
latest_prices = {symbol: None for symbol in SYMBOLS}
data = {symbol: [] for symbol in SYMBOLS}
first_sol_order = None
layer_info = {symbol: {'entry_price': None, 'opened_layers': 0, 'order_type': None} for symbol in SYMBOLS}
sim_balance = 596.64
sim_daily_gain = 0
total_gain = 0
trade_count = 0
total_loss_count = 0
sim_day = date.today()
logger.info("Estruturas de dados iniciais configuradas")

def validate_symbols():
    try:
        exchange_info = binance_client.exchange_info()
        valid_symbols = {s['symbol'].lower() for s in exchange_info['symbols']}
        invalid_symbols = [s for s in SYMBOLS if s not in valid_symbols]
        if invalid_symbols:
            logger.error(f"Pares inválidos encontrados: {invalid_symbols}")
            raise ValueError(f"Pares inválidos: {invalid_symbols}")
        logger.info("Todos os pares de negociação são válidos")
    except ClientError as e:
        logger.error(f"Erro ao validar pares: {e}")
        raise ValueError(f"Erro ao validar pares: {e}")

def set_hedge_mode(symbol):
    try:
        position_mode = binance_client.get_position_mode()
        if not position_mode.get('dualSidePosition', False):
            binance_client.change_position_mode(dualSidePosition=True)
            logger.info(f"Modo de posição configurado para Hedge Mode para {symbol}")
        else:
            logger.info(f"Hedge Mode já configurado para {symbol}")
    except ClientError as e:
        if e.error_code == -4059:
            logger.info(f"Hedge Mode já configurado para {symbol}, ignorando erro: {e}")
        else:
            logger.error(f"Erro ao configurar Hedge Mode para {symbol}: {e}")
            raise
    except Exception as e:
        logger.error(f"Erro inesperado ao configurar Hedge Mode para {symbol}: {e}")
        raise

def get_symbol_precision(symbol):
    try:
        exchange_info = binance_client.exchange_info()
        for s in exchange_info['symbols']:
            if s['symbol'].lower() == symbol.lower():
                return s['quantityPrecision']
        logger.warning(f"Precisão não encontrada para {symbol}, usando padrão 3")
        return 3
    except Exception as e:
        logger.error(f"Erro ao obter precisão para {symbol}: {e}")
        return 3

try:
    validate_symbols()
    for symbol in SYMBOLS:
        set_hedge_mode(symbol.upper())
        binance_client.change_leverage(symbol=symbol.upper(), leverage=LEVERAGE)
    logger.info("Alavancagem configurada para todos os símbolos")
except ClientError as e:
    logger.error(f"Erro ao configurar alavancagem: {e}")
    raise ValueError(f"Erro ao configurar alavancagem: {e}")

def get_account_balance():
    logger.info("Obtendo saldo da conta")
    try:
        if SIMULATED:
            logger.info("Modo simulado: retornando saldo simulado")
            return sim_balance
        balances = binance_client.balance()
        usdt_balance = next((item for item in balances if item["asset"] == "USDT"), None)
        if usdt_balance:
            balance = float(usdt_balance["balance"])
            logger.info(f"Saldo USDT obtido: {balance:.2f}")
            return balance
        logger.warning("USDT não encontrado na lista de saldos")
        return 0
    except Exception as e:
        logger.error(f"Erro ao obter saldo da conta: {e}")
        print(f"Erro ao obter saldo da conta: {e}")
        return sim_balance

def check_balance_sufficiency(symbol, order_type, margin_needed):
    try:
        if SIMULATED:
            return True
        balance = get_account_balance()
        total_used_margin = 0
        for s in SYMBOLS:
            positions = binance_client.get_position_risk(symbol=s.upper())
            for pos in positions:
                margin = float(pos['isolatedMargin']) if float(pos['positionAmt']) != 0 else 0
                total_used_margin += margin
        available_balance = balance - total_used_margin
        safety_threshold = balance * SAFETY_MARGIN
        if available_balance < margin_needed + safety_threshold:
            logger.warning(f"Saldo insuficiente para {symbol} ({order_type}): disponível={available_balance:.2f}, necessário={margin_needed:.2f}, reserva={safety_threshold:.2f}")
            return False
        logger.info(f"Saldo suficiente para {symbol} ({order_type}): disponível={available_balance:.2f}, necessário={margin_needed:.2f}")
        return True
    except Exception as e:
        logger.error(f"Erro ao verificar saldo para {symbol}: {e}")
        return False

async def get_futures_summary(max_retries=3, retry_delay=5):
    logger.info("Obtendo resumo da conta de futuros")
    if SIMULATED:
        logger.info("Modo simulado ativo, retornando saldo simulado")
        return {
            "Total Equity": sim_balance + sim_daily_gain,
            "Margin Balance": sim_balance,
            "Floating P&L": sim_daily_gain,
            "Futures Wallet Balance": sim_balance
        }
    for attempt in range(max_retries):
        try:
            balances = binance_client.balance()
            usdt_balance = next((item for item in balances if item["asset"] == "USDT"), None)
            if not usdt_balance:
                logger.error("USDT não encontrado na lista de saldos")
                return {}
            wallet_balance = float(usdt_balance["balance"])
            margin_balance = float(usdt_balance.get("crossWalletBalance", 0.0))
            pnl = float(usdt_balance.get("crossUnPnl", 0.0))
            summary = {
                "Total Equity": wallet_balance + pnl,
                "Margin Balance": margin_balance,
                "Floating P&L": pnl,
                "Futures Wallet Balance": wallet_balance
            }
            logger.info(f"Resumo da conta obtido: {summary}")
            return summary
        except ClientError as e:
            logger.error(f"Tentativa {attempt + 1}/{max_retries} - Erro ao obter resumo da conta: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
            continue
        except Exception as e:
            logger.error(f"Tentativa {attempt + 1}/{max_retries} - Erro inesperado ao obter resumo da conta: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
            continue
    logger.error("Falha ao obter resumo da conta após todas as tentativas")
    return {}

def format_summary(summary):
    logger.info("Formatando resumo da conta")
    display_summary = {
        k: v * 10 if INFLATE_PUBLIC_BALANCE else v
        for k, v in summary.items()
    }
    formatted = f"""=== Binance Futures Summary ===
💼 Total Equity: {display_summary['Total Equity']:.2f} USDT
📈 Margin Balance: {display_summary['Margin Balance']:.2f} USDT
📉 Floating P&L: {display_summary['Floating P&L']:.2f} USDT
💰 Wallet Balance: {display_summary['Futures Wallet Balance']:.2f} USDT
=============================="""
    logger.info("Resumo formatado com sucesso")
    return formatted

async def connect_telegram():
    logger.info("Iniciando conexão com Telegram")
    client = TelegramClient('trading_session', API_ID, API_HASH)
    try:
        await client.connect()
        logger.info("Conexão com Telegram estabelecida")
        print("🔒 Conectando com o Telegram...")
        if not await client.is_user_authorized():
            logger.info("Usuário não autorizado, solicitando código")
            print("Usuário não autorizado, solicitando código...")
            try:
                await client.send_code_request(PHONE_NUMBER)
                logger.info("Código de autenticação solicitado")
                print("Código solicitado. Verifique seu Telegram ou SMS.")
                code = input("Digite o código recebido por SMS/Telegram: ")
                await client.sign_in(PHONE_NUMBER, code)
                logger.info("Autenticação bem-sucedida")
                print("✅ Autenticação bem-sucedida!")
            except Exception as e:
                logger.error(f"Erro ao autenticar: {e}")
                print(f"Erro ao autenticar: {e}")
                raise
        else:
            logger.info("Usuário já autorizado")
            print("✅ Usuário já autorizado!")
        return client
    except Exception as e:
        logger.error(f"Erro ao conectar ao Telegram: {e}")
        print(f"Erro ao conectar ao Telegram: {e}")
        raise

async def get_all_groups(client):
    logger.info("Obtendo lista de grupos do Telegram")
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

async def send_telegram(client, message, groups, image_type='inf', is_initial=False, is_critical=False):
    global last_telegram_time, last_critical_telegram_time
    logger.info(f"Tentando enviar mensagem: {message}, Tipo de imagem: {image_type}, Inicial: {is_initial}, Crítica: {is_critical}")
    current_time = time.time()
    if not is_initial and not is_critical and current_time - last_telegram_time < 60:
        logger.warning(f"Evitando flood no Telegram, mensagem não enviada: {message}")
        print(f"⚠️ Evitando flood no Telegram, mensagem não enviada: {message}")
        return
    if is_critical and current_time - last_critical_telegram_time < 30:
        logger.warning(f"Evitando flood de mensagens críticas no Telegram: {message}")
        print(f"⚠️ Evitando flood de mensagens críticas: {message}")
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
                    await client.send_file(group_id, image_url, caption=message)
                    logger.info(f"Mensagem com imagem enviada ao grupo {group_id}")
                    print(f"Mensagem com imagem enviada ao grupo {group_id}")
                except Exception as e:
                    logger.error(f"Erro ao validar URL da imagem {image_url}: {e}")
                    print(f"⚠️ Erro ao validar URL da imagem: {e}")
                    await client.send_message(group_id, message)
                    logger.info(f"Mensagem de texto enviada ao grupo {group_id} (sem imagem)")
                    print(f"Mensagem enviada ao grupo {group_id} (sem imagem)")
            else:
                await client.send_message(group_id, message)
                logger.info(f"Mensagem enviada ao grupo {group_id}")
                print(f"Mensagem enviada ao grupo {group_id}")
        except Exception as e:
            logger.error(f"Erro ao enviar mensagem ao grupo {group_id}: {e}")
            print(f"Erro ao enviar mensagem ao grupo {group_id}: {e}")
    if is_critical:
        last_critical_telegram_time = current_time
        logger.info(f"last_critical_telegram_time atualizado para: {last_critical_telegram_time}")
    else:
        last_telegram_time = current_time
        logger.info(f"last_telegram_time atualizado para: {last_telegram_time}")

def save_trade_history(entry):
    logger.info(f"Salvando entrada no histórico: {entry}")
    try:
        if os.path.exists(TRADE_HISTORY_FILE):
            with open(TRADE_HISTORY_FILE, "r") as f:
                data = json.load(f)
        else:
            data = []
        data.append(entry)
        with open(TRADE_HISTORY_FILE, "w") as f:
            json.dump(data, f, indent=4)
        logger.info(f"Histórico salvo com sucesso: {entry}")
    except Exception as e:
        logger.error(f"Erro ao salvar histórico: {e}")
        print(f"Erro ao salvar histórico: {e}")

def get_open_positions(symbol, order_type):
    logger.info(f"Verificando posições abertas para {symbol} ({order_type})")
    try:
        if SIMULATED:
            count = len(orders[symbol.upper()][order_type])
            logger.info(f"Modo simulado: {count} posições abertas para {symbol} ({order_type})")
            return count
        positions = binance_client.get_position_risk(symbol=symbol.upper())
        for pos in positions:
            position_amt = float(pos['positionAmt'])
            if (order_type == 'long' and position_amt > 0) or (order_type == 'short' and position_amt < 0):
                amount = abs(position_amt)
                logger.info(f"Posição aberta encontrada para {symbol} ({order_type}): {amount}")
                return amount
        logger.info(f"Nenhuma posição aberta para {symbol} ({order_type})")
        return 0
    except Exception as e:
        logger.error(f"Erro ao verificar posições abertas para {symbol} ({order_type}): {e}")
        print(f"Erro ao verificar posições abertas para {symbol} ({order_type}): {e}")
        return 0

def get_price_rest(symbol):
    logger.info(f"Obtendo preço via REST para {symbol}")
    try:
        data = binance_client.mark_price(symbol=symbol.upper())
        price = float(data['markPrice'])
        logger.info(f"Preço obtido para {symbol}: {price}")
        return price
    except Exception as e:
        logger.error(f"Erro ao obter preço via REST para {symbol}: {e}")
        print(f"Erro ao obter preço via REST para {symbol}: {e}")
        return None

async def get_kline_data(symbol, interval='1m', limit=22):
    logger.info(f"Obtendo dados de klines para {symbol}, intervalo: {interval}, limite: {limit}")
    try:
        klines = binance_client.klines(symbol=symbol.upper(), interval=interval, limit=limit)
        df = pd.DataFrame(klines, columns=[
            'timestamp', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_asset_volume', 'trades', 'taker_buy_base',
            'taker_buy_quote', 'ignored'
        ])
        df['close'] = df['close'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['volume'] = df['volume'].astype(float)
        logger.info(f"Dados de klines obtidos para {symbol}, {len(df)} candles")
        return df
    except Exception as e:
        logger.error(f"Erro ao obter dados de klines para {symbol}: {e}")
        print(f"Erro ao obter dados de klines para {symbol}: {e}")
        return None

async def check_trading_conditions(symbol, close_price):
    logger.info(f"Verificando condições de trading para {symbol}, preço de fechamento: {close_price}")
    df = await get_kline_data(symbol, interval='1m', limit=22)
    if df is None or len(df) < 22:
        logger.warning(f"Dados insuficientes para {symbol}, tamanho do dataframe: {len(df) if df is not None else 'None'}")
        return False
    df['ema7'] = df['close'].ewm(span=7).mean()
    df['ema21'] = df['close'].ewm(span=21).mean()
    logger.info(f"Condições para {symbol.upper()} - EMA7: {df['ema7'].iloc[-1]:.4f} | EMA21: {df['ema21'].iloc[-1]:.4f}")
    diff = abs(df['ema7'].iloc[-1] - df['ema21'].iloc[-1]) / df['ema21'].iloc[-1]
    if diff < EMA_DIFF_THRESHOLD:
        logger.info(f"Diferença EMA insuficiente para {symbol}: {diff:.4f} < {EMA_DIFF_THRESHOLD}")
        return False
    if CHECK_TREND_CONSISTENCY:
        trend_consistent = False
        if df['ema7'].iloc[-1] > df['ema21'].iloc[-1]:
            trend_consistent = all(df['ema7'].tail(3) > df['ema21'].tail(3))
        elif df['ema7'].iloc[-1] < df['ema21'].iloc[-1]:
            trend_consistent = all(df['ema7'].tail(3) < df['ema21'].tail(3))
        if not trend_consistent:
            logger.info(f"Tendência não consistente para {symbol}")
            return False
    logger.info(f"Condições de trading atendidas para {symbol}")
    return True

def can_place_order(symbol, order_type):
    try:
        positions = binance_client.get_position_risk(symbol=symbol.upper())
        for pos in positions:
            position_amt = float(pos['positionAmt'])
            if position_amt != 0:
                is_long = position_amt > 0
                if (order_type == 'long' and not is_long) or (order_type == 'short' and is_long):
                    logger.warning(f"Conflito de posição para {symbol}: posição atual {'LONG' if is_long else 'SHORT'}, tentativa de {order_type.upper()}")
                    return False
        return True
    except ClientError as e:
        logger.error(f"Erro ao verificar posições para {symbol}: {e}")
        return False

def place_order(order_type, entry_price, symbol, client=None, groups=None):
    global first_sol_order
    logger.info(f"Colocando ordem {order_type.upper()} para {symbol}, preço de entrada: {entry_price}")
    symbol = symbol.upper()
    if get_open_positions(symbol, order_type) > 0:
        logger.warning(f"Já existe posição aberta para {symbol} ({order_type}), pulando nova ordem")
        return f"⚠️ Já existe posição aberta para {symbol} ({order_type})"
    if entry_price is None:
        logger.warning(f"Preço inválido para {symbol}, pulando operação")
        return f"⚠️ Preço inválido para {symbol}, pulando operação"
    margin = TOTAL_MARGIN
    if not check_balance_sufficiency(symbol, order_type, margin):
        msg = f"⚠️ Saldo insuficiente para {symbol} {order_type.upper()}: margem necessária={margin:.2f} USDT"
        logger.warning(msg)
        return msg
    messages = []
    precision = get_symbol_precision(symbol)
    for i, (pct, offset) in enumerate(zip(LAYER_PCTS, LAYER_OFFSETS), 1):
        entry = entry_price * (1 - offset) if order_type == 'long' else entry_price * (1 + offset)
        layer_margin = margin * pct
        qty = round((layer_margin * LEVERAGE) / entry, precision)
        if qty * entry < 5:
            logger.warning(f"Quantidade {qty} muito baixa para {symbol}, valor notional {qty * entry:.2f} < 5 USDT, pulando camada {i}")
            messages.append(f"⚠️ Camada {i}/{len(LAYER_PCTS)} pulada: valor notional insuficiente")
            continue
        order_id = str(uuid.uuid4())
        order_data = {
            'order_id': order_id,
            'type': order_type,
            'entry': entry,
            'amount': qty,
            'cost': layer_margin,
            'open_time': datetime.now(UTC),
            'layer': i
        }
        if SIMULATED:
            orders[symbol][order_type].append(order_data)
            logger.info(f"Ordem simulada colocada: {order_data}")
        else:
            side = 'BUY' if order_type == 'long' else 'SELL'
            try:
                if can_place_order(symbol, order_type):
                    response = binance_client.new_order(
                        symbol=symbol,
                        side=side,
                        type='MARKET',
                        quantity=qty,
                        positionSide=order_type.upper(),
                        newClientOrderId=order_id
                    )
                    order_data['binance_order_id'] = response['orderId']
                    orders[symbol][order_type].append(order_data)
                    logger.info(f"Ordem real colocada: {response}")
                else:
                    msg = f"⚠️ Ordem não colocada para {symbol}: conflito de posição"
                    messages.append(msg)
                    logger.warning(msg)
                    continue
            except ClientError as e:
                msg = f"❌ Erro camada {i}/{len(LAYER_PCTS)}: {e}"
                messages.append(msg)
                logger.error(f"Erro ao colocar ordem real: {msg}")
                continue
        if symbol == 'SOLUSDT' and first_sol_order is None:
            first_sol_order = order_data
        msg = f"✅ ORDEM EXECUTADA: {symbol} {order_type.upper()}\nCamada {i}/{len(LAYER_PCTS)}\nQTD: {qty}"
        messages.append(msg)
        trade_log = {
            "timestamp": str(datetime.now(UTC)),
            "symbol": symbol,
            "type": order_type,
            "layer": i,
            "qty": qty,
            "price": entry,
            "simulated": SIMULATED,
            "order_id": order_id
        }
        save_trade_history(trade_log)
        logger.info(f"ORDEM COLOCADA - {symbol} - {order_type.upper()} - Camada {i} - Preço: {entry:.4f} - Quantidade: {qty}")
        try:
            info = layer_info[symbol.lower()]
            if info['opened_layers'] == 0:
                info['entry_price'] = entry_price
                info['order_type'] = order_type
            info['opened_layers'] += 1
        except Exception as e:
            logger.error(f"Erro ao atualizar layer_info para {symbol}: {e}")
    if messages and client and groups:
        asyncio.create_task(send_telegram(client, "\n".join(messages), groups, image_type=order_type, is_critical=True))
    logger.info(f"Ordem colocada: {symbol} {order_type.upper()} Camada Inicial")
    return "\n".join(messages)

def close_order(order, current_price, symbol, client=None, groups=None):
    global total_gain, trade_count, total_loss_count, sim_balance, sim_daily_gain, first_sol_order
    logger.info(f"Fechando ordem para {symbol}, tipo: {order['type']}, camada: {order['layer']}, preço atual: {current_price}")
    symbol = symbol.upper()
    gain = order['amount'] * (current_price - order['entry']) if order['type'] == 'long' else order['amount'] * (order['entry'] - current_price)
    gain *= (1 - FEE_RATE)
    if SIMULATED:
        total_gain += gain
        sim_daily_gain += gain
        sim_balance += order['cost'] + gain
    else:
        try:
            positions = binance_client.get_position_risk(symbol=symbol)
            position_amt = 0
            for pos in positions:
                amt = float(pos['positionAmt'])
                if (order['type'] == 'long' and amt > 0) or (order['type'] == 'short' and amt < 0):
                    position_amt = abs(amt)
                    break
            if position_amt >= order['amount']:
                side = 'SELL' if order['type'] == 'long' else 'BUY'
                short_order_id = order['order_id'].replace('-', '')[:32]
                client_order_id = f"close_{short_order_id}"[:36]
                binance_client.new_order(
                    symbol=symbol,
                    side=side,
                    type='MARKET',
                    quantity=order['amount'],
                    positionSide=order['type'].upper(),
                    reduceOnly=True,
                    newClientOrderId=client_order_id
                )
                logger.info(f"Ordem real fechada: {symbol} {order['type'].upper()} Camada {order['layer']}")
                realized_pnl = float(pos['realizedPnl']) if 'realizedPnl' in pos else gain
                total_gain += realized_pnl
                sim_daily_gain += realized_pnl
            else:
                logger.warning(f"Quantidade insuficiente para fechar ordem: {position_amt} < {order['amount']}")
                return f"⚠️ Não foi possível fechar ordem para {symbol}: quantidade insuficiente"
        except ClientError as e:
            logger.error(f"Erro ao fechar ordem real: {e}")
            return f"❌ Erro ao fechar ordem para {symbol}: {e}"
    trade_count += 1
    if gain < 0:
        total_loss_count += 1
    orders[symbol][order['type']].remove(order)
    if order == first_sol_order:
        first_sol_order = None
        layer_info[symbol.lower()]['opened_layers'] = 0
        layer_info[symbol.lower()]['entry_price'] = None
        layer_info[symbol.lower()]['order_type'] = None
    elif layer_info[symbol.lower()]['opened_layers'] > 0:
        layer_info[symbol.lower()]['opened_layers'] -= 1
        if layer_info[symbol.lower()]['opened_layers'] == 0:
            layer_info[symbol.lower()]['entry_price'] = None
            layer_info[symbol.lower()]['order_type'] = None
    percentual = (gain / (sim_balance + gain)) * 100 if SIMULATED else (gain / get_account_balance()) * 100
    display_balance = sim_balance * 10 if SIMULATED and INFLATE_PUBLIC_BALANCE else get_account_balance() * 10 if INFLATE_PUBLIC_BALANCE else get_account_balance()
    display_gain = gain * 10 if INFLATE_PUBLIC_BALANCE else gain
    msg = f"""❌ <b>Ordem FECHADA</b>
<b>Par:</b> {symbol}
<b>Tipo:</b> {order['type'].upper()}
<b>Camada:</b> {order['layer']}
<b>Ganho:</b> {display_gain:.2f} USDT ({percentual:.2f}%)
<b>Saldo Atual:</b> {display_balance:.2f} USDT
📈 <i>Operação realizada pelo bot VKINHA Trading</i>"""
    trade_log = {
        "timestamp": str(datetime.now(UTC)),
        "symbol": symbol,
        "type": order['type'],
        "layer": order['layer'],
        "gain": gain,
        "percentual": percentual,
        "balance": sim_balance if SIMULATED else get_account_balance(),
        "simulated": SIMULATED,
        "order_id": order['order_id']
    }
    save_trade_history(trade_log)
    if client and groups:
        asyncio.create_task(send_telegram(client, msg, groups, image_type=order['type'], is_critical=True))
    logger.info(f"Ordem fechada: {symbol} {order['type'].upper()}, Camada: {order['layer']}, Ganho: {gain:.2f} USDT")
    return msg

async def initial_test_operations(client, groups):
    global first_sol_order
    logger.info("Iniciando operação de teste inicial")
    symbol = 'SOLUSDT'
    entry_price = get_price_rest(symbol)
    if not entry_price:
        msg = "⚠️ Erro: Não foi possível obter o preço real do SOLUSDT"
        logger.error(msg)
        print(msg)
        await send_telegram(client, msg, groups, image_type='inf', is_initial=True, is_critical=True)
        return
    if not await check_trading_conditions(symbol, entry_price):
        msg = f"⚠️ Condições de trading não atendidas para teste em {symbol.upper()}"
        logger.warning(msg)
        print(msg)
        await send_telegram(client, msg, groups, image_type='inf', is_initial=True, is_critical=True)
        return
    msg_long = place_order('long', entry_price, symbol, client, groups)
    if msg_long and '✅' in msg_long:
        logger.info(f"Ordem de teste LONG enviada: {msg_long}")
        if first_sol_order:
            async def close_first_order():
                await asyncio.sleep(60)
                if first_sol_order and first_sol_order in orders[symbol]['long']:
                    close_price = latest_prices.get(symbol.lower())
                    if close_price is None:
                        close_price = get_price_rest(symbol) or first_sol_order['entry']
                    msg = close_order(first_sol_order, close_price, symbol, client, groups)
                    logger.info(f"Primeira ordem de teste fechada automaticamente: {msg}")
            asyncio.create_task(close_first_order())

def start_websocket(client, groups):
    logger.info("Iniciando WebSocket da Binance")
    print("🔗 Conectando ao WebSocket da Binance...")
    async def websocket_loop():
        while True:
            try:
                ws_client = UMFuturesWebsocketClient()
                def make_callback(symbol):
                    def callback(msg):
                        logger.info(f"Recebida mensagem WebSocket para {symbol}: {msg['e']}")
                        if msg['e'] == 'kline' and msg['k']['x']:
                            asyncio.create_task(handle_kline_async(msg, client, groups))
                    return callback

                for symbol in SYMBOLS:
                    ws_client.kline(symbol=symbol.lower(), interval="1m", callback=make_callback(symbol))
                    logger.info(f"WebSocket configurado para {symbol}")

                print("✅ WebSocket iniciado.")
                logger.info("WebSocket iniciado")
                while True:
                    await asyncio.sleep(300)
                    logger.info("WebSocket ativo, verificando conexão...")
                    print("🔍 WebSocket ativo...")
            except Exception as e:
                logger.error(f"Erro no WebSocket, reiniciando em 5s: {e}")
                print(f"⚠️ Erro no WebSocket, reiniciando em 5s: {e}")
                await asyncio.sleep(5)
    asyncio.create_task(websocket_loop())

async def handle_kline_async(msg, client, groups):
    logger.info(f"handle_kline_async disparado para: {msg['s']}")
    try:
        symbol = msg['s'].lower()
        close_price = float(msg['k']['c'])
        volume = float(msg['k']['v'])
        timestamp = int(msg['k']['t'])
        dt = datetime.fromtimestamp(timestamp / 1000.0, tz=UTC)
        data[symbol].append({'time': dt, 'close': close_price, 'volume': volume})
        if len(data[symbol]) > 22:
            data[symbol] = data[symbol][-22:]
        latest_prices[symbol] = close_price
        df = pd.DataFrame(data[symbol])
        df['ema7'] = df['close'].ewm(span=7).mean()
        df['ema21'] = df['close'].ewm(span=21).mean()
        ema7 = df['ema7'].iloc[-1]
        ema21 = df['ema21'].iloc[-1]
        diff = abs(ema7 - ema21) / ema21
        messages = []

        trading_conditions_met = await check_trading_conditions(symbol, close_price)
        if trading_conditions_met and diff >= EMA_DIFF_THRESHOLD:
            order_type = 'long' if ema7 > ema21 else 'short'
            logger.info(f"SINAL DETECTADO: {symbol.upper()} - Tipo: {order_type.upper()}")
            msg_order = place_order(order_type, close_price, symbol, client, groups)
            if msg_order:
                prefix = '📈 SINAL LONG' if order_type == 'long' else '📉 SINAL SHORT'
                messages.append(f"{prefix} {symbol.upper()}\n{msg_order}")
                logger.info(f"Enviando sinal: {prefix} {symbol.upper()}")

        tp_msgs = verificar_tp(symbol, client, groups)
        messages.extend(tp_msgs)

        for text in messages:
            await send_telegram(client, text, groups, image_type=('long' if 'LONG' in text else 'short' if 'SHORT' in text else 'inf'), is_critical=True)
    except Exception as e:
        logger.error(f"Erro ao processar mensagem WebSocket para {msg['s']}: {e}")
        print(f"Erro ao processar mensagem WebSocket para {msg['s']}: {e}")

def verificar_tp(symbol, client, groups):
    logger.info(f"Verificando take-profit/stop-loss para {symbol}")
    price = latest_prices.get(symbol.lower())
    messages = []
    if not price:
        logger.info(f"Preço indisponível para {symbol}")
        return messages
    for order_type in ['long', 'short']:
        ativos = orders[symbol.upper()][order_type]
        if not ativos:
            logger.info(f"Sem ordens ativas para {symbol} ({order_type})")
            continue
        for order in ativos[:]:
            if order == first_sol_order:
                continue
            change = (price - order['entry']) / order['entry'] if order['type'] == 'long' else (order['entry'] - price) / order['entry']
            if change >= TP_PCT or change <= -SL_PCT:
                msg = close_order(order, price, symbol, client, groups)
                messages.append(msg)
                logger.info(f"Take-profit/stop-loss atingido para {symbol}, ordem fechada: {msg}")
    return messages

async def monitor_account(client, groups):
    logger.info("Iniciando monitoramento da conta")
    while True:
        try:
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
                await send_telegram(client, msg, groups, image_type='inf', is_critical=True)
        except Exception as e:
            logger.error(f"Erro ao monitorar conta: {e}")
            print(f"Erro ao monitorar conta: {e}")
        await asyncio.sleep(7200)

async def log_status():
    logger.info("Iniciando log recorrente de status")
    while True:
        try:
            print("🔍 Monitorando gráficos para sinais de trade...")
            logger.info("Monitorando gráficos para sinais de trade...")
        except Exception as e:
            logger.error(f"Erro no log_status: {e}")
            print(f"Erro no log_status: {e}")
        await asyncio.sleep(60)

async def main():
    logger.info("Iniciando função principal do bot")
    global sim_balance, sim_daily_gain, total_gain, trade_count, total_loss_count, sim_day
    print(f"💰 Modo: {'REAL' if not SIMULATED else 'SIMULADO'} - Saldo inicial: {sim_balance:.2f} USDT")
    logger.info(f"Modo: {'REAL' if not SIMULATED else 'SIMULADO'} - Saldo inicial: {sim_balance:.2f} USDT")
    client = None
    groups = []
    try:
        client = await connect_telegram()
        async with client:
            groups = await get_all_groups(client)
            modo = "SIMULATED" if SIMULATED else "REAL"
            await send_telegram(client, f"✅ VKINHA Trading iniciado em modo {modo} 🚀", groups, image_type='inf', is_initial=True, is_critical=True)
            logger.info(f"Bot iniciado em modo {modo}")
            print("Loading dados da conta...")
            logger.info("Loading dados da conta...")
            try:
                binance_client.ping()
                print("✅ Conexão com Binance estabelecida")
                logger.info("Conexão com Binance estabelecida")
            except Exception as e:
                msg = f"⚠️ Erro ao conectar com a Binance: {e}"
                print(msg)
                logger.error(msg)
                await send_telegram(client, msg, groups, image_type='inf', is_initial=True, is_critical=True)
                raise
            summary = await get_futures_summary()
            if not summary and not SIMULATED:
                msg = "❌ Falha crítica: Não foi possível obter o resumo da conta. Verifique a API Key, permissões de futuros ou conexão com a Binance."
                print(msg)
                logger.error(msg)
                await send_telegram(client, msg, groups, image_type='inf', is_initial=True, is_critical=True)
                raise ValueError(msg)
            msg = format_summary(summary)
            print(msg)
            logger.info(msg)
            await send_telegram(client, msg, groups, image_type='inf', is_initial=True, is_critical=True)
            print("Sincronizado...")
            logger.info("Sincronizado...")
            start_websocket(client, groups)
            await initial_test_operations(client, groups)
            asyncio.create_task(monitor_account(client, groups))
            asyncio.create_task(log_status())
            await client.run_until_disconnected()
    except Exception as e:
        logger.error(f"Erro no main: {e}")
        print(f"Erro no main: {e}")
        if client:
            await send_telegram(client, f"❌ Erro no bot: {e}", groups, image_type='inf', is_initial=True, is_critical=True)

if __name__ == '__main__':
    logger.info("Iniciando execução do bot VKINHA Trading")
    try:
        asyncio.run(main())
        print("✅ VKINHA Trading finalizado 🚀")
        logger.info("VKINHA Trading finalizado")
    except Exception as e:
        logger.error(f"Erro fatal na execução do bot: {e}")
        print(f"Erro fatal na execução do bot: {e}")
        raise