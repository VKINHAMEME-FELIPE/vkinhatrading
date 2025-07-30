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
type_logger = logging.getLogger(__name__)
type_logger.setLevel(logging.DEBUG)
file_handler = RotatingFileHandler('trading.log', maxBytes=5*1024*1024, backupCount=3)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
type_logger.addHandler(file_handler)
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
type_logger.addHandler(console_handler)
logger = type_logger

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
SAFETY_MARGIN = 0
last_telegram_time = time.time()
last_critical_telegram_time = time.time()

# Verificação de variáveis de ambiente
logger.info("Verificando variáveis de ambiente")
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
TOTAL_MARGIN = 10
TP_PCT = 0.008
SL_PCT = 0.02
FEE_RATE = 0.0004
LAYER_PCTS = [0.2, 0.3, 0.5]
LAYER_OFFSETS = [0.001, 0.003, 0.006]
EMA_DIFF_THRESHOLD = 0.0003
TRADE_HISTORY_FILE = "trade_history.json"
CHECK_TREND_CONSISTENCY = False
logger.info("Constantes de configuração inicializadas: %s", SYMBOLS)

# Inicialização do cliente Binance
logger.info("Inicializando cliente Binance")
try:
    binance_client = UMFutures(key=BINANCE_API_KEY, secret=BINANCE_API_SECRET)
    logger.info("Cliente Binance inicializado com sucesso")
except ClientError as e:
    logger.error("Erro ao inicializar cliente Binance: %s", e)
    raise ValueError(f"Erro ao inicializar cliente Binance: {e}")

# Dados de estado do bot
orders = {sym.upper(): {'long': [], 'short': []} for sym in SYMBOLS}
latest_prices = {sym: None for sym in SYMBOLS}
data = {sym: [] for sym in SYMBOLS}
first_sol_order = None
layer_info = {sym: {'entry_price': None, 'opened_layers': 0, 'order_type': None} for sym in SYMBOLS}
prev_emas = {sym: {'ema7': None, 'ema21': None} for sym in SYMBOLS}
sim_balance = 596.64
sim_daily_gain = 0
total_gain = 0
trade_count = 0
total_loss_count = 0
sim_day = date.today()
logger.info("Estruturas de dados iniciais configuradas")

def validate_symbols():
    logger.info("Validando símbolos de negociação")
    try:
        info = binance_client.exchange_info()
        valid = {s['symbol'].lower() for s in info['symbols']}
        invalid = [s for s in SYMBOLS if s not in valid]
        if invalid:
            logger.error("Pares inválidos encontrados: %s", invalid)
            raise ValueError(f"Pares inválidos: {invalid}")
        logger.info("Todos os pares de negociação são válidos")
    except ClientError as e:
        logger.error("Erro ao validar pares: %s", e)
        raise

def set_hedge_mode(symbol):
    logger.info("Configurando Hedge Mode para %s", symbol)
    try:
        position_mode = binance_client.get_position_mode()
        if not position_mode.get('dualSidePosition', False):
            binance_client.change_position_mode(dualSidePosition=True)
            logger.info("Hedge Mode configurado para %s", symbol)
        else:
            logger.info("Hedge Mode já configurado para %s", symbol)
    except ClientError as e:
        if e.error_code == -4059:
            logger.info("Hedge Mode já configurado para %s, ignorando erro: %s", symbol, e)
        else:
            logger.error("Erro ao configurar Hedge Mode para %s: %s", symbol, e)
            raise
    except Exception as e:
        logger.error("Erro inesperado ao configurar Hedge Mode para %s: %s", symbol, e)
        raise

def get_symbol_precision(symbol):
    logger.info("Obtendo precisão para %s", symbol)
    try:
        exchange_info = binance_client.exchange_info()
        for s in exchange_info['symbols']:
            if s['symbol'].lower() == symbol.lower():
                logger.info("Precisão encontrada para %s: %d", symbol, s['quantityPrecision'])
                return s['quantityPrecision']
        logger.warning("Precisão não encontrada para %s, usando padrão 3", symbol)
        return 3
    except Exception as e:
        logger.error("Erro ao obter precisão para %s: %s", symbol, e)
        return 3

logger.info("Iniciando configuração de símbolos e alavancagem")
try:
    validate_symbols()
    for sym in SYMBOLS:
        logger.info("Configurando Hedge Mode e alavancagem para %s", sym)
        pos_mode = binance_client.get_position_mode()
        if not pos_mode.get('dualSidePosition', False):
            binance_client.change_position_mode(dualSidePosition=True)
        binance_client.change_leverage(symbol=sym.upper(), leverage=LEVERAGE)
        logger.info("Hedge Mode e alavancagem configurados para %s", sym)
    logger.info("Hedge Mode e alavancagem configurados para todos os símbolos")
except Exception as e:
    logger.error("Erro na configuração de símbolos: %s", e)
    raise

def can_place_order(symbol, order_type):
    logger.info("Verificando se é possível colocar ordem para %s (%s)", symbol, order_type)
    logger.info("Hedge Mode ativo, permitindo ordem para %s (%s)", symbol, order_type)
    return True

def get_account_balance():
    logger.info("Obtendo saldo da conta")
    try:
        if SIMULATED:
            logger.info("Modo simulado: saldo simulado = %.2f USDT", sim_balance)
            return sim_balance
        balances = binance_client.balance()
        usdt_balance = next((item for item in balances if item["asset"] == "USDT"), None)
        if usdt_balance:
            balance = float(usdt_balance["balance"])
            logger.info("Saldo USDT obtido: %.2f", balance)
            return balance
        logger.warning("USDT não encontrado na lista de saldos")
        return 0
    except Exception as e:
        logger.error("Erro ao obter saldo da conta: %s", e)
        print(f"Erro ao obter saldo da conta: {e}")
        return sim_balance

def check_balance_sufficiency(symbol, order_type, margin_needed):
    logger.info("Verificando suficiência de saldo para %s (%s), margem necessária: %.2f", symbol, order_type, margin_needed)
    try:
        if SIMULATED:
            logger.info("Modo simulado: saldo suficiente assumido")
            return True
        balance = get_account_balance()
        total_used_margin = 0
        for s in SYMBOLS:
            positions = binance_client.get_position_risk(symbol=s.upper())
            logger.debug("Resposta da API de posições para %s: %s", s, positions)
            for pos in positions:
                margin = float(pos['isolatedMargin']) if float(pos['positionAmt']) != 0 else 0
                total_used_margin += margin
        available_balance = balance - total_used_margin
        safety_threshold = balance * SAFETY_MARGIN
        if available_balance < margin_needed + safety_threshold:
            logger.warning("Saldo insuficiente para %s (%s): disponível=%.2f, necessário=%.2f, reserva=%.2f", 
                          symbol, order_type, available_balance, margin_needed, safety_threshold)
            return False
        logger.info("Saldo suficiente para %s (%s): disponível=%.2f, necessário=%.2f", 
                   symbol, order_type, available_balance, margin_needed)
        return True
    except Exception as e:
        logger.error("Erro ao verificar saldo para %s: %s", symbol, e)
        return False

async def get_futures_summary(max_retries=3, retry_delay=5):
    logger.info("Obtendo resumo da conta de futuros")
    if SIMULATED:
        logger.info("Modo simulado: retornando saldo simulado")
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
            logger.info("Resumo da conta obtido: %s", summary)
            return summary
        except ClientError as e:
            logger.error("Tentativa %d/%d - Erro ao obter resumo da conta: %s", attempt + 1, max_retries, e)
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
            continue
        except Exception as e:
            logger.error("Tentativa %d/%d - Erro inesperado ao obter resumo da conta: %s", attempt + 1, max_retries, e)
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
    logger.info("Resumo formatado com sucesso: %s", formatted)
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
                logger.error("Erro ao autenticar: %s", e)
                print(f"Erro ao autenticar: {e}")
                raise
        else:
            logger.info("Usuário já autorizado")
            print("✅ Usuário já autorizado!")
        return client
    except Exception as e:
        logger.error("Erro ao conectar ao Telegram: %s", e)
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
                        logger.info("Grupo encontrado: ID=%s, Nome=%s", dialog.id, dialog.title)
                        print(f"Grupo encontrado: ID={dialog.id}, Nome={dialog.title}")
                    else:
                        logger.warning("Sem permissão para enviar mensagens no grupo: ID=%s, Nome=%s", dialog.id, dialog.title)
                        print(f"Sem permissão no grupo: ID={dialog.id}, Nome={dialog.title}")
                except Exception as e:
                    logger.warning("Erro ao verificar permissões no grupo %s (%s): %s", dialog.id, dialog.title, e)
                    print(f"Erro ao verificar grupo {dialog.id} ({dialog.title}): {e}")
        logger.info("Encontrados %d grupos para envio de mensagens", len(groups))
        print(f"Encontrados {len(groups)} grupos para envio de mensagens")
        return groups or ['me']
    except Exception as e:
        logger.error("Erro ao obter grupos: %s", e)
        print(f"Erro ao obter grupos: {e}")
        return ['me']

async def send_telegram(client, message, groups, image_type='inf', is_initial=False, is_critical=False):
    global last_telegram_time, last_critical_telegram_time
    logger.info("Tentando enviar mensagem: %s, Tipo de imagem: %s, Inicial: %s, Crítica: %s", 
                message, image_type, is_initial, is_critical)
    current_time = time.time()
    if not is_initial and not is_critical and current_time - last_telegram_time < 60:
        logger.warning("Evitando flood no Telegram, mensagem não enviada: %s", message)
        print(f"⚠️ Evitando flood no Telegram, mensagem não enviada: {message}")
        return
    if is_critical and current_time - last_critical_telegram_time < 30:
        logger.warning("Evitando flood de mensagens críticas no Telegram: %s", message)
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
                    logger.info("URL da imagem válida: %s", image_url)
                    await client.send_file(group_id, image_url, caption=message)
                    logger.info("Mensagem com imagem enviada ao grupo %s", group_id)
                    print(f"Mensagem com imagem enviada ao grupo {group_id}")
                except Exception as e:
                    logger.error("Erro ao validar URL da imagem %s: %s", image_url, e)
                    print(f"⚠️ Erro ao validar URL da imagem: {e}")
                    await client.send_message(group_id, message)
                    logger.info("Mensagem de texto enviada ao grupo %s (sem imagem)", group_id)
                    print(f"Mensagem enviada ao grupo {group_id} (sem imagem)")
            else:
                await client.send_message(group_id, message)
                logger.info("Mensagem enviada ao grupo %s", group_id)
                print(f"Mensagem enviada ao grupo {group_id}")
        except Exception as e:
            logger.error("Erro ao enviar mensagem ao grupo %s: %s", group_id, e)
            print(f"Erro ao enviar mensagem ao grupo {group_id}: {e}")
    if is_critical:
        last_critical_telegram_time = current_time
        logger.info("last_critical_telegram_time atualizado para: %s", last_critical_telegram_time)
    else:
        last_telegram_time = current_time
        logger.info("last_telegram_time atualizado para: %s", last_telegram_time)

def save_trade_history(entry):
    logger.info("Salvando entrada no histórico: %s", entry)
    try:
        if os.path.exists(TRADE_HISTORY_FILE):
            with open(TRADE_HISTORY_FILE, "r") as f:
                data = json.load(f)
        else:
            data = []
        data.append(entry)
        with open(TRADE_HISTORY_FILE, "w") as f:
            json.dump(data, f, indent=4)
        logger.info("Histórico salvo com sucesso: %s", entry)
    except Exception as e:
        logger.error("Erro ao salvar histórico: %s", e)
        print(f"Erro ao salvar histórico: {e}")

def get_open_positions(symbol, order_type):
    logger.info("Verificando posições abertas para %s (%s)", symbol, order_type)
    try:
        if SIMULATED:
            count = len(orders[symbol.upper()][order_type])
            logger.info("Modo simulado: %d posições abertas para %s (%s)", count, symbol, order_type)
            return count
        positions = binance_client.get_position_risk(symbol=symbol.upper())
        logger.debug("Resposta da API de posições para %s (%s): %s", symbol, order_type, positions)
        for pos in positions:
            position_amt = float(pos['positionAmt'])
            if (order_type == 'long' and position_amt > 0.01) or (order_type == 'short' and position_amt < -0.01):
                amount = abs(position_amt)
                logger.info("Posição aberta encontrada para %s (%s): %.4f", symbol, order_type, amount)
                return amount
        logger.info("Nenhuma posição aberta para %s (%s)", symbol, order_type)
        return 0
    except Exception as e:
        logger.error("Erro ao verificar posições abertas para %s (%s): %s", symbol, order_type, e)
        print(f"Erro ao verificar posições abertas para {symbol} ({order_type}): {e}")
        return 0

def get_price_rest(symbol):
    logger.info("Obtendo preço via REST para %s", symbol)
    try:
        data = binance_client.mark_price(symbol=symbol.upper())
        price = float(data['markPrice'])
        logger.info("Preço obtido para %s: %.4f", symbol, price)
        return price
    except Exception as e:
        logger.error("Erro ao obter preço via REST para %s: %s", symbol, e)
        print(f"Erro ao obter preço via REST para {symbol}: {e}")
        return None

async def get_kline_data(symbol, interval='1m', limit=22):
    logger.info("Obtendo dados de klines para %s, intervalo: %s, limite: %d", symbol, interval, limit)
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
        logger.info("Dados de klines obtidos para %s, %d candles", symbol, len(df))
        return df
    except Exception as e:
        logger.error("Erro ao obter dados de klines para %s: %s", symbol, e)
        print(f"Erro ao obter dados de klines para {symbol}: {e}")
        return None

async def check_trading_conditions(symbol, close_price):
    logger.info("Verificando condições de trading para %s, preço de fechamento: %.4f", symbol, close_price)
    df = await get_kline_data(symbol, interval='1m', limit=22)
    if df is None or len(df) < 22:
        logger.warning("Dados insuficientes para %s, tamanho do dataframe: %s", 
                      symbol, len(df) if df is not None else 'None')
        return False
    df['ema7'] = df['close'].ewm(span=7).mean()
    df['ema21'] = df['close'].ewm(span=21).mean()
    ema7 = df['ema7'].iloc[-1]
    ema21 = df['ema21'].iloc[-1]
    logger.info("Condições para %s - EMA7: %.4f | EMA21: %.4f", symbol.upper(), ema7, ema21)
    diff = abs(ema7 - ema21) / ema21
    logger.debug("Diferença EMA para %s: %.4f", symbol, diff)
    if diff < EMA_DIFF_THRESHOLD:
        logger.info("Sem sinal para %s: Diferença EMA insuficiente: %.4f < %.4f", symbol, diff, EMA_DIFF_THRESHOLD)
        return False
    if CHECK_TREND_CONSISTENCY:
        trend_consistent = False
        if ema7 > ema21:
            trend_consistent = all(df['ema7'].tail(3) > df['ema21'].tail(3))
        elif ema7 < ema21:
            trend_consistent = all(df['ema7'].tail(3) < df['ema21'].tail(3))
        logger.debug("Consistência de tendência para %s: %s", symbol, trend_consistent)
        if not trend_consistent:
            logger.info("Sem sinal para %s: Tendência não consistente", symbol)
            return False
    logger.info("Condições de trading atendidas para %s", symbol)
    return True

def place_order(order_type, entry_price, symbol, client=None, groups=None, test_mode=False):
    global first_sol_order
    logger.info("Iniciando colocação de ordem %s para %s, preço de entrada: %.4f, test_mode: %s", order_type.upper(), symbol, entry_price, test_mode)
    symbol = symbol.upper()
    logger.info("Verificando posições abertas para %s (%s)", symbol, order_type)
    if get_open_positions(symbol, order_type) > 0:
        logger.warning("Pulando entrada para %s (%s), já tem posição aberta!", symbol, order_type)
        print(f"⚠️ Pulando entrada para {symbol} ({order_type}), já tem posição aberta!")
        return f"⚠️ Já existe posição aberta para {symbol} ({order_type})"
    if entry_price is None:
        logger.warning("Preço inválido para %s, pulando operação", symbol)
        return f"⚠️ Preço inválido para {symbol}, pulando operação"
    margin = TOTAL_MARGIN
    logger.info("Verificando suficiência de saldo para %s (%s)", symbol, order_type)
    if not check_balance_sufficiency(symbol, order_type, margin):
        msg = f"⚠️ Saldo insuficiente para {symbol} {order_type.upper()}: margem necessária={margin:.2f} USDT"
        logger.warning(msg)
        return msg
    logger.info("Decidindo abrir ordem para %s (%s)", symbol, order_type)
    messages = []
    precision = get_symbol_precision(symbol)
    
    if test_mode:
        pct = LAYER_PCTS[0]
        offset = LAYER_OFFSETS[0]
        entry = entry_price * (1 - offset) if order_type == 'long' else entry_price * (1 + offset)
        layer_margin = TOTAL_MARGIN * pct
        qty = round((layer_margin * LEVERAGE) / entry, precision)
        if qty * entry < 5:
            logger.warning("Quantidade %s muito baixa para %s, valor notional %.2f < 5 USDT, pulando camada", 
                          qty, symbol, qty * entry)
            return "⚠️ Camada pulada: valor notional insuficiente"
        logger.info("Tentando abrir ordem de teste %s para %s: QTD=%.4f, Preço=%.4f", 
                   order_type.upper(), symbol, qty, entry)
        order_id = str(uuid.uuid4())
        order_data = {
            'order_id': order_id,
            'type': order_type,
            'entry': entry,
            'amount': qty,
            'cost': layer_margin,
            'open_time': datetime.now(UTC),
            'layer': 1
        }
        if SIMULATED:
            orders[symbol][order_type].append(order_data)
            logger.info("Ordem simulada de teste colocada com sucesso: %s", order_data)
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
                    logger.info("Ordem real de teste colocada com sucesso: %s", response)
                else:
                    msg = f"⚠️ Ordem de teste não colocada para {symbol}: conflito de posição"
                    logger.warning(msg)
                    return msg
            except ClientError as e:
                msg = f"❌ Erro ao colocar ordem de teste: {e}"
                logger.error(msg)
                return msg
        if symbol == 'SOLUSDT' and first_sol_order is None:
            first_sol_order = order_data
            logger.info("Primeira ordem SOLUSDT de teste registrada: %s", order_data)
        msg = f"✅ ORDEM DE TESTE EXECUTADA: {symbol} {order_type.upper()}\nCamada 1/1\nQTD: {qty}"
        messages.append(msg)
        trade_log = {
            "timestamp": str(datetime.now(UTC)),
            "symbol": symbol,
            "type": order_type,
            "layer": 1,
            "qty": qty,
            "price": entry,
            "simulated": SIMULATED,
            "order_id": order_id,
            "test_mode": True
        }
        save_trade_history(trade_log)
        logger.info("ORDEM DE TESTE COLOCADA - %s - %s - Camada 1 - Preço: %.4f - Quantidade: %s", 
                   symbol, order_type.upper(), entry, qty)
    else:
        for i, (pct, offset) in enumerate(zip(LAYER_PCTS, LAYER_OFFSETS), 1):
            entry = entry_price * (1 - offset) if order_type == 'long' else entry_price * (1 + offset)
            layer_margin = margin * pct
            qty = round((layer_margin * LEVERAGE) / entry, precision)
            if qty * entry < 5:
                logger.warning("Quantidade %s muito baixa para %s, valor notional %.2f < 5 USDT, pulando camada %d", 
                              qty, symbol, qty * entry, i)
                messages.append(f"⚠️ Camada {i}/{len(LAYER_PCTS)} pulada: valor notional insuficiente")
                continue
            logger.info("Tentando abrir ordem %s para %s: camada=%d, QTD=%.4f, Preço=%.4f", 
                       order_type.upper(), symbol, i, qty, entry)
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
                logger.info("Ordem simulada colocada com sucesso: %s", order_data)
            else:
                side = 'BUY' if order_type == 'long' else 'SELL'
                logger.info("Verificando possibilidade de colocar ordem para %s (%s)", symbol, order_type)
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
                        logger.info("Ordem real colocada com sucesso: %s", response)
                    else:
                        msg = f"⚠️ Ordem não colocada para {symbol}: conflito de posição"
                        messages.append(msg)
                        logger.warning(msg)
                        continue
                except ClientError as e:
                    msg = f"❌ Erro camada {i}/{len(LAYER_PCTS)}: {e}"
                    messages.append(msg)
                    logger.error("Erro ao colocar ordem real: %s", msg)
                    continue
            if symbol == 'SOLUSDT' and first_sol_order is None:
                first_sol_order = order_data
                logger.info("Primeira ordem SOLUSDT registrada: %s", order_data)
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
            logger.info("ORDEM COLOCADA - %s - %s - Camada %d - Preço: %.4f - Quantidade: %s", 
                       symbol, order_type.upper(), i, entry, qty)
            try:
                info = layer_info[symbol.lower()]
                if info['opened_layers'] == 0:
                    info['entry_price'] = entry_price
                    info['order_type'] = order_type
                info['opened_layers'] += 1
                logger.info("layer_info atualizado para %s: %s", symbol, info)
            except Exception as e:
                logger.error("Erro ao atualizar layer_info para %s: %s", symbol, e)
    
    if messages and client and groups:
        asyncio.create_task(send_telegram(client, "\n".join(messages), groups, image_type=order_type, is_critical=True))
    logger.info("Ordem colocada: %s %s Camada Inicial", symbol, order_type.upper())
    return "\n".join(messages)

def close_order(order, current_price, symbol, client=None, groups=None):
    global total_gain, trade_count, total_loss_count, sim_balance, sim_daily_gain, first_sol_order
    logger.info("Fechando ordem para %s, tipo: %s, camada: %d, preço atual: %.4f", 
                symbol, order['type'], order['layer'], current_price)
    symbol = symbol.upper()
    gain = order['amount'] * (current_price - order['entry']) if order['type'] == 'long' else order['amount'] * (order['entry'] - current_price)
    gain *= (1 - FEE_RATE)
    if SIMULATED:
        total_gain += gain
        sim_daily_gain += gain
        sim_balance += order['cost'] + gain
        logger.info("Modo simulado: ganho=%.2f, saldo atualizado=%.2f", gain, sim_balance)
    else:
        try:
            positions = binance_client.get_position_risk(symbol=symbol)
            logger.debug("Resposta da API de posições para %s ao fechar ordem: %s", symbol, positions)
            position_amt = 0
            for pos in positions:
                amt = float(pos['positionAmt'])
                if (order['type'] == 'long' and amt > 0.01) or (order['type'] == 'short' and amt < -0.01):
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
                    newClientOrderId=client_order_id
                )
                logger.info("Ordem real fechada com sucesso: %s %s Camada %d", symbol, order['type'].upper(), order['layer'])
                realized_pnl = float(pos['realizedPnl']) if 'realizedPnl' in pos else gain
                total_gain += realized_pnl
                sim_daily_gain += realized_pnl
                logger.info("Ganho real: %.2f, total_gain: %.2f", realized_pnl, total_gain)
            else:
                logger.warning("Quantidade insuficiente para fechar ordem: %.4f < %.4f", position_amt, order['amount'])
                return f"⚠️ Não foi possível fechar ordem para {symbol}: quantidade insuficiente"
        except ClientError as e:
            logger.error("Erro ao fechar ordem real: %s", e)
            return f"❌ Erro ao fechar ordem para {symbol}: {e}"
    trade_count += 1
    if gain < 0:
        total_loss_count += 1
    orders[symbol][order['type']].remove(order)
    logger.info("Ordem removida do rastreamento: %s (%s)", symbol, order['type'])
    if order == first_sol_order:
        first_sol_order = None
        layer_info[symbol.lower()]['opened_layers'] = 0
        layer_info[symbol.lower()]['entry_price'] = None
        layer_info[symbol.lower()]['order_type'] = None
        logger.info("Primeira ordem SOLUSDT fechada, layer_info resetado")
    elif layer_info[symbol.lower()]['opened_layers'] > 0:
        layer_info[symbol.lower()]['opened_layers'] -= 1
        if layer_info[symbol.lower()]['opened_layers'] == 0:
            layer_info[symbol.lower()]['entry_price'] = None
            layer_info[symbol.lower()]['order_type'] = None
        logger.info("layer_info atualizado para %s: %s", symbol, layer_info[symbol.lower()])
    percentual = (gain / (sim_balance + gain)) * 100 if SIMULATED else (gain / get_account_balance()) * 100
    display_balance = sim_balance * 10 if SIMULATED and INFLATE_PUBLIC_BALANCE else get_account_balance() * 10 if INFLATE_PUBLIC_BALANCE else get_account_balance()
    display_gain = gain * 13 if INFLATE_PUBLIC_BALANCE else gain
    msg = f"""❌ **Ordem FECHADA**
**Par:** {symbol}
**Tipo:** {order['type'].upper()}
**Camada:** {order['layer']}
**Ganho:** {display_gain:.2f} USDT ({percentual:.2f}%)
**Saldo Atual:** {display_balance:.2f} USDT
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
    logger.info("Ordem fechada: %s %s, Camada: %d, Ganho: %.2f USDT", symbol, order['type'].upper(), order['layer'], gain)
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
    logger.info("Executando primeira operação de teste para %s", symbol)
    order_type = 'long'
    msg_test = place_order(order_type, entry_price, symbol, client, groups, test_mode=True)
    if msg_test and '✅' in msg_test:
        logger.info("Ordem de teste enviada: %s", msg_test)
        if first_sol_order:
            async def close_first_order():
                await asyncio.sleep(60)
                if first_sol_order and first_sol_order in orders[symbol]['long']:
                    close_price = latest_prices.get(symbol.lower())
                    if close_price is None:
                        close_price = get_price_rest(symbol) or first_sol_order['entry']
                    logger.info("Fechando primeira ordem de teste para %s, preço: %.4f", symbol, close_price)
                    msg = close_order(first_sol_order, close_price, symbol, client, groups)
                    logger.info("Primeira ordem de teste fechada automaticamente: %s", msg)
            asyncio.create_task(close_first_order())

def start_websocket(client, groups):
    logger.info("Iniciando WebSocket da Binance")
    print("🔗 Conectando ao WebSocket da Binance...")
    async def websocket_loop():
        while True:
            try:
                ws_client = UMFuturesWebsocketClient()
                logger.info("Iniciando WebSocket para todos os símbolos")
                def make_callback(symbol):
                    def callback(msg):
                        print(f"=== RECEBI UM KLINE! === Symbol: {symbol}")
                        logger.info(f"Recebida mensagem WebSocket para {symbol}: {msg}")
                        if msg['e'] == 'kline':
                            if msg['k']['x']:
                                logger.info("Candle fechado para %s: %s", symbol, msg['k'])
                                asyncio.create_task(handle_kline_async(msg, client, groups))
                            else:
                                logger.debug("Mensagem ignorada para %s: candle não fechado", symbol)
                        else:
                            logger.debug("Mensagem ignorada para %s: tipo %s", symbol, msg['e'])
                    return callback

                for symbol in SYMBOLS:
                    logger.info("--- Configurando WebSocket para %s ---", symbol)
                    ws_client.kline(symbol=symbol.lower(), interval="1m", callback=make_callback(symbol))
                    logger.info("WebSocket configurado para %s", symbol)
                    logger.debug("WebSocket ativo para %s, aguardando mensagens", symbol)

                print("✅ WebSocket iniciado.")
                logger.info("WebSocket iniciado")
                while True:
                    await asyncio.sleep(300)
                    logger.info("WebSocket ativo, verificando conexão...")
                    print("🔍 WebSocket ativo...")
            except Exception as e:
                logger.error("Erro no WebSocket para %s, reiniciando em 5s: %s", symbol, e)
                print(f"⚠️ Erro no WebSocket para {symbol}, reiniciando em 5s: {e}")
                await asyncio.sleep(5)
    asyncio.create_task(websocket_loop())

async def handle_kline_async(msg, client, groups):
    print(f"=== RECEBI UM KLINE! === Symbol: {msg.get('s', 'N/A')}")
    logger.info(f"Iniciando processamento de kline para {msg.get('s', 'N/A').upper()}")
    symbol = msg['s'].lower()
    close_price = float(msg['k']['c'])
    volume = float(msg['k']['v'])
    logger.info("--- Processando %s --- Preço: %.4f, Volume: %.2f", symbol.upper(), close_price, volume)
    data[symbol].append({'time': datetime.fromtimestamp(msg['k']['t']/1000, tz=UTC), 'close': close_price, 'volume': volume})
    if len(data[symbol]) > 22:
        data[symbol] = data[symbol][-22:]
    logger.debug("Dados armazenados para %s: %d candles", symbol, len(data[symbol]))
    logger.debug("Últimos 5 candles para %s: %s", symbol, data[symbol][-5:])
    latest_prices[symbol] = close_price

    df = pd.DataFrame(data[symbol])
    df['ema7'] = df['close'].ewm(span=7).mean()
    df['ema21'] = df['close'].ewm(span=21).mean()
    ema7 = df['ema7'].iloc[-1]
    ema21 = df['ema21'].iloc[-1]
    diff = abs(ema7 - ema21) / ema21 if ema21 else 0
    logger.info(
        f"[{symbol.upper()}] Preço: {close_price:.4f} | EMA7: {ema7:.4f} | EMA21: {ema21:.4f} | Diff: {diff:.6f} | "
        + ("SEM TENDÊNCIA" if diff < EMA_DIFF_THRESHOLD else "TENDÊNCIA DETECTADA")
    )
    logger.debug(f"[{symbol.upper()}] Últimos 5 closes: {df['close'].tail(5).tolist()}")

    long_positions = get_open_positions(symbol.upper(), 'long')
    short_positions = get_open_positions(symbol.upper(), 'short')
    logger.info("Status da posição para %s - Long: %.4f, Short: %.4f", symbol.upper(), long_positions, short_positions)

    prev = prev_emas[symbol]
    if prev['ema7'] is not None and prev['ema21'] is not None:
        if prev['ema7'] > prev['ema21'] and ema7 < ema21:
            logger.info("Sinal de reversão detectado para %s: EMA7 (%.4f) cruzou abaixo de EMA21 (%.4f), fechando LONG e abrindo SHORT", 
                       symbol.upper(), ema7, ema21)
            for order in orders[symbol.upper()]['long'][:]:
                logger.info("Fechando ordem LONG para %s devido a crossover", symbol)
                msg_close = close_order(order, close_price, symbol, client, groups)
                asyncio.create_task(send_telegram(client, msg_close, groups, image_type='long', is_critical=True))
            layer_info[symbol]['opened_layers'] = 0
            layer_info[symbol]['entry_price'] = None
            layer_info[symbol]['order_type'] = None
            logger.info("layer_info resetado para %s após crossover DOWN", symbol)
            logger.info("Decidindo abrir ordem SHORT para %s", symbol)
            msg_rev = place_order('short', close_price, symbol, client, groups)
            asyncio.create_task(send_telegram(client, f"📉 REVERSAL SHORT {symbol.upper()}\n{msg_rev}", 
                                            groups, image_type='short', is_critical=True))
        elif prev['ema7'] < prev['ema21'] and ema7 > ema21:
            logger.info("Sinal de reversão detectado para %s: EMA7 (%.4f) cruzou acima de EMA21 (%.4f), fechando SHORT e abrindo LONG", 
                       symbol.upper(), ema7, ema21)
            for order in orders[symbol.upper()]['short'][:]:
                logger.info("Fechando ordem SHORT para %s devido a crossover", symbol)
                msg_close = close_order(order, close_price, symbol, client, groups)
                asyncio.create_task(send_telegram(client, msg_close, groups, image_type='short', is_critical=True))
            layer_info[symbol]['opened_layers'] = 0
            layer_info[symbol]['entry_price'] = None
            layer_info[symbol]['order_type'] = None
            logger.info("layer_info resetado para %s após crossover UP", symbol)
            logger.info("Decidindo abrir ordem LONG para %s", symbol)
            msg_rev = place_order('long', close_price, symbol, client, groups)
            asyncio.create_task(send_telegram(client, f"📈 REVERSAL LONG {symbol.upper()}\n{msg_rev}", 
                                            groups, image_type='long', is_critical=True))
        else:
            logger.info("Nenhum cruzamento de EMAs detectado para %s: EMA7=%.4f, EMA21=%.4f", symbol, ema7, ema21)

    prev['ema7'] = ema7
    prev['ema21'] = ema21
    logger.debug("Valores anteriores de EMA atualizados para %s: EMA7=%.4f, EMA21=%.4f", symbol, ema7, ema21)

    if diff < EMA_DIFF_THRESHOLD:
        logger.info("Nenhum sinal de entrada para %s: diferença EMA (%.6f) < threshold (%.6f)", 
                   symbol, diff, EMA_DIFF_THRESHOLD)
    else:
        order_type = 'long' if ema7 > ema21 else 'short'
        logger.info("SINAL DETECTADO para %s - Tipo: %s, EMA7: %.4f, EMA21: %.4f, Diff: %.6f", 
                   symbol.upper(), order_type.upper(), ema7, ema21, diff)
        msg_order = place_order(order_type, close_price, symbol, client, groups)
        prefix = '📈 SINAL LONG' if order_type == 'long' else '📉 SINAL SHORT'
        asyncio.create_task(send_telegram(client, f"{prefix} {symbol.upper()}\n{msg_order}", 
                                        groups, image_type=order_type, is_critical=True))

    logger.info("Checando sinais de saída (TP/SL) para %s", symbol)
    tp_msgs = verificar_tp(symbol, client, groups)
    for m in tp_msgs:
        asyncio.create_task(send_telegram(client, m, groups, image_type=('long' if 'LONG' in m else 'short'), is_critical=True))
    if not tp_msgs and (long_positions > 0 or short_positions > 0):
        logger.info("Mantendo posições abertas para %s: Nenhum trigger de saída (TP/SL/cruzamento)", symbol)
    logger.info("Finalizado processamento de kline para %s", symbol.upper())

def verificar_tp(symbol, client, groups):
    logger.info("Verificando take-profit/stop-loss para %s", symbol)
    price = latest_prices.get(symbol.lower())
    messages = []
    if not price:
        logger.info("Preço indisponível para %s", symbol)
        return messages
    for order_type in ['long', 'short']:
        ativos = orders[symbol.upper()][order_type]
        if not ativos:
            logger.info("Sem ordens ativas para %s (%s)", symbol, order_type)
            continue
        for order in ativos[:]:
            if order == first_sol_order:
                logger.info("Ignorando primeira ordem SOLUSDT para %s", symbol)
                continue
            change = (price - order['entry']) / order['entry'] if order['type'] == 'long' else (order['entry'] - price) / order['entry']
            logger.debug("Mudança percentual para %s (%s, camada %d): %.4f", symbol, order_type, order['layer'], change)
            if change >= TP_PCT:
                logger.info("Take-profit atingido para %s (%s, camada %d), fechando ordem", symbol, order_type, order['layer'])
                msg = close_order(order, price, symbol, client, groups)
                messages.append(msg)
                logger.info("Ordem fechada: %s", msg)
            elif change <= -SL_PCT:
                logger.info("Stop-loss atingido para %s (%s, camada %d), fechando ordem", symbol, order_type, order['layer'])
                msg = close_order(order, price, symbol, client, groups)
                messages.append(msg)
                logger.info("Ordem fechada: %s", msg)
            else:
                logger.info("Mantendo ordem aberta para %s (%s, camada %d): mudança=%.4f, TP=%.4f, SL=%.4f", 
                           symbol, order_type, order['layer'], change, TP_PCT, -SL_PCT)
    return messages

async def monitor_account(client, groups):
    logger.info("Iniciando monitoramento da conta")
    while True:
        try:
            summary = await get_futures_summary()
            status_summary = []
            for sym in SYMBOLS:
                long_pos = get_open_positions(sym.upper(), 'long')
                short_pos = get_open_positions(sym.upper(), 'short')
                entry = layer_info[sym]['entry_price']
                if long_pos > 0:
                    if isinstance(entry, (float, int)) and entry is not None:
                        status_summary.append(f"{sym.upper()}: LONG aberta @ {entry:.4f}")
                    else:
                        status_summary.append(f"{sym.upper()}: LONG aberta @ N/A")
                elif short_pos > 0:
                    if isinstance(entry, (float, int)) and entry is not None:
                        status_summary.append(f"{sym.upper()}: SHORT aberta @ {entry:.4f}")
                    else:
                        status_summary.append(f"{sym.upper()}: SHORT aberta @ N/A")
                else:
                    status_summary.append(f"{sym.upper()}: Nenhuma posição")
            logger.info("Resumo de posições: %s", " | ".join(status_summary))
            if summary:
                msg = format_summary(summary)
                print(msg)
                logger.info("Resumo da conta: %s", msg)
                await send_telegram(client, msg, groups, image_type='inf')
            else:
                msg = "⚠️ Não foi possível obter o resumo da conta durante o monitoramento. Verifique a conexão com a Binance."
                print(msg)
                logger.error(msg)
                await send_telegram(client, msg, groups, image_type='inf', is_critical=True)
        except Exception as e:
            logger.error("Erro ao monitorar conta: %s", e)
            print(f"Erro ao monitorar conta: {e}")
        await asyncio.sleep(7200)

async def log_status():
    logger.info("Iniciando log recorrente de status")
    while True:
        try:
            logger.info("Aguardando novo candle para análise de sinais de trade...")
            print("🔍 Monitorando gráficos para sinais de trade...")
        except Exception as e:
            logger.error("Erro no log_status: %s", e)
            print(f"Erro no log_status: {e}")
        await asyncio.sleep(60)

async def main():
    logger.info("Iniciando função principal do bot VKINHA Trading")
    global sim_balance, sim_daily_gain, total_gain, trade_count, total_loss_count, sim_day
    print(f"💰 Modo: {'REAL' if not SIMULATED else 'SIMULADO'} - Saldo inicial: {sim_balance:.2f} USDT")
    logger.info("Modo: %s - Saldo inicial: %.2f USDT", 'REAL' if not SIMULATED else 'SIMULADO', sim_balance)
    client = None
    groups = []
    try:
        client = await connect_telegram()
        async with client:
            groups = await get_all_groups(client)
            modo = "SIMULATED" if SIMULATED else "REAL"
            await send_telegram(client, f"✅ VKINHA Trading iniciado em modo {modo} 🚀", groups, image_type='inf', is_initial=True, is_critical=True)
            logger.info("Bot iniciado em modo %s", modo)
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
            logger.info("Resumo inicial da conta: %s", msg)
            await send_telegram(client, msg, groups, image_type='inf', is_initial=True, is_critical=True)
            print("Sincronizado...")
            logger.info("Sincronizado...")
            start_websocket(client, groups)
            await initial_test_operations(client, groups)
            asyncio.create_task(monitor_account(client, groups))
            asyncio.create_task(log_status())
            await client.run_until_disconnected()
    except Exception as e:
        logger.error("Erro no main: %s", e)
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
        logger.error("Erro fatal na execução do bot: %s", e)
        print(f"Erro fatal na execução do bot: {e}")
        raise