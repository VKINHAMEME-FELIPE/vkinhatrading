import asyncio
from telethon import TelegramClient
from datetime import datetime, UTC, date
from dotenv import load_dotenv
import os
import pandas as pd
import time
import logging
import json
from binance.um_futures import UMFutures
from binance.error import ClientError
from binance.websocket.um_futures.websocket_client import UMFuturesWebsocketClient
import urllib.request
import numpy as np

# Configuração do logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler('trading.log')
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
INFLATE_PUBLIC_BALANCE = True  # Exibir valores inflados (x10) em mensagens Telegram
SAFETY_MARGIN = 0.2  # Reserva de 20% do saldo para evitar liquidação

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
TOTAL_MARGIN = 6.67  # 6.67 USDT para LONG e 6.67 USDT para SHORT por símbolo
TP_PCT = 0.008
SL_PCT = 0.02
FEE_RATE = 0.0004
LAYER_PCTS = [0.3, 0.3, 0.4]
LAYER_OFFSETS = [0.001, 0.003, 0.006]
EMA_DIFF_THRESHOLD = 0.001
TRADE_HISTORY_FILE = "trade_history.json"
CHECK_TREND_CONSISTENCY = False
MIN_VOLUME_FACTOR = 0.8
logger.info("Constantes de configuração inicializadas")

# Validação de pares suportados
def validate_symbols():
    try:
        exchange_info = binance_client.exchange_info()
        valid_symbols = {s['symbol'].lower() for s in exchange_info['symbols']}
        invalid_symbols = [s for s in SYMBOLS if s not in valid_symbols]
        if invalid_symbols:
            logger.error(f"Pares inválidos encontrados: {invalid_symbols}")
            raise ValueError(f"Pares inválidos: {invalid_symbols}. Verifique os pares suportados na Binance Futures.")
        logger.info("Todos os pares de negociação são válidos")
    except ClientError as e:
        logger.error(f"Erro ao validar pares: {e}")
        raise ValueError(f"Erro ao validar pares: {e}")

# Inicialização do cliente Binance
def set_hedge_mode(symbol):
    """
    Garante que o modo de posição está em hedge (Dual Side Position).

    A biblioteca binance-futures-connector não possui o método
    ``get_position_side``; em seu lugar, utiliza-se ``get_position_mode``
    para verificar se o modo Dual Side está ativo. Caso não esteja,
    ``change_position_mode`` é chamado para habilitar o modo hedge.
    """
    try:
        # Verificar o estado atual do modo de posição (One-way ou Hedge)
        position_mode = binance_client.get_position_mode()
        # A chave 'dualSidePosition' indica se o Hedge Mode está ativo
        if not position_mode.get('dualSidePosition', False):
            binance_client.change_position_mode(dualSidePosition=True)
            logger.info(f"Modo de posição configurado para Hedge Mode para {symbol}")
        else:
            logger.info(f"Hedge Mode já configurado para {symbol}")
    except ClientError as e:
        # Código -4059 indica que o modo já está ativado
        if e.error_code == -4059:
            logger.info(f"Hedge Mode já configurado para {symbol}, ignorando erro: {e}")
        else:
            logger.error(f"Erro ao configurar Hedge Mode para {symbol}: {e}")
    except Exception as e:
        logger.error(f"Erro inesperado ao configurar Hedge Mode para {symbol}: {e}")

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
    binance_client = UMFutures(key=BINANCE_API_KEY, secret=BINANCE_API_SECRET)
    validate_symbols()
    for symbol in SYMBOLS:
        set_hedge_mode(symbol.upper())
        binance_client.change_leverage(symbol=symbol.upper(), leverage=LEVERAGE)
    logger.info("Cliente Binance inicializado e alavancagem configurada")
except ClientError as e:
    logger.error(f"Erro ao inicializar cliente Binance: {e}")
    raise ValueError(f"Erro ao inicializar cliente Binance: {e}")

orders = {symbol.upper(): {'long': [], 'short': []} for symbol in SYMBOLS}
latest_prices = {symbol: None for symbol in SYMBOLS}
last_telegram_time = time.time()
data = {symbol: [] for symbol in SYMBOLS}
logger.info("Estruturas de dados iniciais configuradas")

# Funções de Saldo
def get_account_balance():
    logger.info("Obtendo saldo da conta")
    try:
        if SIMULATED:
            logger.info("Modo simulado: retornando saldo simulado de 596.64 USDT")
            return 596.64
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
        return 596.64

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
        print("⚠️ Modo simulado ativo, retornando saldo simulado")
        return {
            "Total Equity": 596.64,
            "Margin Balance": 596.64,
            "Floating P&L": 0.0,
            "Futures Wallet Balance": 596.64
        }
    for attempt in range(max_retries):
        try:
            balances = binance_client.balance()
            usdt_balance = next((item for item in balances if item["asset"] == "USDT"), None)
            if not usdt_balance:
                logger.error("USDT não encontrado na lista de saldos")
                print("⚠️ USDT não encontrado na lista de saldos")
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

# Funções de Telegram
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

async def send_telegram(client, message, groups, image_type='inf', is_initial=False):
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
                    logger.info(f"Mensagem de texto enviada ao grupo {group_id} (sem imagem)")
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

# Funções Binance
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
        # A biblioteca utiliza o método 'klines' para obter candles.  O método
        # 'get_klines' não existe em UMFutures, o que provocava o erro
        # 'UMFutures object has no attribute get_klines'.
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
    # Calcular EMAs antes de referenciar
    df['ema7'] = df['close'].ewm(span=7).mean()
    df['ema21'] = df['close'].ewm(span=21).mean()
    logger.info(
        f"Condições para {symbol.upper()} - Volume: {df['volume'].iloc[-1]:.2f} | EMA7: {df['ema7'].iloc[-1]:.4f} | EMA21: {df['ema21'].iloc[-1]:.4f}"
    )
    avg_volume = df['volume'].mean()
    recent_volume = df['volume'].iloc[-1]
    if recent_volume < avg_volume * MIN_VOLUME_FACTOR:
        logger.info(
            f"Volume insuficiente para {symbol}: {recent_volume:.2f} < {MIN_VOLUME_FACTOR*100}% da Média {avg_volume:.2f}"
        )
        return False
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

def place_order(order_type, entry_price, symbol):
    logger.info(f"Colocando ordem {order_type.upper()} para {symbol}, preço de entrada: {entry_price}")
    symbol = symbol.upper()
    if get_open_positions(symbol, order_type) >= len(LAYER_PCTS):
        logger.warning(f"Limite de ordens atingido para {symbol} ({order_type})")
        return f"⚠️ Limite de ordens atingido para {symbol} ({order_type})"
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
        logger.info(f"Calculando camada {i}/{len(LAYER_PCTS)}: entrada={entry:.4f}, margem={layer_margin:.2f}, quantidade={qty}")
        if SIMULATED:
            order_data = {
                'type': order_type,
                'entry': entry,
                'amount': qty,
                'cost': layer_margin,
                'open_time': datetime.now(UTC),
                'layer': i
            }
            orders[symbol][order_type].append(order_data)
            msg = f"✅ ORDEM EXECUTADA: {symbol} {order_type.upper()}\nCamada {i}/{len(LAYER_PCTS)}\nQTD: {qty}"
            messages.append(msg)
            logger.info(f"Ordem simulada colocada: {msg}")
        else:
            side = 'BUY' if order_type == 'long' else 'SELL'
            try:
                if can_place_order(symbol, order_type):
                    binance_client.new_order(
                        symbol=symbol,
                        side=side,
                        type='MARKET',
                        quantity=qty,
                        positionSide=order_type.upper()
                    )
                    msg = f"✅ ORDEM EXECUTADA: {symbol} {order_type.upper()}\nCamada {i}/{len(LAYER_PCTS)}\nQTD: {qty}"
                    messages.append(msg)
                    logger.info(f"Ordem real colocada: {msg}")
                else:
                    msg = f"⚠️ Ordem não colocada para {symbol}: conflito de posição"
                    messages.append(msg)
                    logger.warning(msg)
                    continue
            except ClientError as e:
                msg = f"❌ Erro camada {i}/{len(LAYER_PCTS)}: {e}"
                messages.append(msg)
                logger.error(f"Erro ao colocar ordem real: {msg}")
        trade_log = {
            "timestamp": str(datetime.now(UTC)),
            "symbol": symbol,
            "type": order_type,
            "layer": i,
            "qty": qty,
            "price": entry,
            "simulated": SIMULATED
        }
        save_trade_history(trade_log)
        logger.info(f"ORDEM COLOCADA - {symbol} - {order_type.upper()} - Camada {i} - Preço: {entry:.4f} - Quantidade: {qty}")
    logger.info(f"Ordem colocada: {symbol} {order_type.upper()} Camada Inicial")
    return "\n".join(messages)

def close_order(order, current_price, symbol):
    logger.info(f"Fechando ordem para {symbol}, tipo: {order['type']}, camada: {order['layer']}, preço atual: {current_price}")
    global total_gain, trade_count, total_loss_count, sim_balance, sim_daily_gain
    gain = order['amount'] * (current_price - order['entry']) if order['type'] == 'long' else order['amount'] * (order['entry'] - current_price)
    gain *= (1 - FEE_RATE)
    total_gain += gain
    sim_daily_gain += gain
    sim_balance += order['cost'] + gain
    trade_count += 1
    if gain < 0:
        total_loss_count += 1
    orders[symbol.upper()][order['type']].remove(order)
    percentual = (gain / sim_balance) * 100
    display_balance = sim_balance * 10 if INFLATE_PUBLIC_BALANCE else sim_balance
    display_gain = gain * 10 if INFLATE_PUBLIC_BALANCE else gain
    msg = f"""❌ <b>Ordem FECHADA</b>
<b>Par:</b> {symbol.upper()}
<b>Tipo:</b> {order['type'].upper()}
<b>Camada:</b> {order['layer']}
<b>Ganho:</b> {display_gain:.2f} USDT ({percentual:.2f}%)
<b>Saldo Atual:</b> {display_balance:.2f} USDT
📈 <i>Operação realizada pelo bot VKINHA Trading</i>"""
    logger.info(f"Ordem fechada: {symbol.upper()} {order['type'].upper()}, Camada: {order['layer']}, Ganho: {gain:.2f} USDT")
    trade_log = {
        "timestamp": str(datetime.now(UTC)),
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
    logger.info("Iniciando operação de teste inicial")
    if not SIMULATED:
        logger.info("Modo REAL ativo, executando operação de teste inicial com margem reduzida")
        symbol = 'SOLUSDT'
        entry_price = get_price_rest(symbol)
        if entry_price and await check_trading_conditions(symbol, entry_price):
            msg_long = place_order('long', entry_price, symbol)
            if msg_long:
                await send_telegram(client, f"🧪 Ordem TESTE LONG em {symbol.upper()}\n{msg_long}", groups, image_type='long')
                logger.info(f"Ordem de teste LONG enviada: {msg_long}")
        return
    symbol = 'SOLUSDT'
    entry_price = None
    logger.info("Iniciando operação de teste")
    print("Aguardando preço real do SOLUSDT...")
    for _ in range(20):
        if symbol.lower() in latest_prices and latest_prices[symbol.lower()] is not None:
            entry_price = latest_prices[symbol.lower()]
            logger.info(f"Preço obtido via WebSocket para {symbol}: {entry_price}")
            break
        await asyncio.sleep(1)
    if not entry_price:
        entry_price = get_price_rest(symbol)
        if not entry_price:
            msg = "⚠️ Erro: Não foi possível obter o preço real do SOLUSDT"
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
        logger.info(f"Ordem de teste LONG enviada: {msg_long}")
    await asyncio.sleep(60)
    close_price = latest_prices.get(symbol.lower(), entry_price)
    messages = []
    for order in orders[symbol]['long'][:]:
        msg = close_order(order, close_price, symbol)
        messages.append(msg)
        logger.info(f"Ordem de teste fechada: {msg}")
    for msg in messages:
        await send_telegram(client, msg, groups, image_type=order['type'])

def start_websocket(client, groups):
    logger.info("Iniciando WebSocket da Binance")
    print("🔗 Conectando ao WebSocket da Binance...")
    ws_client = UMFuturesWebsocketClient()
    def make_callback(symbol):
        def callback(msg):
            logger.info(f"Recebida mensagem WebSocket para {symbol}: {msg['e']}")
            asyncio.create_task(handle_kline_async(msg, client, groups))
        return callback
    for symbol in SYMBOLS:
        ws_client.kline(symbol=symbol.lower(), interval="1m", callback=make_callback(symbol))
        logger.info(f"WebSocket configurado para {symbol}")
    print("✅ WebSocket iniciado.")
    logger.info("WebSocket iniciado")
    async def check_websocket_status():
        while True:
            logger.info("Verificando status do WebSocket...")
            print("🔍 Verificando status do WebSocket...")
            await asyncio.sleep(300)
    asyncio.create_task(check_websocket_status())

async def handle_kline_async(msg, client, groups):
    logger.info(f"handle_kline_async disparado para: {msg['s']}")
    print(f"🚀 handle_kline_async disparado para: {msg['s']}")
    try:
        logger.info(f"Recebida mensagem WebSocket para {msg['s']}")
        handle_kline(msg, client, groups)
        logger.info(f"Mensagem WebSocket processada para {msg['s']}")
    except Exception as e:
        logger.error(f"Erro ao processar mensagem WebSocket para {msg['s']}: {e}")
        print(f"Erro ao processar mensagem WebSocket para {msg['s']}: {e}")

def handle_kline(msg, client, groups):
    logger.info(f"Recebido do WebSocket para {msg['s']}: {msg}")
    print(f"📩 Recebido do WebSocket: {msg}")
    global sim_day, sim_daily_gain
    if msg['e'] != 'kline' or not msg['k']['x']:
        logger.info(f"Ignorado candle inacabado para {msg['s']}")
        return
    symbol = msg['s'].lower()
    close_price = float(msg['k']['c'])
    timestamp = int(msg['k']['t'])
    dt = datetime.fromtimestamp(timestamp / 1000.0, tz=UTC)
    logger.info(f"Processando candle fechado para {symbol.upper()}: Preço={close_price}, Timestamp={dt}")
    data[symbol].append({'time': dt, 'close': close_price})
    if len(data[symbol]) < 22:
        logger.info(f"Dados insuficientes para {symbol.upper()}, aguardando mais candles")
        return
    data[symbol] = data[symbol][-22:]
    latest_prices[symbol] = close_price
    print(f"📈 Último preço de {symbol.upper()}: {close_price}")
    logger.info(f"Último preço atualizado para {symbol.upper()}: {close_price}")
    df = pd.DataFrame(data[symbol])
    df = df[['time', 'close']]
    df['ema7'] = df['close'].ewm(span=7).mean()
    df['ema21'] = df['close'].ewm(span=21).mean()
    ema7 = df['ema7'].iloc[-1]
    ema21 = df['ema21'].iloc[-1]
    messages = verificar_tp(symbol)
    if not orders[symbol.upper()]['long'] and not orders[symbol.upper()]['short']:
        trading_conditions_met = asyncio.run_coroutine_threadsafe(check_trading_conditions(symbol, close_price), asyncio.get_event_loop()).result()
        if trading_conditions_met:
            diff = abs(ema7 - ema21) / ema21
            if diff >= EMA_DIFF_THRESHOLD:
                logger.info(f"SINAL DETECTADO: {symbol.upper()} - Tipo: {'LONG' if ema7 > ema21 else 'SHORT'} - EMA7={ema7:.4f} - EMA21={ema21:.4f}")
                if ema7 > ema21:
                    msg = place_order('long', close_price, symbol)
                    if msg:
                        messages.append(f"📈 SINAL LONG {symbol.upper()}\n{msg}")
                        logger.info(f"Sinal LONG detectado e ordem colocada: {msg}")
                elif ema7 < ema21:
                    msg = place_order('short', close_price, symbol)
                    if msg:
                        messages.append(f"📉 SINAL SHORT {symbol.upper()}\n{msg}")
                        logger.info(f"Sinal SHORT detectado e ordem colocada: {msg}")
            else:
                logger.info(f"Candle rejeitado para {symbol.upper()}: Diferença EMA insuficiente ({diff:.4f} < {EMA_DIFF_THRESHOLD})")
        else:
            df_klines = asyncio.run_coroutine_threadsafe(get_kline_data(symbol, interval='1m', limit=22), asyncio.get_event_loop()).result()
            if df_klines is None or len(df_klines) < 22:
                logger.info(f"Candle rejeitado para {symbol.upper()}: Dados insuficientes (tamanho do dataframe: {len(df_klines) if df_klines is not None else 'None'})")
            else:
                avg_volume = df_klines['volume'].mean()
                recent_volume = df_klines['volume'].iloc[-1]
                if recent_volume < avg_volume * MIN_VOLUME_FACTOR:
                    logger.info(f"Candle rejeitado para {symbol.upper()}: Volume insuficiente ({recent_volume:.2f} < {MIN_VOLUME_FACTOR*100}% da média {avg_volume:.2f})")
                if CHECK_TREND_CONSISTENCY:
                    df_klines['ema7'] = df_klines['close'].ewm(span=7).mean()
                    df_klines['ema21'] = df_klines['close'].ewm(span=21).mean()
                    trend_consistent = False
                    if df_klines['ema7'].iloc[-1] > df_klines['ema21'].iloc[-1]:
                        trend_consistent = all(df_klines['ema7'].tail(3) > df_klines['ema21'].tail(3))
                    elif df_klines['ema7'].iloc[-1] < df_klines['ema21'].iloc[-1]:
                        trend_consistent = all(df_klines['ema7'].tail(3) < df_klines['ema21'].tail(3))
                    if not trend_consistent:
                        logger.info(f"Candle rejeitado para {symbol.upper()}: Tendência não consistente")
    for msg in messages:
        image_type = 'long' if 'SINAL LONG' in msg else 'short' if 'SINAL SHORT' in msg else 'inf'
        if 'Ordem FECHADA' in msg:
            order_type = 'long' if 'LONG' in msg else 'short'
            image_type = order_type
        asyncio.create_task(send_telegram(client, msg, groups, image_type=image_type))
        logger.info(f"Mensagem enviada para Telegram: {msg}")
    now = datetime.now(UTC)
    if now.hour == 21 and date.today() != sim_day:
        sim_day = date.today()
        display_daily_gain = sim_daily_gain * 10 if INFLATE_PUBLIC_BALANCE else sim_daily_gain
        display_balance = sim_balance * 10 if INFLATE_PUBLIC_BALANCE else sim_balance
        msg = f"""📆 <b> Fatório Diário</b>
📈 Rentabilidade: {display_daily_gain:.2f} USDT
💰 Saldo Atual: {display_balance:.2f} USDT
⚠️ <i>Modo {'simulado' if SIMULATED else 'real'} ativo</i>"""
        asyncio.create_task(send_telegram(client, msg, groups, image_type='inf'))
        logger.info(f"Relatório diário enviado: {msg}")
        sim_daily_gain = 0

def verificar_tp(symbol):
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
            change = (price - order['entry']) / order['entry'] if order['type'] == 'long' else (order['entry'] - price) / order['entry']
            if change >= TP_PCT or change <= -SL_PCT:
                msg = close_order(order, price, symbol)
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
                await send_telegram(client, msg, groups, image_type='inf')
        except Exception as e:
            logger.error(f"Erro ao monitorar conta: {e}")
            print(f"Erro ao monitorar conta: {e}")
        await asyncio.sleep(7200)
        logger.info("Ciclo de monitoramento da conta concluído")

async def log_status():
    logger.info("Iniciando log recorrente de status")
    while True:
        try:
            print("🔍 Monitorando gráficos para sinais de trade...")
            logger.info("Monitorando gráficos para sinais de trade...")
        except Exception as e:
            logger.error(f"Erro no log_status: {e}")
            print(f"Erro no log_status: {e}")
        await asyncio.sleep(600)

async def main():
    logger.info("Iniciando função principal do bot")
    global sim_balance, sim_daily_gain, total_gain, trade_count, total_loss_count, sim_day
    sim_balance = 596.64
    sim_daily_gain = 0
    total_gain = 0
    trade_count = 0
    total_loss_count = 0
    sim_day = date.today()
    print(f"💰 Modo: {'REAL' if not SIMULATED else 'SIMULADO'} - Saldo inicial: {sim_balance:.2f} USDT")
    logger.info(f"Modo: {'REAL' if not SIMULATED else 'SIMULADO'} - Saldo inicial: {sim_balance:.2f} USDT")
    client = None
    groups = []
    try:
        client = await connect_telegram()
        async with client:
            groups = await get_all_groups(client)
            modo = "SIMULATED" if SIMULATED else "REAL"
            await send_telegram(client, f"✅ VKINHA Trading iniciado em modo {modo} 🚀", groups, image_type='inf', is_initial=True)
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
                await send_telegram(client, msg, groups, image_type='inf', is_initial=True)
                raise
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
            logger.info("Sincronizado...")
            start_websocket(client, groups)
            await initial_test_operations(client, groups)
            asyncio.create_task(monitor_account(client, groups))
            asyncio.create_task(log_status())
            await asyncio.sleep(10)
            entry_price = get_price_rest("SOLUSDT")
            logger.info(f"Preço inicial para SOLUSDT: {entry_price}")
            msg = place_order("long", entry_price, "SOLUSDT")
            # Exibe no console e no log
            print(msg)
            logger.info(f"Ordem inicial colocada: {msg}")
            # Envia notificação ao Telegram para informar a abertura da operação
            if msg:
                try:
                    await send_telegram(client, f"📈 Ordem inicial LONG em SOLUSDT\n{msg}", groups, image_type='long')
                except Exception as e:
                    logger.error(f"Falha ao enviar mensagem de abertura de ordem inicial: {e}")
            # Mantém a sessão do Telegram aberta até ser desconectada
            await client.run_until_disconnected()
    except Exception as e:
        logger.error(f"Erro no main: {e}")
        print(f"Erro no main: {e}")
        if client:
            await send_telegram(client, f"❌ Erro no bot: {e}", groups, image_type='inf', is_initial=True)

if __name__ == '__main__':
    logger.info("Iniciando execução do bot VKINHA Trading")
    try:
        asyncio.run(main())
        print("✅ VKINHA Trading finalizado 🚀")
        logger.info("VKINHA Trading finalizado")
    except Exception as e:
        logger.error(f"Erro fatal na execução do bot: {e}")
        print(f"Erro fatal na execução do bot: {e}")