import asyncio
import json
import os
import pandas as pd
import logging
from logging.handlers import RotatingFileHandler
from binance.um_futures import UMFutures
from binance.error import ClientError
import uuid
import websockets
from datetime import datetime, timezone, date
from dotenv import load_dotenv
import math

# Configuração do logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
file_handler = RotatingFileHandler('trading.log', maxBytes=5*1024*1024, backupCount=3)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(console_handler)
logger.info("Logger configurado com sucesso")

# Carregando variáveis de ambiente
logger.info("Carregando variáveis de ambiente")
load_dotenv()
BINANCE_API_KEY = os.getenv("API_KEY")
BINANCE_API_SECRET = os.getenv("API_SECRET")
SIMULATED = os.getenv("SIMULATED", "true").lower() == "true"
INFLATE_PUBLIC_BALANCE = True
SAFETY_MARGIN = 2
logger.info("Variáveis de ambiente carregadas: SIMULATED=%s", SIMULATED)

# Verificação de variáveis de ambiente
logger.info("Verificando variáveis de ambiente")
if not all([BINANCE_API_KEY, BINANCE_API_SECRET]) and not SIMULATED:
    logger.error("BINANCE_API_KEY ou BINANCE_API_SECRET não encontrados no .env")
    raise ValueError("BINANCE_API_KEY ou BINANCE_API_SECRET não encontrados no .env")
logger.info("Variáveis de ambiente validadas com sucesso")

# Pares de negociação e configurações
SYMBOLS = ['btcusdt', 'ethusdt', 'solusdt', 'bnbusdt','nearusdt','xrpusdt','dogeusdt','ltcusdt','suiusdt']
LEVERAGE = 20
TOTAL_MARGIN = 10
TP_PCT = 0.015
SL_PCT = 0.013
FEE_RATE = 0.0004
LAYER_PCTS = [0.2, 0.3, 0.5]
LAYER_OFFSETS = [0.003, 0.005, 0.009]
EMA_DIFF_THRESHOLD = 0
TRADE_HISTORY_FILE = "trade_history.json"
CHECK_TREND_CONSISTENCY = False
INTERVAL = "1m"
MIN_VOLUME = 0
TRAILING_STOP_MIN_PCT = 0.005
MIN_NOTIONAL_VALUE = 2.0
MAX_EXPOSURE_PCT = 0.5
COOLDOWN_CANDLES = 10
MAX_TRADES_PER_DAY = 5
SLIPPAGE_PCT = 0.001
logger.info("Constantes de configuração inicializadas: SYMBOLS=%s, LEVERAGE=%d, TOTAL_MARGIN=%.2f, MIN_NOTIONAL_VALUE=%.2f", SYMBOLS, LEVERAGE, TOTAL_MARGIN, MIN_NOTIONAL_VALUE)

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
latest_prices = {sym.lower(): None for sym in SYMBOLS}
data = {sym.lower(): [] for sym in SYMBOLS}
layer_info = {sym.lower(): {'entry_price': None, 'opened_layers': 0, 'order_type': None, 'next_layer': 1} for sym in SYMBOLS}
prev_emas = {sym.lower(): {'ema7': None, 'ema21': None} for sym in SYMBOLS}
trailing_stops = {sym.upper(): {'long': {}, 'short': {}} for sym in SYMBOLS}
positions_in_memory = {sym.upper(): {'long': False, 'short': False} for sym in SYMBOLS}
sim_balance = 596.64
sim_daily_gain = 0
total_gain = 0
trade_count = 0
total_loss_count = 0
sim_day = date.today()
trade_cooldown = {sym.upper(): {'last_trade_time': None, 'trade_count': 0} for sym in SYMBOLS}
logger.info("Estruturas de dados iniciais configuradas: sim_balance=%.2f", sim_balance)

# Função utilitária para formatar quantidade
def format_quantity(symbol, qty):
    precision = get_symbol_precision(symbol)
    step_size = 1 / (10 ** precision)
    try:
        # Arredondar para cima para garantir valor mínimo
        formatted_qty = float(f"{math.ceil(qty / step_size) * step_size:.{precision}f}")
        current_price = latest_prices.get(symbol.lower(), 0)
        if formatted_qty * current_price < MIN_NOTIONAL_VALUE:
            logger.warning("Quantidade %.4f para %s resulta em valor notional %.2f < %.2f USDT, ajustando quantidade",
                          formatted_qty, symbol.upper(), formatted_qty * current_price, MIN_NOTIONAL_VALUE)
            # Tentar ajustar a quantidade para atingir o valor notional mínimo
            min_qty = math.ceil((MIN_NOTIONAL_VALUE / current_price) / step_size) * step_size
            formatted_qty = float(f"{min_qty:.{precision}f}")
            if formatted_qty * current_price < MIN_NOTIONAL_VALUE:
                logger.warning("Quantidade ajustada %.4f ainda abaixo do valor notional mínimo, pulando operação", formatted_qty)
                return 0
        logger.debug("Quantidade formatada para %s: %.4f (valor notional: %.2f USDT)", symbol.upper(), formatted_qty, formatted_qty * current_price)
        return formatted_qty
    except Exception as e:
        logger.error("Erro ao formatar quantidade para %s: %s", symbol.upper(), e)
        return 0

# Função para atualizar posições em memória
def update_memory_after_order(symbol, order_type, is_open):
    logger.info("Atualizando memória para %s (%s): is_open=%s", symbol.upper(), order_type, is_open)
    positions_in_memory[symbol.upper()][order_type] = is_open
    logger.debug("Memória atualizada: %s", positions_in_memory[symbol.upper()])

# Funções auxiliares
def calcula_ema(candles, period):
    logger.info("Calculando EMA%d para %d candles", period, len(candles))
    try:
        df = pd.DataFrame(candles, columns=['close'])
        df['close'] = df['close'].astype(float)
        ema = df['close'].ewm(span=period, adjust=False).mean().iloc[-1]
        logger.debug("EMA%d calculada: %.4f", period, ema)
        return ema
    except Exception as e:
        logger.error("Erro ao calcular EMA%d: %s", period, e)
        return None

def calcula_rsi(candles, period=14):
    logger.info("Calculando RSI%d para %d candles", period, len(candles))
    try:
        df = pd.DataFrame(candles, columns=['close'])
        df['close'] = df['close'].astype(float)
        delta = df['close'].diff()
        gain = delta.where(delta > 0, 0).rolling(window=period).mean()
        loss = -delta.where(delta < 0, 0).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        rsi_value = rsi.iloc[-1]
        logger.debug("RSI%d calculado: %.2f", period, rsi_value)
        return rsi_value
    except Exception as e:
        logger.error("Erro ao calcular RSI%d: %s", period, e)
        return None

def cruzou(ema7, ema21, medias_iniciais):
    logger.info("Verificando cruzamento: EMA7=%.4f, EMA21=%.4f, Prev_EMA7=%s, Prev_EMA21=%s",
                ema7, ema21, medias_iniciais['ema7'], medias_iniciais['ema21'])
    try:
        prev_ema7 = medias_iniciais['ema7']
        prev_ema21 = medias_iniciais['ema21']
        if prev_ema7 is None or prev_ema21 is None:
            logger.warning("Sinal rejeitado: EMAs anteriores não disponíveis para cruzamento")
            return False
        if prev_ema7 <= prev_ema21 and ema7 > ema21:
            logger.info("Cruzamento para cima detectado")
            return True
        elif prev_ema7 >= prev_ema21 and ema7 < ema21:
            logger.info("Cruzamento para baixo detectado")
            return True
        logger.info("Sinal rejeitado: EMAs não cruzaram")
        return False
    except Exception as e:
        logger.error("Erro ao verificar cruzamento: %s", e)
        return False

def check_trend_consistency(candles, ema7, ema21):
    logger.info("Verificando consistência de tendência para %d candles", len(candles))
    try:
        if len(candles) < 10:
            logger.warning("Menos de 10 candles disponíveis, rejeitando sinal")
            return False
        df = pd.DataFrame(candles[-10:], columns=['close'])
        df['close'] = df['close'].astype(float)
        df['ema7'] = df['close'].ewm(span=7, adjust=False).mean()
        df['ema21'] = df['close'].ewm(span=21, adjust=False).mean()
        if ema7 > ema21:
            consistent = (df['ema7'] > df['ema21']).all()
            logger.debug("Tendência LONG: consistente=%s", consistent)
            return consistent
        else:
            consistent = (df['ema7'] < df['ema21']).all()
            logger.debug("Tendência SHORT: consistente=%s", consistent)
            return consistent
    except Exception as e:
        logger.error("Erro ao verificar consistência de tendência: %s", e)
        return False

def check_cooldown_and_trade_limit(symbol):
    logger.info("Verificando cooldown e limite de trades para %s", symbol)
    try:
        trade_info = trade_cooldown[symbol.upper()]
        current_time = datetime.now(timezone.utc)
        
        # Resetar contagem diária se for um novo dia
        if trade_info['last_trade_time'] and trade_info['last_trade_time'].date() != current_time.date():
            trade_info['trade_count'] = 0
            logger.info("Novo dia detectado, resetando contagem de trades para %s", symbol)
        
        # Verificar limite de trades por dia
        if trade_info['trade_count'] >= MAX_TRADES_PER_DAY:
            logger.info("Limite de %d trades por dia atingido para %s", MAX_TRADES_PER_DAY, symbol)
            return False
        
        # Verificar cooldown
        if trade_info['last_trade_time']:
            candles_passed = (current_time - trade_info['last_trade_time']).total_seconds() / 60
            if candles_passed < COOLDOWN_CANDLES:
                logger.info("Cooldown ativo para %s: %.2f candles passaram, necessário %d", 
                           symbol, candles_passed, COOLDOWN_CANDLES)
                return False
        
        logger.info("Cooldown e limite de trades verificados para %s: OK", symbol)
        return True
    except Exception as e:
        logger.error("Erro ao verificar cooldown para %s: %s", symbol, e)
        return False

def check_exposure_limit(symbol, margin_needed):
    logger.info("Verificando limite de exposição para %s, margem necessária: %.2f", symbol, margin_needed)
    try:
        balance = get_account_balance()
        total_exposure = 0
        for sym in SYMBOLS:
            for order_type in ['long', 'short']:
                for order in orders[sym.upper()][order_type]:
                    total_exposure += order['cost']
        if (total_exposure + margin_needed) / balance > MAX_EXPOSURE_PCT:
            logger.warning("Limite de exposição atingido para %s: exposição atual=%.2f, necessário=%.2f, limite=%.2f",
                          symbol, total_exposure, margin_needed, balance * MAX_EXPOSURE_PCT)
            return False
        logger.debug("Exposição OK para %s: exposição atual=%.2f, necessário=%.2f, limite=%.2f",
                    symbol, total_exposure, margin_needed, balance * MAX_EXPOSURE_PCT)
        return True
    except Exception as e:
        logger.error("Erro ao verificar limite de exposição para %s: %s", symbol, e)
        return False

def validar_sinal(symbol, ema7, ema21, candles, volume):
    logger.info("Validando sinal para %s: EMA7=%.4f, EMA21=%.4f, Volume=%.2f", symbol, ema7, ema21, volume)
    try:
        cruzamento_ema = cruzou(ema7, ema21, prev_emas[symbol.lower()])
        if not cruzamento_ema:
            logger.info("Sinal rejeitado para %s: EMAs não cruzaram", symbol)
            return False
        if volume < MIN_VOLUME:
            logger.info("Sinal rejeitado para %s: Volume insuficiente (%.2f < %.2f)", symbol, volume, MIN_VOLUME)
            return False
        rsi = calcula_rsi(candles)
        if rsi is None:
            logger.info("Sinal rejeitado para %s: RSI não pôde ser calculado", symbol)
            return False
        if not (rsi > 50 if ema7 > ema21 else rsi < 50):
            logger.info("Sinal rejeitado para %s: RSI fora da faixa (%.2f)", symbol, rsi)
            return False
        if CHECK_TREND_CONSISTENCY:
            if not check_trend_consistency(candles, ema7, ema21):
                logger.info("Sinal rejeitado para %s: Tendência não consistente por 10 candles", symbol)
                return False
            ema_diff = abs(ema7 - ema21) / ema21
            if ema_diff < EMA_DIFF_THRESHOLD:
                logger.info("Sinal rejeitado para %s: Diferença EMA (%.4f) menor que o limite (%.4f)", symbol, ema_diff, EMA_DIFF_THRESHOLD)
                return False
        logger.info("Sinal validado para %s: Todos os filtros passaram", symbol)
        return True
    except Exception as e:
        logger.error("Erro ao validar sinal para %s: %s", symbol, e)
        return False

def validate_symbols():
    logger.info("Validando símbolos de negociação: %s", SYMBOLS)
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
        set_hedge_mode(sym)
        binance_client.change_leverage(symbol=sym.upper(), leverage=LEVERAGE)
        logger.info("Alavancagem configurada para %s: %d", sym, LEVERAGE)
    logger.info("Hedge Mode e alavancagem configurados para todos os símbolos")
except Exception as e:
    logger.error("Erro na configuração de símbolos: %s", e)
    raise

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
        logger.info("Dados de klines processados para %s, %d candles", symbol, len(df))
        return df
    except Exception as e:
        logger.error("Erro ao obter dados de klines para %s: %s", symbol, e)
        return None

def get_account_balance():
    logger.info("Obtendo saldo da conta")
    try:
        if SIMULATED:
            logger.debug("Modo simulado: retornando saldo simulado %.2f", sim_balance)
            return sim_balance
        balances = binance_client.balance()
        usdt_balance = next((item for item in balances if item["asset"] == "USDT"), None)
        if usdt_balance:
            balance = float(usdt_balance["balance"])
            logger.debug("Saldo real obtido: %.2f USDT", balance)
            return balance
        logger.warning("Saldo USDT não encontrado, retornando 0")
        return 0
    except Exception as e:
        logger.error("Erro ao obter saldo da conta: %s", e)
        return sim_balance

def check_balance_sufficiency(symbol, order_type, margin_needed):
    logger.info("Verificando suficiência de saldo para %s (%s), margem necessária: %.2f", symbol, order_type, margin_needed)
    try:
        if SIMULATED:
            logger.debug("Modo simulado: saldo suficiente assumido")
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
            logger.warning("Saldo insuficiente: disponível=%.2f, necessário=%.2f, margem de segurança=%.2f",
                          available_balance, margin_needed, safety_threshold)
            return False
        logger.debug("Saldo suficiente: disponível=%.2f, necessário=%.2f", available_balance, margin_needed)
        return True
    except Exception as e:
        logger.error("Erro ao verificar saldo para %s: %s", symbol, e)
        return False

async def get_futures_summary(max_retries=3, retry_delay=5):
    logger.info("Obtendo resumo da conta de futuros")
    if SIMULATED:
        summary = {
            "Total Equity": sim_balance + sim_daily_gain,
            "Margin Balance": sim_balance,
            "Floating P&L": sim_daily_gain,
            "Futures Wallet Balance": sim_balance
        }
        logger.debug("Resumo simulado: %s", summary)
        return summary
    for attempt in range(max_retries):
        try:
            balances = binance_client.balance()
            usdt_balance = next((item for item in balances if item["asset"] == "USDT"), None)
            if not usdt_balance:
                logger.warning("Saldo USDT não encontrado no resumo")
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
            logger.debug("Resumo da conta obtido: %s", summary)
            return summary
        except Exception as e:
            logger.error("Tentativa %d/%d - Erro ao obter resumo da conta: %s", attempt + 1, max_retries, e)
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
            continue
    logger.error("Falha ao obter resumo após %d tentativas", max_retries)
    return {}

def save_trade_history(entry):
    logger.info("Salvando entrada no histórico de trades: %s", entry)
    try:
        if os.path.exists(TRADE_HISTORY_FILE):
            with open(TRADE_HISTORY_FILE, "r") as f:
                data = json.load(f)
        else:
            data = []
        data.append(entry)
        with open(TRADE_HISTORY_FILE, "w") as f:
            json.dump(data, f, indent=4)
        logger.debug("Histórico de trades salvo com sucesso")
    except Exception as e:
        logger.error("Erro ao salvar histórico: %s", e)

def get_open_positions(symbol, order_type):
    logger.info("Verificando posições abertas para %s (%s)", symbol, order_type)
    try:
        if positions_in_memory[symbol.upper()][order_type]:
            logger.debug("Posição em memória encontrada para %s (%s)", symbol, order_type)
            return True
        if SIMULATED:
            count = len(orders[symbol.upper()][order_type])
            logger.debug("Modo simulado: %d posições abertas para %s (%s)", count, symbol, order_type)
            return count > 0
        positions = binance_client.get_position_risk(symbol=symbol.upper())
        for pos in positions:
            position_amt = float(pos['positionAmt'])
            if (order_type == 'long' and position_amt > 0.01) or (order_type == 'short' and position_amt < -0.01):
                logger.debug("Posição encontrada: quantidade=%.4f", abs(position_amt))
                return True
        logger.debug("Nenhuma posição aberta encontrada para %s (%s)", symbol, order_type)
        return False
    except Exception as e:
        logger.error("Erro ao verificar posições abertas para %s (%s): %s", symbol, order_type, e)
        return False

def get_price_rest(symbol):
    logger.info("Obtendo preço via REST para %s", symbol)
    try:
        data = binance_client.mark_price(symbol=symbol.upper())
        funding_rate = float(data.get('lastFundingRate', 0))
        if funding_rate > 0:
            price = float(data['markPrice']) * (1 + SLIPPAGE_PCT)
        else:
            price = float(data['markPrice']) * (1 - SLIPPAGE_PCT)
        logger.debug("Preço obtido com slippage: %.4f", price)
        return price
    except Exception as e:
        logger.error("Erro ao obter preço via REST para %s: %s", symbol, e)
        return None

async def close_small_orders():
    logger.info("Verificando ordens pequenas (<%.2f USDT) para fechamento automático", MIN_NOTIONAL_VALUE)
    messages = []
    for symbol in SYMBOLS:
        symbol_upper = symbol.upper()
        symbol_lower = symbol.lower()
        for order_type in ['long', 'short']:
            ativos = orders[symbol_upper][order_type][:]
            for order in ativos:
                if order['cost'] * latest_prices.get(symbol_lower, 0) < MIN_NOTIONAL_VALUE:
                    logger.info("Fechando ordem pequena para %s (%s, camada %d): valor=%.2f",
                               symbol_upper, order_type, order['layer'], order['cost'] * latest_prices.get(symbol_lower, 0))
                    price = latest_prices.get(symbol_lower)
                    if price:
                        msg = await close_order(order, price, symbol_upper, close_all=True, reason='ordem_menor_que_1_5usd')
                        messages.append(msg)
                        # Agendar verificação 10 segundos após fechamento
                        asyncio.create_task(schedule_post_close_check(symbol_upper, order_type, 10))
    return messages

async def schedule_post_close_check(symbol, order_type, delay):
    logger.info("Agendando verificação pós-fechamento para %s (%s) após %d segundos", symbol, order_type, delay)
    await asyncio.sleep(delay)
    messages = await verificar_tp(symbol)
    messages.extend(await close_small_orders())
    for m in messages:
        logger.info("Resultado da verificação pós-fechamento para %s: %s", symbol, m)

async def place_order(order_type, entry_price, symbol, layer_index):
    logger.info("Colocando ordem: %s, tipo=%s, preço=%.4f, camada=%d", symbol, order_type, entry_price, layer_index)
    symbol_upper = symbol.upper()
    symbol_lower = symbol.lower()
    
    # Verificação de cooldown e limite de trades
    if not check_cooldown_and_trade_limit(symbol_upper):
        logger.warning("Ordem para %s rejeitada: cooldown ativo ou limite de trades atingido", symbol_upper)
        return f"Ordem para {symbol_upper} rejeitada: cooldown ativo ou limite de trades atingido"
    
    # Verificação reforçada para evitar posições LONG e SHORT simultâneas
    opposite_type = 'short' if order_type == 'long' else 'long'
    if get_open_positions(symbol_upper, opposite_type):
        logger.info("Fechando posição oposta para %s (%s) antes de abrir nova ordem", symbol_upper, opposite_type)
        for order in orders[symbol_upper][opposite_type][:]:
            price = latest_prices.get(symbol_lower)
            if price:
                await close_order(order, price, symbol_upper, close_all=True, reason='opposite_position')
        orders[symbol_upper][opposite_type].clear()
        layer_info[symbol_lower]['opened_layers'] = 0
        layer_info[symbol_lower]['entry_price'] = None
        layer_info[symbol_lower]['order_type'] = None
        layer_info[symbol_lower]['next_layer'] = 1
        update_memory_after_order(symbol_upper, opposite_type, False)

    if get_open_positions(symbol_upper, order_type):
        logger.info("Já existe posição aberta para %s (%s), não abrindo nova ordem", symbol_upper, order_type)
        return f"Já existe posição aberta para {symbol_upper} ({order_type})"
        
    if entry_price is None:
        logger.warning("Preço inválido para %s, pulando ordem", symbol_upper)
        return f"Preço inválido para {symbol_upper}, pulando operação"
        
    margin = TOTAL_MARGIN
    if not check_balance_sufficiency(symbol_upper, order_type, margin * LAYER_PCTS[layer_index - 1]):
        logger.warning("Saldo insuficiente para %s (%s, camada %d): margem necessária=%.2f",
                      symbol_upper, order_type, layer_index, margin * LAYER_PCTS[layer_index - 1])
        return f"Saldo insuficiente para {symbol_upper} {order_type.upper()}: margem necessária={margin * LAYER_PCTS[layer_index - 1]:.2f} USDT"
    
    if not check_exposure_limit(symbol_upper, margin * LAYER_PCTS[layer_index - 1]):
        logger.warning("Limite de exposição excedido para %s (%s, camada %d)", symbol_upper, order_type, layer_index)
        return f"Limite de exposição excedido para {symbol_upper} ({order_type})"
        
    entry = entry_price * (1 - LAYER_OFFSETS[layer_index - 1]) if order_type == 'long' else entry_price * (1 + LAYER_OFFSETS[layer_index - 1])
    layer_margin = margin * LAYER_PCTS[layer_index - 1]
    qty = format_quantity(symbol_upper, (layer_margin * LEVERAGE) / entry)
    if qty == 0:
        logger.warning("Camada %d pulada para %s: valor notional insuficiente", layer_index, symbol_upper)
        return f"Camada {layer_index} pulada: valor notional insuficiente"
        
    order_id = str(uuid.uuid4())
    order_data = {
        'order_id': order_id,
        'type': order_type,
        'entry': entry,
        'amount': qty,
        'cost': layer_margin,
        'open_time': datetime.now(timezone.utc),
        'layer': layer_index,
        'tp1_hit': False,
        'trailing_stop_price': None
    }
    
    if SIMULATED:
        orders[symbol_upper][order_type].append(order_data)
        logger.debug("Ordem simulada colocada: %s, camada=%d, quantidade=%.4f", symbol_upper, layer_index, qty)
    else:
        side = 'BUY' if order_type == 'long' else 'SELL'
        try:
            response = binance_client.new_order(
                symbol=symbol_upper,
                side=side,
                type='MARKET',
                quantity=qty,
                positionSide=order_type.upper(),
                newClientOrderId=order_id
            )
            order_data['binance_order_id'] = response['orderId']
            orders[symbol_upper][order_type].append(order_data)
            logger.debug("Ordem real colocada: %s, camada=%d, quantidade=%.4f, order_id=%s",
                        symbol_upper, layer_index, qty, response['orderId'])
        except ClientError as e:
            logger.error("Erro ao colocar ordem camada %d para %s: %s", layer_index, symbol_upper, e)
            return f"Erro camada {layer_index}: {e}"
            
    trade_log = {
        "timestamp": str(datetime.now(timezone.utc)),
        "symbol": symbol_upper,
        "type": order_type,
        "layer": layer_index,
        "qty": qty,
        "price": entry,
        "simulated": SIMULATED,
        "order_id": order_id
    }
    save_trade_history(trade_log)
    
    # Atualizar cooldown e contagem de trades
    trade_cooldown[symbol_upper]['last_trade_time'] = datetime.now(timezone.utc)
    trade_cooldown[symbol_upper]['trade_count'] += 1
    
    info = layer_info[symbol_lower]
    if info['opened_layers'] == 0:
        info['entry_price'] = entry_price
        info['order_type'] = order_type
    info['opened_layers'] += 1
    info['next_layer'] = min(layer_index + 1, len(LAYER_PCTS) + 1)
    update_memory_after_order(symbol_upper, order_type, True)
    
    logger.info("Ordem completada para %s: camada %d, quantidade=%.4f", symbol_upper, layer_index, qty)
    return f"ORDEM EXECUTADA: {symbol_upper} {order_type.upper()}\nCamada {layer_index}/{len(LAYER_PCTS)}\nQTD: {qty}"

async def close_order(order, current_price, symbol, is_partial=False, close_all=False, reason=''):
    logger.info("Fechando ordem para %s: tipo=%s, camada=%d, preço atual=%.4f, parcial=%s, close_all=%s, motivo=%s",
                symbol, order['type'], order['layer'], current_price, is_partial, close_all, reason)
    global total_gain, trade_count, total_loss_count, sim_balance, sim_daily_gain
    symbol_upper = symbol.upper()
    symbol_lower = symbol.lower()
    
    qty = order['amount'] * 0.5 if is_partial else order['amount']
    qty = format_quantity(symbol_upper, qty)
    if qty == 0 and not close_all:
        logger.warning("Quantidade arredondada para zero para %s (%s, camada %d), pulando fechamento",
                      symbol_upper, order['type'], order['layer'])
        return f"Quantidade muito pequena para negociar."
        
    if qty * current_price < MIN_NOTIONAL_VALUE and not close_all:
        logger.warning("Quantidade %s para %s (%s, camada %d) resulta em valor notional < %.2f USDT, pulando fechamento",
                      qty, symbol_upper, order['type'], order['layer'], MIN_NOTIONAL_VALUE)
        return f"Valor notional insuficiente para {symbol_upper} ({order['type']}), pulando fechamento"
        
    if not SIMULATED:
        try:
            positions = binance_client.get_position_risk(symbol=symbol_upper)
            position_amt = 0
            for pos in positions:
                amt = float(pos['positionAmt'])
                if (order['type'] == 'long' and amt > 0.01) or (order['type'] == 'short' and amt < -0.01):
                    position_amt = abs(amt)
                    break
            if position_amt == 0:
                logger.warning("Nenhuma posição aberta encontrada para %s (%s, camada %d), removendo ordem local",
                              symbol_upper, order['type'], order['layer'])
                orders[symbol_upper][order['type']].remove(order)
                layer_info[symbol_lower]['opened_layers'] -= 1
                if layer_info[symbol_lower]['opened_layers'] == 0:
                    layer_info[symbol_lower]['entry_price'] = None
                    layer_info[symbol_lower]['order_type'] = None
                    layer_info[symbol_lower]['next_layer'] = 1
                    update_memory_after_order(symbol_upper, order['type'], False)
                if order['order_id'] in trailing_stops[symbol_upper][order['type']]:
                    del trailing_stops[symbol_upper][order['type']][order['order_id']]
                return f"Posição não encontrada para {symbol_upper} ({order['type']}), ordem removida localmente"
            if position_amt < qty:
                logger.warning("Quantidade insuficiente para %s (%s, camada %d): disponível=%.4f, solicitado=%.4f",
                              symbol_upper, order['type'], order['layer'], position_amt, qty)
                if close_all:
                    qty = format_quantity(symbol_upper, position_amt)
                    if qty == 0:
                        logger.warning("Quantidade ajustada para zero após formatação, pulando fechamento")
                        return f"Quantidade ajustada insuficiente para {symbol_upper} ({order['type']})"
                else:
                    return f"Quantidade insuficiente para {symbol_upper} ({order['type']}): disponível={position_amt:.4f}, solicitado={qty:.4f}"
        except ClientError as e:
            logger.error("Erro ao obter posição para %s: %s", symbol_upper, e)
            return f"Erro ao verificar posição para {symbol_upper}: {e}"
            
    gain = qty * (current_price - order['entry']) if order['type'] == 'long' else qty * (order['entry'] - current_price)
    gain *= (1 - FEE_RATE)
    
    if SIMULATED:
        total_gain += gain
        sim_daily_gain += gain
        sim_balance += (order['cost'] * (0.5 if is_partial else 1)) + gain
        logger.debug("Modo simulado: ganho=%.2f, novo saldo=%.2f", gain, sim_balance)
    else:
        try:
            if qty > 0:
                side = 'SELL' if order['type'] == 'long' else 'BUY'
                client_order_id = f"close_{order['order_id'].replace('-', '')[:30]}"
                response = binance_client.new_order(
                    symbol=symbol_upper,
                    side=side,
                    type='MARKET',
                    quantity=qty,
                    positionSide=order['type'].upper(),
                    newClientOrderId=client_order_id
                )
                realized_pnl = float(pos.get('realizedPnl', gain))
                total_gain += realized_pnl
                sim_daily_gain += realized_pnl
                logger.debug("Ordem real fechada: ganho realizado=%.2f", realized_pnl)
        except ClientError as e:
            logger.error("Erro ao fechar ordem para %s: %s", symbol_upper, e)
            return f"Erro ao fechar ordem para {symbol_upper}: {e}"
            
    if is_partial:
        order['amount'] -= qty
        order['cost'] *= 0.5
        order['tp1_hit'] = True
        order['trailing_stop_price'] = order['entry'] * (1 + TRAILING_STOP_MIN_PCT) if order['type'] == 'long' else order['entry'] * (1 - TRAILING_STOP_MIN_PCT)
        trailing_stops[symbol_upper][order['type']][order['order_id']] = order['trailing_stop_price']
        logger.debug("Fechamento parcial: %s (%s, camada %d), nova quantidade=%.4f, trailing stop=%.4f",
                     symbol_upper, order['type'], order['layer'], order['amount'], order['trailing_stop_price'])
        # Agendar verificação 10 segundos após fechamento parcial
        asyncio.create_task(schedule_post_close_check(symbol_upper, order['type'], 10))
    else:
        trade_count += 1
        if gain < 0:
            total_loss_count += 1
        orders[symbol_upper][order['type']].remove(order)
        layer_info[symbol_lower]['opened_layers'] -= 1
        if layer_info[symbol_lower]['opened_layers'] == 0:
            layer_info[symbol_lower]['entry_price'] = None
            layer_info[symbol_lower]['order_type'] = None
            layer_info[symbol_lower]['next_layer'] = 1
            update_memory_after_order(symbol_upper, order['type'], False)
        if order['order_id'] in trailing_stops[symbol_upper][order['type']]:
            del trailing_stops[symbol_upper][order['type']][order['order_id']]
            
    percentual = (gain / (sim_balance + gain)) * 100 if SIMULATED else (gain / get_account_balance()) * 100
    display_balance = sim_balance * 10 if SIMULATED and INFLATE_PUBLIC_BALANCE else get_account_balance() * 10 if INFLATE_PUBLIC_BALANCE else get_account_balance()
    display_gain = gain * 13 if INFLATE_PUBLIC_BALANCE else gain
    
    msg = f"""Ordem {'PARCIALMENTE ' if is_partial else ''}FECHADA
Par: {symbol_upper}
Tipo: {order['type'].upper()}
Camada: {order['layer']}
Ganho: {display_gain:.2f} USDT ({percentual:.2f}%)
Saldo Atual: {display_balance:.2f} USDT
Motivo: {reason}"""
    
    trade_log = {
        "timestamp": str(datetime.now(timezone.utc)),
        "symbol": symbol_upper,
        "type": order['type'],
        "layer": order['layer'],
        "gain": gain,
        "percentual": percentual,
        "balance": sim_balance if SIMULATED else get_account_balance(),
        "simulated": SIMULATED,
        "order_id": order['order_id'],
        "partial": is_partial,
        "reason": reason
    }
    save_trade_history(trade_log)
    logger.info("Ordem fechada para %s: %s", symbol_upper, msg)
    return msg

async def handle_kline_async(msg):
    symbol_lower = msg['s'].lower()
    symbol_upper = msg['s'].upper()
    close_price = float(msg['k']['c'])
    volume = float(msg['k']['v'])
    logger.info("Recebido novo candle para %s: Close=%.4f, Volume=%.2f", symbol_upper, close_price, volume)
    
    data[symbol_lower].append({'time': datetime.fromtimestamp(msg['k']['t']/1000, tz=timezone.utc), 'close': close_price, 'volume': volume})
    if len(data[symbol_lower]) > 22:
        data[symbol_lower] = data[symbol_lower][-22:]
    
    if len(data[symbol_lower]) < 22:
        logger.info("Aguardando %d candles para iniciar %s", 22 - len(data[symbol_lower]), symbol_upper)
        return
    
    df = pd.DataFrame(data[symbol_lower])
    ema7 = df['close'].ewm(span=7).mean().iloc[-1]
    ema21 = df['close'].ewm(span=21).mean().iloc[-1]
    prev = prev_emas[symbol_lower]
    
    if validar_sinal(symbol_upper, ema7, ema21, data[symbol_lower], volume):
        info = layer_info[symbol_lower]
        order_type = 'long' if ema7 > ema21 else 'short'
        
        # Só abre camada 1 com sinal EMA
        if info['opened_layers'] == 0:
            msg_order = await place_order(order_type, close_price, symbol_upper, 1)
            logger.info("Resultado da ordem %s para %s: %s", order_type.upper(), symbol_upper, msg_order)
        else:
            # Camadas subsequentes baseadas apenas no preço
            next_layer = info['next_layer']
            if next_layer <= len(LAYER_PCTS) and info['order_type'] == order_type:
                trigger_price = info['entry_price'] * (1 - LAYER_OFFSETS[next_layer - 1]) if order_type == 'long' else info['entry_price'] * (1 + LAYER_OFFSETS[next_layer - 1])
                if (order_type == 'long' and close_price <= trigger_price) or (order_type == 'short' and close_price >= trigger_price):
                    msg_order = await place_order(order_type, close_price, symbol_upper, next_layer)
                    logger.info("Resultado da ordem %s camada %d para %s: %s", order_type.upper(), next_layer, symbol_upper, msg_order)
    
    prev_emas[symbol_lower]['ema7'] = ema7
    prev_emas[symbol_lower]['ema21'] = ema21

async def process_new_candle(symbol, candle):
    logger.info("Processando novo candle para %s", symbol)
    symbol_upper = symbol.upper()
    symbol_lower = symbol.lower()
    data[symbol_lower].append({
        'time': datetime.fromtimestamp(candle['t']/1000, tz=timezone.utc),
        'close': float(candle['c']),
        'volume': float(candle['v'])
    })
    msg = {
        'e': 'kline',
        's': symbol_upper,
        'k': {
            't': candle['t'],
            'c': float(candle['c']),
            'v': float(candle['v']),
            'x': True
        }
    }
    await handle_kline_async(msg)

async def candle_listener(symbol):
    logger.info("Iniciando listener de candles para %s", symbol)
    url = f"wss://fstream.binance.com/ws/{symbol.lower()}@kline_{INTERVAL}"
    while True:
        try:
            async with websockets.connect(url) as ws:
                logger.info("WebSocket aberto para %s", symbol.upper())
                while True:
                    message = await ws.recv()
                    data = json.loads(message)
                    k = data["k"]
                    if k["x"]:
                        await process_new_candle(symbol.upper(), k)
        except websockets.exceptions.ConnectionClosed as e:
            logger.warning("WebSocket fechado para %s: %s", symbol.upper(), e)
            await asyncio.sleep(5)
        except Exception as e:
            logger.error("Erro no WebSocket para %s: %s", symbol.upper(), e)
            await asyncio.sleep(5)

async def verificar_tp(symbol):
    logger.info("Verificando take-profit/stop-loss para %s", symbol)
    symbol_upper = symbol.upper()
    symbol_lower = symbol.lower()
    price = latest_prices.get(symbol_lower)
    if not price:
        logger.warning("Preço não disponível para %s, pulando verificação", symbol_upper)
        return []
    df = pd.DataFrame(data[symbol_lower][-22:])
    if len(df) < 7:
        logger.warning("Dados insuficientes para calcular EMA7 para %s", symbol_upper)
        return []
    ema7 = df['close'].ewm(span=7).mean().iloc[-1]
    messages = []
    for order_type in ['long', 'short']:
        ativos = orders[symbol_upper][order_type]
        if not ativos:
            continue
        should_close_all = False
        close_reason = ''
        for order in ativos[:]:
            change = (price - order['entry']) / order['entry'] if order['type'] == 'long' else (order['entry'] - price) / order['entry']
            if not order['tp1_hit'] and change >= TP_PCT:
                logger.info("Take-profit TP1 atingido para %s (%s, camada %d): mudança=%.4f",
                           symbol_upper, order_type, order['layer'], change)
                msg = await close_order(order, price, symbol_upper, is_partial=True, reason='tp1')
                messages.append(msg)
                if all(o['tp1_hit'] for o in ativos):
                    should_close_all = True
                    close_reason = 'tp1_all_layers'
            elif order['tp1_hit']:
                min_trailing_stop = order['entry'] * (1 + TRAILING_STOP_MIN_PCT) if order['type'] == 'long' else order['entry'] * (1 - TRAILING_STOP_MIN_PCT)
                current_trailing_stop = trailing_stops[symbol_upper][order_type].get(order['order_id'], order['trailing_stop_price'])
                new_trailing_stop = ema7 if order['type'] == 'long' and ema7 > min_trailing_stop else ema7 if order['type'] == 'short' and ema7 < min_trailing_stop else current_trailing_stop
                new_trailing_stop = max(new_trailing_stop, min_trailing_stop) if order['type'] == 'long' else min(new_trailing_stop, min_trailing_stop)
                if new_trailing_stop != current_trailing_stop:
                    logger.info("Trailing stop atualizado para %s (%s, camada %d): de %.4f para %.4f, mínimo=%.4f",
                                symbol_upper, order_type, order['layer'], current_trailing_stop, new_trailing_stop, min_trailing_stop)
                    trailing_stops[symbol_upper][order_type][order['order_id']] = new_trailing_stop
                    order['trailing_stop_price'] = new_trailing_stop
                if (order['type'] == 'long' and price <= new_trailing_stop) or (order['type'] == 'short' and price >= new_trailing_stop):
                    logger.info("Trailing stop acionado para %s (%s, camada %d): preço=%.4f, stop=%.4f",
                                symbol_upper, order_type, order['layer'], price, new_trailing_stop)
                    should_close_all = True
                    close_reason = 'trailing_stop'
            elif change <= -SL_PCT:
                logger.info("Stop-loss atingido para %s (%s, camada %d): mudança=%.4f",
                           symbol_upper, order_type, order['layer'], change)
                should_close_all = True
                close_reason = 'stop_loss'
        if should_close_all:
            for order in ativos[:]:
                msg = await close_order(order, price, symbol_upper, close_all=True, reason=close_reason)
                messages.append(msg)
            orders[symbol_upper][order_type].clear()
            layer_info[symbol_lower]['opened_layers'] = 0
            layer_info[symbol_lower]['entry_price'] = None
            layer_info[symbol_lower]['order_type'] = None
            layer_info[symbol_lower]['next_layer'] = 1
            update_memory_after_order(symbol_upper, order_type, False)
            trailing_stops[symbol_upper][order_type].clear()
    return messages

async def monitor_account():
    logger.info("Iniciando monitoramento da conta")
    global sim_day, sim_daily_gain
    while True:
        try:
            if sim_day != date.today():
                logger.info("Novo dia detectado, resetando ganho diário e contagem de trades")
                sim_day = date.today()
                sim_daily_gain = 0
                for sym in SYMBOLS:
                    trade_cooldown[sym.upper()]['trade_count'] = 0
            summary = await get_futures_summary()
            if not summary:
                logger.warning("Resumo da conta não obtido, aguardando próxima tentativa")
                await asyncio.sleep(60)
                continue
            for sym in SYMBOLS:
                price = get_price_rest(sym)
                if price:
                    latest_prices[sym.lower()] = price
                    messages = await verificar_tp(sym)
                    for m in messages:
                        logger.info("Resultado da verificação TP/SL para %s: %s", sym.upper(), m)
            small_order_msgs = await close_small_orders()
            for m in small_order_msgs:
                logger.info("Fechamento automático de ordem pequena: %s", m)
            logger.info("Resumo da conta: %s", summary)
            await asyncio.sleep(60)
        except Exception as e:
            logger.error("Erro no monitoramento da conta: %s", e)
            await asyncio.sleep(60)

async def main():
    logger.info("Iniciando função principal do bot")
    global sim_balance, sim_daily_gain, total_gain, trade_count, total_loss_count, sim_day
    logger.info("Modo: %s - Saldo inicial: %.2f USDT", 'REAL' if not SIMULATED else 'SIMULADO', sim_balance)
    
    try:
        binance_client.ping()
        logger.info("Conexão com Binance estabelecida")
    except Exception as e:
        logger.error("Erro ao conectar com a Binance: %s", e)
        raise
    
    for sym in SYMBOLS:
        df = await get_kline_data(sym, interval='1m', limit=22)
        if df is None or len(df) < 22:
            logger.error("Não foi possível obter 22 candles para %s", sym.upper())
            continue
        candles = df[['close']].to_dict('records')
        ema7 = calcula_ema(candles, 7)
        ema21 = calcula_ema(candles, 21)
        if ema7 is not None and ema21 is not None:
            prev_emas[sym.lower()] = {'ema7': ema7, 'ema21': ema21}
            logger.info("%s - Candle inicial 22 | EMA7=%.4f | EMA21=%.4f", sym.upper(), ema7, ema21)
        rsi = calcula_rsi(candles)
        if rsi is not None:
            logger.info("%s - RSI inicial=%.2f", sym.upper(), rsi)
    
    summary = await get_futures_summary()
    if not summary and not SIMULATED:
        logger.error("Falha crítica: Não foi possível obter o resumo da conta")
        raise ValueError("Não foi possível obter o resumo da conta")
    
    logger.info("Resumo inicial da conta: %s", summary)
    
    websocket_task = asyncio.create_task(start_websocket())
    monitor_task = asyncio.create_task(monitor_account())
    await asyncio.gather(websocket_task, monitor_task)

async def start_websocket():
    logger.info("Iniciando WebSockets para todos os símbolos")
    tasks = [asyncio.create_task(candle_listener(sym)) for sym in SYMBOLS]
    logger.info("WebSockets iniciados para todos os símbolos")
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    logger.info("Iniciando o bot VKINHA Trading")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot interrompido pelo usuário")
    except Exception as e:
        logger.error("Erro fatal no bot: %s", e)