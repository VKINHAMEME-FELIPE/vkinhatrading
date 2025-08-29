# Bot - Modo Simulado/Testnet

## 🚀 Instruções
1. Crie uma conta na Binance Testnet:
   https://testnet.binancefuture.com

2. Gere sua `API_KEY` e `API_SECRET` de Testnet

3. Ative o modo testnet no seu código adicionando:
```python
client = Client(API_KEY, API_SECRET, testnet=True)
```

4. Rode o script:
```bash
pip install -r requirements.txt
python bot_cointech2u_style.py
```

## 🛡️ Segurança
- Este bot **não executa ordens reais** se configurado corretamente com Testnet.
- Para simulações sem ordem nenhuma, modifique as funções `futures_create_order` para apenas imprimir logs.
