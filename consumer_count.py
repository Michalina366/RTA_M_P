from kafka import KafkaConsumer
from collections import Counter, defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='count-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = defaultdict(float)
msg_count = 0

# TWÓJ KOD
# Dla każdej wiadomości:
#   1. store_counts[store] += 1
#   2. total_amount[store] += amount
#   3. Co 10 wiadomości: print tabela

for message in consumer:
    tx = message.value
    store = tx.get('store')
    amount = tx.get('amount', 0)

    # 1. liczba transakcji
    store_counts[store] += 1

    # 2. suma kwot
    total_amount[store] += amount

    msg_count += 1

    # 3. co 10 wiadomości — wypisz tabelę
    if msg_count % 10 == 0:
        print("\n=== PODSUMOWANIE ===")
        print(f"{'Sklep':<10} | {'Liczba':<6} | {'Suma (PLN)':<12}")
        print("-" * 32)
        
        for s in store_counts:
            print(f"{s:<10} | {store_counts[s]:<6} | {total_amount[s]:<12.2f}")
        
        print(f"\nPrzetworzono wiadomości: {msg_count}\n")
