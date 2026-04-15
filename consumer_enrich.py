from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    tx = message.value
    amount = tx.get('amount', 0)

    # określenie poziomu ryzyka
    if amount > 3000:
        risk = "HIGH"
    elif amount > 1000:
        risk = "MEDIUM"
    else:
        risk = "LOW"

    # dodanie pola
    tx['risk_level'] = risk

    print(tx)
