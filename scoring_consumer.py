from kafka import KafkaConsumer, KafkaProducer
import json
from datetime import datetime

consumer = KafkaConsumer('transactions', bootstrap_servers='broker:9092',
    auto_offset_reset='earliest', group_id='scoring-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

alert_producer = KafkaProducer(bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for tx in consumer:
    tx = tx.value

    score = 0

    amount = tx.get('amount', 0)
    category = tx.get('category', '')

    # godzina z timestampu
    hour = datetime.fromisoformat(tx['timestamp']).hour

    # R1
    if amount > 3000:
        score += 3

    # R2
    if category == 'elektronika' and amount > 1500:
        score += 2

    # R3
    if hour < 6:
        score += 2

    # ALERT
    if score >= 3:
        alert = {
            'tx_id': tx.get('tx_id'),
            'score': score,
            'tx': tx
        }

        alert_producer.send('alerts', value=alert)
        print("ALERT:", alert)
