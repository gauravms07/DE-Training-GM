import json, random, time 
from datetime import datetime 
from kafka import KafkaProducer 


producer = KafkaProducer( 
    bootstrap_servers='gaurav-test-namespace.servicebus.windows.net:9093', 
    security_protocol='SASL_SSL', 
    sasl_mechanism='PLAIN', 
    sasl_plain_username='$ConnectionString', 
    sasl_plain_password='Endpoint=sb://gaurav-test-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=J45GoR86k6KOBb4X+4Q9iuhE2SjXVXJRi+AEhMiAvIc=', 
    value_serializer=lambda v: json.dumps(v).encode('utf-8'), 
    key_serializer=lambda k: k.encode('utf-8') 
) 

 
users = ["U100", "U101", "U102"] 
locations = ["Mumbai", "Delhi", "Bangalore", "NYC", "London"] 


def generate_txn(): 
    return { 
        "transactionId": f"TX{random.randint(1000,9999)}", 
        "cardNumber": f"9876-XXXX-XXXX-{random.randint(1000,9999)}", 
        "amount": round(random.uniform(100, 100000), 2), 
        "location": random.choice(locations), 
        "timestamp": datetime.utcnow().isoformat(), 
        "userId": random.choice(users) 
    } 

 
while True: 
    txn = generate_txn() 
    print("Sending:", txn) 
    producer.send("transactions", key=txn["transactionId"], value=txn) 
    time.sleep(1) 