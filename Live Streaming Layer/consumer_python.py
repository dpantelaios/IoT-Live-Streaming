from kafka import KafkaConsumer
from json import loads
from time import sleep


consumer_th1 = KafkaConsumer(
    bootstrap_servers=['kafka:29090'],
    #bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group-id',
    value_deserializer=lambda x: loads(x.decode('utf-8')),
    #value_deserializer = lambda x: x.decode('utf-8'),
    key_deserializer=lambda x: loads(x.decode('utf-8'))
)

# consumer_th1.subscribe(['th1', 'th2','hvac1', 'hvac2', 'miac1', 'miac2','w1', 'e_tot', 'mov1', 'wtot'])
# consumer_th1.subscribe(['AGGREGATED_DIFF'])
# consumer_th1.subscribe(['th1'])
# consumer_th1.subscribe(['etot'])
# consumer_th1.subscribe(['AGGREGATED'])
consumer_th1.subscribe(['RAW'])


for event in consumer_th1:
    #print(event)
    event_data = event.value
    # Do whatever you want
    print(event.topic, " ",  event_data)
    #sleep(2)