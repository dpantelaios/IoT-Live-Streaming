import datetime, timedelta
from randomtimestamp import random_time
import random
import time
from json import dumps
from kafka import KafkaProducer

current_date = datetime.datetime(2020, 5, 17)
msg_interval = 0.125

random.seed(15)
#GENERATE EVERY 15 MINUTES(1 seconds in our simulation)
def generate_thermal_sensor_values():
    return round(random.uniform(12, 35), 2)

def generate_energy_air_conditioner(min_value, max_value):
    return round(random.uniform(min_value, max_value), 2)

def generate_energy_rest_devices(min_value, max_value):
    return round(random.uniform(min_value, max_value), 2)

def generate_water_consumption():
    return round(random.uniform(0, 1), 2)

#GENERATE EVERY 1 DAY(96 seconds in our simulation)
def generate_Energy_total():
    return round(random.uniform(-1000, 1000), 2)

def generate_total_water_consumptions():
    return 110 + round(random.uniform(-10, 10), 2)

def generate_move_detection_daily(date):
    total_moves = random.randint(4, 6)
    timestamps = []
    for i in range(total_moves):
        temp_time = random_time()
        temp_time = date + timedelta.Timedelta(hours = temp_time.hour, minutes = temp_time.minute)
        timestamps.append(temp_time)
    timestamps.sort()
    return timestamps

producer = KafkaProducer(
    bootstrap_servers=['kafka:29090'],
    value_serializer=lambda x: dumps(x).encode('utf-8'),
    key_serializer=lambda x: dumps(x).encode('utf-8')
    )

starttime = time.time()
Etotal = Water_total = 0

two_days_late_w1_count = 0
ten_days_late_w1_count = 0

### debug variables ###
daily_hvac1=0
daily_hvac2=0
daily_miac1=0
daily_miac2=0
dailyEtotal=0
daily_w1 = 0
dailyWtot=0
total_moves = 0
### debug variables ###

while True:
    s = current_date.timestamp()
    
    # 15 minute data
    th1_val = generate_thermal_sensor_values()
    th2_val = generate_thermal_sensor_values()
    hvac1_val = generate_energy_air_conditioner(0, 100)
    hvac2_val = generate_energy_air_conditioner(0, 200)
    miac1_val = generate_energy_rest_devices(0, 150)
    miac2_val = generate_energy_rest_devices(0, 200)
    w1_val = generate_water_consumption()

    th1 = {"produceDate":str(current_date), "value":str(th1_val)}
    th2 = {"produceDate":str(current_date), "value":str(th2_val)}
    
    hvac1 = {"produceDate":str(current_date), "value":str(hvac1_val)}
    hvac2 = {"produceDate":str(current_date), "value":str(hvac2_val)}
    miac1 = {"produceDate":str(current_date), "value":str(miac1_val)}
    miac2 = {"produceDate":str(current_date), "value":str(miac2_val)}

    w1 = {"produceDate":str(current_date), "value":str(w1_val)}

    # tst = {"measurement":"weather", "temperature": 33.6, "Timestamp":1465839830100400200}
    producer.send('min15', value=th1, key="th1")
    producer.send('min15', value=th2, key="th2")
    
    producer.send('min15', value=hvac1, key="hvac1")
    producer.send('min15', value=hvac2, key="hvac2")
    producer.send('min15', value=miac1, key="miac1")
    producer.send('min15', value=miac2, key="miac2")

    producer.send('min15', value=w1, key="w1")

    if current_date.hour == 0 and current_date.minute == 0:
        print("hvac1 daily sum: {}, hvac2 daily sum: {}, miac1 daily sum: {}\nenergy total: {}, device sum: {}, difference: {}".format(daily_hvac1, daily_hvac2, daily_miac1, dailyEtotal, daily_hvac1+daily_hvac2+daily_miac1+daily_miac2, dailyEtotal-(daily_hvac1+daily_hvac2+daily_miac1+daily_miac2)))
        print("w1 dail sum for {}: {}, Wtot: {}, difference: {}".format(current_date - timedelta.Timedelta(days=1), daily_w1, dailyWtot, dailyWtot-daily_w1))
        
        # daily data Etot, Wtot
        dailyEtotal = 2600*24 + generate_Energy_total()
        Etotal += dailyEtotal
        Etotal = round(Etotal, 2)
        
        dailyWtot = generate_total_water_consumptions()
        Water_total += dailyWtot
        Water_total = round(Water_total, 2)
        
        #generate 4-6 timestamps per day to send a move detection
        timestamps = generate_move_detection_daily(current_date)
        producer.send('day', value={"produceDate":str(current_date), "value":str(Etotal)}, key="etot")
        producer.send('day', value={"produceDate":str(current_date), "value":str(Water_total)}, key="wtot")

        ### debug variables ###
        daily_hvac1 = 0
        daily_hvac2 = 0
        daily_miac1 = 0
        daily_miac2 = 0
        daily_w1 = 0
    
    ### debug variables ###
    daily_hvac1 = daily_hvac1 + hvac1_val
    daily_hvac2 = daily_hvac2 + hvac2_val
    daily_miac1 = daily_miac1 + miac1_val
    daily_miac2 = daily_miac2 + miac2_val
    daily_w1 += w1_val
    ### debug variables ###

    # check if it is time to send a move detection
    for temp_timestamp in timestamps:
        if temp_timestamp <= current_date:
            mov1 = {"produceDate":str(current_date), "value":1}
            producer.send('movementSensor', value=mov1, key="moveDetection")

            ### debug variable ###
            # total_moves += 1
            # print("MOVE DETECTION: current_date: {}, total_move_detections: {}".format(current_date, total_moves))
            ### debug variable ###     
                   
            timestamps.pop(0)

    # check if it is time to send a late accepted event for w1
    if two_days_late_w1_count == 20:
        two_days_early_date = current_date - timedelta.Timedelta(days=2)
        two_days_late_w1_count = 0
        two_days_late_w1_val = generate_water_consumption()
        two_days_late_w1 = {"produceDate":str(two_days_early_date), "value":str(two_days_late_w1_val)}
        print("Late accepted: current_date: {}, sent_date: {}, w1_value: {}".format(current_date, two_days_early_date, two_days_late_w1_val))
        producer.send('min15', value=two_days_late_w1, key="w1")

    # check if it is time to send a late rejected event for w1
    if ten_days_late_w1_count == 120:
        ten_days_early_date = current_date - timedelta.Timedelta(days=10)
        ten_days_late_w1_count = 0
        ten_days_late_w1_val = generate_water_consumption()
        ten_days_late_w1 = {"produceDate":str(ten_days_early_date), "value":str(ten_days_late_w1_val)}
        # print("Late rejected: current_date: {}, sent_date: {}, w1_value: {}".format(current_date, ten_days_early_date, ten_days_late_w1_val))
        producer.send('min15', value=ten_days_late_w1, key="w1")

    two_days_late_w1_count += 1
    ten_days_late_w1_count += 1
    
    #move to next timestamp
    current_date = current_date + timedelta.Timedelta(minutes=15)
    #interval between messages
    time.sleep(msg_interval - ((time.time() - starttime) % msg_interval))
