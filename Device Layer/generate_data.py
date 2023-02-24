import datetime, timedelta
from randomtimestamp import random_time
import random
import time
# from datetime import datetime
from json import dumps
from kafka import KafkaProducer

starting_date = datetime.datetime(2020, 5, 17)
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
        # print(temp_time)
        # print(temp_time.hour)
        temp_time = date + timedelta.Timedelta(hours = temp_time.hour, minutes = temp_time.minute)
        timestamps.append(temp_time)
    timestamps.sort()
    # print(timestamps)
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

daily_hvac1=0
daily_hvac2=0
daily_miac1=0
dailyEtotal=0
daily_w1 = 0
dailyWtot=0

while True:
    # print(starting_date)
    s = starting_date.timestamp()
    # print(s)

    th1 = {"produceDate":str(starting_date), "value":str(generate_thermal_sensor_values())}
    th2 = str(starting_date) + " | " + str(generate_thermal_sensor_values())
    hvac1_val = generate_energy_air_conditioner(0, 100)
    hvac2_val = generate_energy_air_conditioner(0, 200)
    miac1_val = generate_energy_rest_devices(0, 150)
    hvac1 = {"produceDate":str(starting_date), "value":str(hvac1_val)}
    hvac2 = {"produceDate":str(starting_date), "value":str(hvac2_val)}
    miac1 = {"produceDate":str(starting_date), "value":str(miac1_val)}
    miac2 = str(starting_date) + " | " + str(generate_energy_rest_devices(0, 200))
    w1_val = generate_water_consumption()
    w1 = {"produceDate":str(starting_date), "value":str(w1_val)}
    
    # print("TH1: ", th1)
    # print("TH2: ", th2)
    # print("HVAC1: ", hvac1)
    # print("HVAC2: ", hvac2)
    # print("miac1: ", miac1)
    # print("MIAC2: ", miac2)
    # print("W1: ", w1)
    # tst = {"measurement":"weather", "temperature": 33.6, "Timestamp":1465839830100400200}
    # producer.send('RAW', value={"temperature":33.6}, key="th1")
    
    # producer.send('RAW', value={"produceDate":s, "value":generate_thermal_sensor_values()}, key="th1")
    
    # producer.send('th1', value=th1, key="th1")
    # producer.send('th2', value=th2)
    
    ## producer.send('th1', value=hvac1, key="hvac1")
    ## producer.send('th1', value=hvac2, key="hvac2")
    ## producer.send('th1', value=miac1, key="miac1")
    ## producer.send('th1', value=w1, key="w1")

    # producer.send('hvac2', value=hvac2)
    # producer.send('miac1', value=miac1)
    # producer.send('miac2', value=miac2)
    # producer.send('w1', value=w1)

    # print("hvac1 : {}, hvac2 : {}, miac1 : {},total sum: {}".format(hvac1_val, hvac2_val, miac1_val, daily_hvac1))

    if starting_date.hour == 0 and starting_date.minute == 0:
        print("hvac1 daily sum: {}, hvac2 daily sum: {}, miac1 daily sum: {}\nenergy total: {}, device sum: {}, difference: {}".format(daily_hvac1, daily_hvac2, daily_miac1, dailyEtotal, daily_hvac1+daily_hvac2+daily_miac1, dailyEtotal-(daily_hvac1+daily_hvac2+daily_miac1)))
        print("w1 dail sum: {}, Wtot: {}, difference: {}".format(daily_w1, dailyWtot, dailyWtot-daily_w1))
        dailyEtotal = 2600*24 + generate_Energy_total()
        Etotal += dailyEtotal
        Etotal = round(Etotal, 2)
        dailyWtot = generate_total_water_consumptions()
        Water_total += dailyWtot
        Water_total = round(Water_total, 2)
        timestamps = generate_move_detection_daily(starting_date)
        Etotal_str = str(starting_date) + " | " + str(Etotal)
        Water_total_str = str(starting_date) + " | " + str(Water_total)
        # print("Etot: ", Etotal_str)
        ## producer.send('etot', value={"produceDate":str(starting_date), "value":str(Etotal)}, key="etot")
        # print("Water_total_str: ", Water_total_str)
        # producer.send('etot', value={"produceDate":str(starting_date), "value":str(Water_total)}, key="wtot")

        ## print("hvac1 daily sum: {}".format(hvac1_daily_sum))
        daily_hvac1 = 0
        daily_hvac2 = 0
        daily_miac1 = 0
        daily_w1 = 0
    
    daily_hvac1 = daily_hvac1 + hvac1_val
    daily_hvac2 = daily_hvac2 + hvac2_val
    daily_miac1 = daily_miac1 + miac1_val
    daily_w1 += w1_val

    for temp_timestamp in timestamps:
        if temp_timestamp <= starting_date:
            # print("SENT MOVE DETECTION")
            mov1 = str(temp_timestamp) + " | 1"
            # print(mov1)
            # producer.send('mov1', value=mov1)
            timestamps.pop(0)
    if two_days_late_w1_count == 20:
        two_days_early_date = starting_date - timedelta.Timedelta(days=2)
        two_days_late_w1_count = 0
        two_days_late_w1_val = generate_water_consumption()
        two_days_late_w1 = {"produceDate":str(two_days_early_date), "value":str(two_days_late_w1_val)}
        print("current_date: {}, sent_date: {}, w1_value: {}".format(starting_date, two_days_early_date, two_days_late_w1_val))
        ## producer.send('th1', value=two_days_late_w1, key="two_days_late_w1")

    if ten_days_late_w1_count == 120:
        ten_days_early_date = starting_date - timedelta.Timedelta(days=10)
        ten_days_late_w1_count = 0
        ten_days_late_w1_val = generate_water_consumption()
        ten_days_late_w1 = {"produceDate":str(ten_days_early_date), "value":str(ten_days_late_w1_val)}
        print("current_date: {}, sent_date: {}, w1_value: {}".format(starting_date, ten_days_early_date, ten_days_late_w1_val))
       ## producer.send('th1', value=ten_days_late_w1, key="ten_days_late_w1")

    two_days_late_w1_count += 1
    ten_days_late_w1_count += 1
    
    starting_date = starting_date + timedelta.Timedelta(minutes=15)
    time.sleep(msg_interval - ((time.time() - starttime) % msg_interval))
