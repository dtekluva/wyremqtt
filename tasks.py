from time import sleep
from config import app


from pathlib import WindowsPath
import random
# import winsound
import requests
import datetime
import json

from paho.mqtt import client as mqtt_client

from database import db_manager

from celery import Celery


app = Celery('tasks', broker = 'amqps://shuzgizc:Cyn1AqSowjmaU3KWIhF_6heAERl-FHJN@fish.rmq.cloudamqp.com/shuzgizc', backend='db+postgresql://postgres:19sedimat54@localhost/postgres')

@app.task
def reverse(text):
    sleep(5)
    print("Done")
    return text[::-1]


broker = 'broker.emqx.io'
port = 1883
topic = "/gw/acrelHW/#"
# generate client ID with pub prefix randomly
client_id = f'python-mqtt-{random.randint(0, 100)}'
username = 'emqx'
password = 'public'


def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
        log_data(json.loads(msg.payload.decode()))
        # winsound.Beep(2000, 100)

    client.subscribe(topic)
    client.on_message = on_message


def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()

def log_data(data):

    if data.get("type") != "data":

        with open("file.txt", "a") as file:
            file.write(f"Irrelevant Data -> {data}\n\n")
        print("IRRELEVANT --> ", data)

    elif data.get("meterStatus") == "missing":

        with open("file.txt", "a") as file:
            file.write(f"Missing Device -> {data.get('meterSN')}\n\n")
        print("PASSING --> ", data)

    else:

        with open("file.txt", "a") as file:
            file.write(f"{data}\n\n")

        compile_for_wyre(data)

@app.task
def compile_for_wyre(data):
    print("DATA : ", data)
    
    # data = {"type":"data","time":"20211124101800","gwSN":"12109132830001","meterSN":"12109132830004","meterName":"adl400","meterStatus":"normal","ch":0,"ua":221,"ub":220.9,"uc":220.9,"ia":random.randint(1,50),"ib":random.randint(1,50),"ic":random.randint(1,50),"pa":random.randint(1,50),"pb":random.randint(1,50),"pc":random.randint(1,50),"p":random.randint(1,50),"qa":random.randint(1,50),"qb":random.randint(1,50),"qc":random.randint(1,50),"q":random.randint(1,50),"pf":random.randint(1,50),"pt":1,"ct":40,"wh":8.98,"wl":0.6,"varh":5.21,"varl":5.43}


    try:
        payload = {
                    "device_id": data["meterSN"],
                    "post_datetime": str(datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')),
                    "post_date": str(datetime.datetime.now().strftime('%Y-%m-%d')),
                    "post_time": str(datetime.datetime.now().strftime('%H:%M:%S')),
                    "voltage_l1_l12":data["ua"],
                    "voltage_l2_l23":data["ub"],
                    "voltage_l3_l31":data["uc"],
                    "current_l1":data["ia"]*40,
                    "current_l2":data["ib"]*40,
                    "current_l3":data["ic"]*40,
                    "kw_l1":data["pa"]*40,
                    "kw_l2":data["pb"]*40,
                    "kw_l3":data["pc"]*40,
                    "kvar_l1":data["qa"]*40,
                    "kvar_l2":data["qb"]*40,
                    "kvar_l3":data["qc"]*40,
                    "kva_l1":data["pa"]*data["pf"],
                    "kva_l2":data["pb"]*data["pf"],
                    "kva_l3":data["pc"]*data["pf"],
                    "power_factor_l1":0,
                    "power_factor_l2":0,
                    "power_factor_l3":0,
                    "total_kw":data["p"]*40,
                    "total_kvar":data["q"]*40,
                    "total_kva":data["p"]*40*data["pf"],
                    "total_pf":data["pf"],
                    "avg_frequency":0,
                    "neutral_current":0,
                    "volt_thd_l1_l12":0,
                    "volt_thd_l2_l23":0,
                    "volt_thd_l3_l31":0,
                    "current_thd_l1":0,
                    "current_thd_l2":0,
                    "current_thd_l3":0,
                    "current_tdd_l1":0,
                    "current_tdd_l2":0,
                    "current_tdd_l3":0,
                    "kwh_import":data["wh"]*data["ct"],
                    "kwh_export":0,
                    "kvarh_import":data["varh"]/1000,
                    "kvah_total":0,
                    "max_amp_demand_l1":0,
                    "max_amp_demand_l2":0,
                    "max_amp_demand_l3":0,
                    "max_sliding_window_kw_demand":0,
                    "accum_kw_demand":0,
                    "max_sliding_window_kva_demand":0,
                    "present_sliding_window_kw_demand":0,
                    "present_sliding_window_kva_demand":0,
                    "accum_kva_demand":0,
                    "pf_import_at_maximum_kva_sliding_window_demand":0
                }
                
        datalogs_payload = {
                              "total_kw": payload["total_kw"],
                              "post_date": payload["post_date"],
                              "post_time": payload["post_time"],
                              "pulse_counter": 0.0,
                              "post_datetime": payload["post_datetime"],
                              "digital_input_1": 0.0,
                              "digital_input_2": 0.0,
                              "digital_input_3": 0.0,
                              "digital_input_4": 0.0,
                              "summary_energy_register_1": payload["kwh_import"],
                              "summary_energy_register_2": payload["kwh_import"],
                              "device_id": payload["device_id"] 
                          }
                          
        print("RAW LOGS", datalogs_payload)
        db_manager.write_to_readings_table(**payload)
        # print(payload)
        if datalogs_payload["summary_energy_register_1"] == 0:
            return dict()
            
        response = requests.post("https://wyreng.xyz/posts/reading/", json=payload)
        print("RESPONSE : ", response.content)
        response = requests.post("https://wyreng.xyz/posts/datalog/", json=datalogs_payload)
        print("DATALOG RESPONSE : ", response.content)
        
    except KeyError:
        print("error")