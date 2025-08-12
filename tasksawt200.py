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
from pprint import pprint


app = Celery('tasks', broker = 'amqps://shuzgizc:Cyn1AqSowjmaU3KWIhF_6heAERl-FHJN@fish.rmq.cloudamqp.com/shuzgizc', backend='db+postgresql://postgres:19sedimat54@localhost/postgres')

@app.task
def reverse(text):
    sleep(5)
    print("Done")
    return text[::-1]



broker = 'broker.emqx.io'
port = 1883
topic = "data/dev/#"
# generate client ID with pub prefix randomly
client_id = f'python-mqtt-{random.randint(0, 100)}'
username = 'broker'
password = 'public'

print("AWT200..!!")

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

    if data.get("data"):

        data["adw300"] = True
        compile_for_wyre(data)

    elif data.get("reported"):

        data["adw300"] = True
        compile_for_wyre(data)

    elif data.get("type") != "data":

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

def repackage_for_awt200(data):

    # reported_key = data.get("reported", None)
    # device_id = list(reported_key.keys())[0]
    # device_actual_id = device_id.split("_")[-1]

    reported_key = data.get("reported", None)

    if reported_key:
        device_id = list(reported_key.keys())[0]
        # Split on "_" and get the longest chunk
        device_actual_id = max(device_id.split("_"), key=len)


    if reported_key is None:
        return {}

    data = data.get("reported", None)[device_id]
    template = {
            "type":"data",
            "state":data.get("state"),
            "time":float(data.get("timestamp",0)),
            "gwSN":"00000000000",
            "meterSN":device_actual_id,
            "meterName":"AWT200",
            "meterStatus":"normal",
            "ch":0,
            "ua":float(data.get("Ua",0)),
            "ub":float(data.get("Ub",0)),
            "uc":float(data.get("Uc",0)),
            "ia":float(data.get("Ia",0)),
            "ib":float(data.get("Ib",0)),
            "ic":float(data.get("Ic",0)),
            "pa":float(data.get("Pa",0)),
            "pb":float(data.get("Pb",0)),
            "pc":float(data.get("Pc",0)),
            "p":float(data.get("P",0)),
            "qa":float(data.get("Qa",0)),
            "qb":float(data.get("Qb",0)),
            "qc":float(data.get("Qc",0)),
            "q":float(data.get("Q",0)),
            "pfa":float(data.get("PFa",0)),
            "pfb":float(data.get("PFb",0)),
            "pfc":float(data.get("PFc",0)),
            "pf":float(data.get("PF",0)),
            "pt":1,
            "ct":1,
            "wh":float(data.get("EP",0)),
            "wl":float(data.get("EQ",0)),
            "varh":float(data.get("reported",0)),
            "varl":47.28,
            "hz":float(data.get( "Fr",0))
            }

    pprint(template)
    return template


@app.task
def compile_for_wyre(data):
    print("DATA : ", data)

    # data = {"type":"data","time":"20211124101800","gwSN":"12109132830001","meterSN":"12109132830004","meterName":"adl400","meterStatus":"normal","ch":0,"ua":221,"ub":220.9,"uc":220.9,"ia":random.randint(1,50),"ib":random.randint(1,50),"ic":random.randint(1,50),"pa":random.randint(1,50),"pb":random.randint(1,50),"pc":random.randint(1,50),"p":random.randint(1,50),"qa":random.randint(1,50),"qb":random.randint(1,50),"qc":random.randint(1,50),"q":random.randint(1,50),"pf":random.randint(1,50),"pt":1,"ct":40,"wh":8.98,"wl":0.6,"varh":5.21,"varl":5.43}

    if data.get("timestamp"):

        timestamp = data.get("timestamp")
        china_tz = datetime.timezone(datetime.timedelta(hours=8))
        dt_object = datetime.datetime.fromtimestamp(timestamp, tz=china_tz)
        timestamp = dt_object
        print("TIMESTAMP::::::", timestamp)
        print("TIMESTAMP::::::", timestamp)
        print("TIMESTAMP::::::", timestamp)
        print("TIMESTAMP::::::", timestamp)

    else:
        timestamp = datetime.datetime.now()
    timestamp = datetime.datetime.now()

    if data.get("adw300") == True:
        data = repackage_for_awt200(data)

    device_id = data["meterSN"] if not (data.get("ch", "")) else data["meterSN"] + "-" + str(data.get("ch", "")) # Add meter channel to nmeter number if the meter has a channel.

    # if data["p"]*data.get("ct", 1) if data.get("ct", False) or not data.get("meterName", False) else data["p"]/1000 < 1:

    #     return
    # if data.get("state") == "OFFLINE":
    #     return

    try:
        try:
            pf = data["pf"] or 1
        except:
            pf = 0

        payload = {
            "device_id": device_id,
            "post_datetime": str(timestamp.strftime('%Y-%m-%dT%H:%M:%S')),
            "post_date": str(timestamp.strftime('%Y-%m-%d')),
            "post_time": str(timestamp.strftime('%H:%M:%S')),
            "voltage_l1_l12":data["ua"],
            "voltage_l2_l23":data["ub"],
            "voltage_l3_l31":data["uc"],
            "current_l1":data["ia"]*data.get("ct", 1),
            "current_l2":data["ib"]*data.get("ct", 1),
            "current_l3":data["ic"]*data.get("ct", 1),
            "kw_l1":data["pa"]*data.get("ct", 1) if data.get("ct", False) or not data.get("meterName", False) else data["pa"]/1000,
            "kw_l2":data["pb"]*data.get("ct", 1) if data.get("ct", False) or not data.get("meterName", False) else data["pb"]/1000,
            "kw_l3":data["pc"]*data.get("ct", 1) if data.get("ct", False) or not data.get("meterName", False) else data["pc"]/1000,
            "kvar_l1":data["qa"]*data.get("ct", 1) if data.get("ct", False) or not data.get("meterName", False) else data["qa"]/1000,
            "kvar_l2":data["qb"]*data.get("ct", 1) if data.get("ct", False) or not data.get("meterName", False) else data["qb"]/1000,
            "kvar_l3":data["qc"]*data.get("ct", 1) if data.get("ct", False) or not data.get("meterName", False) else data["qc"]/1000,
            "kva_l1":data["pa"]/pf if data.get("ct", False) or not data.get("meterName", False) else (data["pa"]/1000)/pf or 1,
            "kva_l2":data["pb"]/pf if data.get("ct", False) or not data.get("meterName", False) else (data["pb"]/1000)/pf,
            "kva_l3":data["pc"]/pf if data.get("ct", False) or not data.get("meterName", False) else (data["pa"]/1000)/pf,
            "power_factor_l1":0,
            "power_factor_l2":0,
            "power_factor_l3":0,
            "total_kw":data["p"]*data.get("ct", 1) if data.get("ct", False) or not data.get("meterName", False) else data["p"]/1000,
            "total_kvar":data["q"]*data.get("ct", 1) if data.get("ct", False) or not data.get("meterName", False) else data["q"]/1000,
            "total_kva":data["p"]*data.get("ct", 1)/pf if data.get("ct", False) or not data.get("meterName", False) else (data["p"]/1000)/pf,
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
            "kwh_import":data["wh"]*data.get("ct", 1),
            "kwh_export":0,
            "kvarh_import":data["varh"],
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
        # db_manager.write_to_readings_table(**payload)
        print("DEVICE ID ####################", device_id)
        # print(payload)
        if True: # datalogs_payload["summary_energy_register_1"] == 0:
            print("FAILED CONDITION ####################", device_id)
            print("SUMMARY ENERGY :::::::::", datalogs_payload["summary_energy_register_1"] )
            #return dict()

        response = requests.post("https://backend.wyreng.com/posts/reading/", json=payload)
        print("DEVICE ID ####################", device_id)
        print("RESPONSE : ", response.content)
        response = requests.post("https://backend.wyreng.com/posts/datalog/", json=datalogs_payload)
        print("DATALOG RESPONSE : ", response.content)

    except SyntaxError:
        print("error")
