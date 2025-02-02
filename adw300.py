# data_subset = {
#     data = {
#     0: {'data item name': '设备编号', 'type': 'String', 'unit': '', 'note': '设备唯一序列号 Device Unique Serial Number'},
#     1: {'data item name': 'A相电压', 'type': '电压', 'unit': 'V', 'note': ''},
#     2: {'data item name': 'B相电压', 'type': '电压', 'unit': 'V', 'note': ''},
#     3: {'data item name': 'C相电压', 'type': '电压', 'unit': 'V', 'note': ''},
#     4: {'data item name': 'AB线电压', 'type': '电压', 'unit': 'V', 'note': ''},
#     5: {'data item name': 'BC线电压', 'type': '电压', 'unit': 'V', 'note': ''},
#     6: {'data item name': 'CA线电压', 'type': '电压', 'unit': 'V', 'note': ''},
#     7: {'data item name': 'A相电流', 'type': '电流', 'unit': 'A', 'note': ''},
#     8: {'data item name': 'B相电流', 'type': '电流', 'unit': 'A', 'note': ''},
#     9: {'data item name': 'C相电流', 'type': '电流', 'unit': 'A', 'note': ''},
#     10: {'data item name': 'A相有功功率', 'type': '功率', 'unit': 'kW', 'note': ''},
#     11: {'data item name': 'B相有功功率', 'type': '功率', 'unit': 'kW', 'note': ''},
#     12: {'data item name': 'C相有功功率', 'type': '功率', 'unit': 'kW', 'note': ''},
#     13: {'data item name': '总有功功率', 'type': '功率', 'unit': 'kW', 'note': ''},
#     14: {'data item name': 'A相无功功率', 'type': '功率', 'unit': 'kvar', 'note': ''},
#     15: {'data item name': 'B相无功功率', 'type': '功率', 'unit': 'kvar', 'note': ''},
#     16: {'data item name': 'C相无功功率', 'type': '功率', 'unit': 'kvar', 'note': ''},
#     17: {'data item name': '总无功功率', 'type': '功率', 'unit': 'kvar', 'note': ''},
#     18: {'data item name': 'A相视在功率', 'type': '功率', 'unit': 'kVA', 'note': ''},
#     19: {'data item name': 'B相视在功率', 'type': '功率', 'unit': 'kVA', 'note': ''},
#     20: {'data item name': 'C相视在功率', 'type': '功率', 'unit': 'kVA', 'note': ''},
#     21: {'data item name': '总视在功率', 'type': '功率', 'unit': 'kVA', 'note': ''},
#     22: {'data item name': 'A相功率因数', 'type': '功率因数', 'unit': '', 'note': ''},
#     23: {'data item name': 'B相功率因数', 'type': '功率因数', 'unit': '', 'note': ''},
#     24: {'data item name': 'C相功率因数', 'type': '功率因数', 'unit': '', 'note': ''},
#     25: {'data item name': '总功率因数', 'type': '功率因数', 'unit': '', 'note': ''},
#     26: {'data item name': '频率', 'type': '频率', 'unit': 'Hz', 'note': ''},
#     27: {'data item name': '信号强度', 'type': '工况', 'unit': 'dBm', 'note': 'working condition'},
#     28: {'data item name': '正向有功总电能示值', 'type': '电能示值', 'unit': 'kWh', 'note': 'Energy indication value'},
#     29: {'data item name': '反向有功总电能示值', 'type': '电能示值', 'unit': 'kWh', 'note': 'Energy indication value'},
#     30: {'data item name': '正向无功总电能示值', 'type': '电能示值', 'unit': 'kvarh', 'note': 'Energy indication value'},
#     31: {'data item name': '反向无功总电能示值', 'type': '电能示值', 'unit': 'kvarh', 'note': 'Energy indication value'},
#     32: {'data item name': '当前有功需量', 'type': '需量', 'unit': 'kW', 'note': 'demand'},
#     33: {'data item name': 'ICCID', 'type': '工况', 'unit': '', 'note': '强制开机上送且只上送一次 Force upload and upload only once'},
#     34: {'data item name': 'PT', 'type': '电压变比', 'unit': '', 'note': 'Voltage ratio'},
#     35: {'data item name': 'CT', 'type': '电流', 'unit': '', 'note': ''},
#     36: {'data item name': 'A相温度', 'type': '', 'unit': '℃', 'note': ''},
#     37: {'data item name': 'B相温度', 'type': '', 'unit': '℃', 'note': ''},
#     38: {'data item name': 'C相温度', 'type': '', 'unit': '℃', 'note': ''},
#     39: {'data item name': 'N相温度', 'type': '', 'unit': '℃', 'note': ''},
#     40: {'data item name': '剩余电流', 'type': 'Leakage current', 'unit': 'A', 'note': ''},
#     41: {'data item name': '4路DI状态', 'type': 'DI状态', 'unit': '', 'note': 'bit0:DI1 bit1:DI2 bit2:DI3 bit3:DI4'},
#     42: {'data item name': 'A相正向有功总电能示值', 'type': '电能示值', 'unit': 'Kwh', 'note': 'Indication value of total positive active energy in phase A'},
#     43: {'data item name': 'A相反向有功总电能示值', 'type': '电能示值', 'unit': 'Kwh', 'note': ''},
#     44: {'data item name': 'A相正向无功总电能示值', 'type': '电能示值', 'unit': 'kvarh', 'note': ''},
#     45: {'data item name': 'A相反向无功总电能示值', 'type': '电能示值', 'unit': 'kvarh', 'note': ''},
#     46: {'data item name': 'B相正向有功总电能示值', 'type': '电能示值', 'unit': 'Kwh', 'note': ''},
#     47: {'data item name': 'B相反向有功总电能示值', 'type': '电能示值', 'unit': 'Kwh', 'note': ''},
#     48: {'data item name': 'B相正向无功总电能示值', 'type': '电能示值', 'unit': 'kvarh', 'note': ''},
#     49: {'data item name': 'B相反向无功总电能示值', 'type': '电能示值', 'unit': 'kvarh', 'note': ''},
#     50: {'data item name': 'C相正向有功总电能示值', 'type': '电能示值', 'unit': 'Kwh', 'note': 'Indication value of total positive active energy in phase C'},
#     51: {'data item name': 'C相反向有功总电能示值', 'type': '电能示值', 'unit': 'Kwh', 'note': ''},
#     52: {'data item name': 'C相正向无功总电能示值', 'type': '电能示值', 'unit': 'kvarh', 'note': 'Indication value of total positive reactive energy in phase B'},
#     53: {'data item name': 'C相反向无功总电能示值', 'type': '电能示值', 'unit': 'kvarh', 'note': ''},
#     54: {'data item name': 'A相谐波电压总畸变率', 'type': '谐波含量', 'unit': '%', 'note': 'Harmonic content'},
#     55: {'data item name': 'B相谐波电压总畸变率', 'type': '谐波含量', 'unit': '%', 'note': 'Harmonic content'},
#     56: {'data item name': 'C相谐波电压总畸变率', 'type': '谐波含量', 'unit': '%', 'note': 'Harmonic content'},
#     57: {'data item name': 'A相谐波电流总畸变率', 'type': '谐波含量', 'unit': '%', 'note': 'Harmonic content'},
#     58: {'data item name': 'B相谐波电流总畸变率', 'type': '谐波含量', 'unit': '%', 'note': 'Harmonic content'},
#     59: {'data item name': 'C相谐波电流总畸变率', 'type': '谐波含量', 'unit': '%', 'note': 'Harmonic content'},
#     60: {'data item name': '当前反向有功需量', 'type': '', 'unit': 'kw', 'note': ''},
#     61: {'data item name': '当前正向无功需量', 'type': '', 'unit': 'kvar', 'note': ''},
#     62: {'data item name': '当前反向无功需量', 'type': '', 'unit': 'kvar', 'note': ''},
#     63: {'data item name': '电压不平衡度', 'type': '', 'unit': '%', 'note': ''},
#     64: {'data item name': '电流不平衡度', 'type': '', 'unit': '%', 'note': ''},
#     65: {'data item name': '尖时段有功总电能', 'type': '', 'unit': 'Kwh', 'note': ''},
#     66: {'data item name': '峰时段有功总电能', 'type': '', 'unit': 'Kwh', 'note': ''},
#     67: {'data item name': '平时段有功总电能', 'type': '', 'unit': 'Kwh', 'note': ''},
#     68: {'data item name': '谷时段有功总电能', 'type': '', 'unit': 'Kwh', 'note': ''},
#     69: {'data item name': '第一象限无功电能', 'type': '', 'unit': 'kvarh', 'note': ''},
#     70: {'data item name': '第二象限无功电能', 'type': '', 'unit': 'kvarh', 'note': ''},
#     71: {'data item name': '第三象限无功电能', 'type': '', 'unit': 'kvarh', 'note': ''},
#     72: {'data item name': '第四象限无功电能', 'type': '', 'unit': 'kvarh', 'note': ''}
# }

def find_params(data, param_id):

    target = param_id
    for obj in data:
        if obj.get("id", 0) == target:
            return (obj["val"])

    else:
        return (0)

data = {"data":[{"tp":1736871132698,"point":[{"id":0,"val":"24102809510006"},{"id":1,"val":0},{"id":2,"val":211.4},{"id":3,"val":0},{"id":4,"val":211.4},{"id":5,"val":211.4},{"id":6,"val":0},{"id":7,"val":0},{"id":8,"val":0},{"id":9,"val":0},{"id":10,"val":0},{"id":11,"val":0},{"id":12,"val":0},{"id":13,"val":0},{"id":14,"val":0},{"id":15,"val":0},{"id":16,"val":0},{"id":17,"val":0},{"id":18,"val":0},{"id":19,"val":0},{"id":20,"val":0},{"id":21,"val":0},{"id":22,"val":1},{"id":23,"val":1},{"id":24,"val":1},{"id":25,"val":1},{"id":26,"val":50.23},{"id":27,"val":14},{"id":28,"val":0},{"id":29,"val":0},{"id":30,"val":0},{"id":31,"val":0},{"id":32,"val":0},{"id":33,"val":"8923420037880998454F"},{"id":34,"val":1},{"id":35,"val":2}]}]}

[{"id":0,"val":"24102809510006"},{"id":1,"val":0},{"id":2,"val":211.4},{"id":3,"val":0},{"id":4,"val":211.4},{"id":5,"val":211.4},{"id":6,"val":0},{"id":7,"val":0},{"id":8,"val":0},{"id":9,"val":0},{"id":10,"val":0},{"id":11,"val":0},{"id":12,"val":0},{"id":13,"val":0},{"id":14,"val":0},{"id":15,"val":0},{"id":16,"val":0},{"id":17,"val":0},{"id":18,"val":0},{"id":19,"val":0},{"id":20,"val":0},{"id":21,"val":0},{"id":22,"val":1},{"id":23,"val":1},{"id":24,"val":1},{"id":25,"val":1},{"id":26,"val":50.23},{"id":27,"val":14},{"id":28,"val":0},{"id":29,"val":0},{"id":30,"val":0},{"id":31,"val":0},{"id":32,"val":0},{"id":33,"val":"8923420037880998454F"},{"id":34,"val":1},{"id":35,"val":2}]


data = data["data"][0]["point"]

x = {
   "type":"data",
   "time":"20250116100500",
   "gwSN":"12212073190010",
   "meterSN":"12212073190091",
   "meterName":"ADL400",
   "meterStatus":"normal",
   "ch":0,
   "ua":find_params(data,1),
   "ub":find_params(data,2),
   "uc":find_params(data,3),
   "ia":find_params(data,7),
   "ib":find_params(data,8),
   "ic":find_params(data,9),
   "pa":find_params(data,10),
   "pb":find_params(data,11),
   "pc":find_params(data,12),
   "p":find_params(data,13),
   "qa":find_params(data,14),
   "qb":find_params(data,15),
   "qc":find_params(data,16),
   "q":find_params(data,17),
   "pfa":find_params(data,22),
   "pfb":find_params(data,23),
   "pfc":find_params(data,24),
   "pf":find_params(data,25),
   "pt":find_params(data,34),
   "ct":find_params(data,35),
   "wh":find_params(data,42),
   "wl":0,
   "varh":find_params(data,44),
   "varl":47.28,
   "hz":find_params(data,26)
}

# print(data["data"][0]["point"])


print(x)
# print(find_params(data["data"][0]["point"], 2))

# AWT200

{"msgid":"571","method":"update","sn":"24120405680002","timestamp":1738468200,"sendtime":1738468200,"version":1,"reported":{"0_24120405680027":{"state":"ONLINE","EP":"21.60","EPI":"21.60","EPIJ":"0.00","EPIF":"0.00","EPIP":"21.60","EPIG":"0.00","EPE":"0.00","EQ":"1.60","EQL":"1.60","EQC":"0.00","Ua":"226.0","Ub":"0.0","Uc":"228.1","Ia":"0.0","Ib":"0.0","Ic":"0.0","Pa":"0.0","Pb":"0.0","Pc":"0.0","P":"0.0","Qa":"0.0","Qb":"0.0","Qc":"0.0","Q":"0.0","PFa":"0.0","PFb":"0.0","PFc":"0.0","PF":"0.0","Fr":"50.1","Uab":"226.0","Ubc":"228.1","Uac":"393.2","Uub":"150.6","Iub":"0.0"}}}