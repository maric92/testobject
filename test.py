import json
from datetime import datetime
from urllib.request import urlopen
from jsonschema import validate


main_api =  "http://snap.datastream.center/techquest"
class openning:
    def validates():
        with open("input-2017-02-01.json", "r") as json_data:
            schema = {
                "type": "object",
                "properties": {
                    "user": {
                        "type": "number"
                    },
                # валидация на интервал чисел и количество знаков после точки
                    "ts": {"type": "real(4)",
                            "minimum": 146000000,
                            "maximum": 148000000
                            },
                # валидация словаря на значение полей
                     "context": {
                         "type": "object",
                          "num": ["hard"(0,100), "soft"(0,1000), "level"(0,20)]
                }, #проверка на формат данных
                "ip": {"type": "string",
                       }
            }
        }
    validate(json_data, schema)

class Conversion:
    def __init__(self, user,time, context, ip):
        self.user = user
        self.time = time
        self.context = context
        self.ip = ip

    while True:
        data = json.load(json_data)

     # конвертация timestamp в datetime
        time = 'ts'
        dt_object = datetime.fromtimestamp(time)

     #конвертация object в словарь
        context = 'context'
        con = json.loads(context)

        user = ['user']
        us = json.loads(user)
     #конвертация json-string в строку
        ip = ['ip']
        address = json.loads(ip)

        data = [us, dt_object, con, address]
        json.dump(data, open('report_input', 'w'))
        break

#загрузка в базу
class DBreport:
    def __init__(self):
        self.conn = sqlite3.connect('report_input.db')
        self.c = self.conn.cursor()
        self.c.execute(
            '''CREATE TABLE IF NOT EXISTS report_input (us integer primary key, dt_object datetime, con text, address text)''')
        self.conn.commit()

    def insert_data(self, dt_object, con, address):
        self.c.execute('''INSERT INTO finance(us, dt_object datetime, con, address) VALUES (?, ?, ?, ?)''',
                       (us, dt_object, con, address))
        self.conn.commit()
        db = DBreport()


class DBerror:
    def __init__(self):
        self.conn = sqlite3.connect('data_error.db')
        self.c = self.conn.cursor()
        self.c.execute(
            '''CREATE TABLE IF NOT EXISTS data_error (api_report text primary key, api_date datetime, row_text text, error_text text,
             ins_ts datetime)''')
        self.conn.commit()

    def insert_data(self, api_date, row_text, error_text, ins_ts):
        self.c.execute('''INSERT INTO data_error(api_report, api_date, row_text, error_text, ins_ts) VALUES (?, ?, ?, ?, ?)''',
                       (api_report, api_date, row_text, error_text, ins_ts))
        self.conn.commit()


