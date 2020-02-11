# import iModules.returns.Src.returns as returns
import logging
import time
import importlib
import sys
import json
import os.path
import logging.handlers
from datetime import datetime


handler = logging.handlers.TimedRotatingFileHandler(os.path.join(os.path.dirname(__file__),"logs/Results--"+datetime.now().strftime('%Y-%m-%d')+".log"), when="midnight", interval=1, backupCount=3)
handler.setFormatter(logging.Formatter('%(asctime)s::%(name)s::%(levelname)s:: %(lineno)d::%(message)s'))
log = logging.getLogger("processunit")
log.addHandler(handler)
log.setLevel(logging.DEBUG)

log = logging.getLogger("processunit.returns")
"""
### RabbitMq Message templete:
    "data":{
        "event":"nonNavPort",
        "date":"2018-01-01"
    }
"""
json_message=json.loads(sys.argv[1])
#json_message={"event":"portsec","date":"2019-12-04"}

def callEvent(event, date):
    log.info("Calling {} event.".format(event))
    print("Src.{}".format(event))
    module = importlib.import_module("Src.{}".format(event))
    module.process(date)


def process(message):
    #message={"event":"portsec","date":"2018-01-01"}
    log.info("+---------------------------------ReturnsProcessUnit-----------------------------------------+")

    start = time.time()
    log.info("Reading message. {}".format(message))
    if message['event'] == "calculate_all_returns":
        for i in ["securities", "benchmark", "navPort",  "portsec"]:
            callEvent(i, message["date"])
    else:
        callEvent(message["event"], message["date"])
    log.info("Time Consumed: {}".format(time.time() - start))


process(json_message);


""" {
    "eventname": "returns",
    "user": {
        "user_id": "sagarsp",
        "sessionid": "237260429126477122008302166698",
        "app_id": "xyzdas123"
    },
    "data": {
        "event": "calculate_all_returns",
        "date": "2019-24-12"
    }
} """