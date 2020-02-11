import iModules.returns.Src.dbUnit as db
import pandas as pd
import os
import logging
import time
import iModules.returns.Src.computeUnit as cu
import pyfolio as pyf

log = logging.getLogger("rabbit.returns.nonNavPort")
pd.options.mode.chained_assignment = None

dir = os.path.dirname(os.path.abspath(__file__))


# NON_NAV_PORT
def process(date):
    start = time.time()

    # Get Historical data from Mongo
    """ histData = db.getDataFromMongo("rawdatareturns_nonnavport")
    if not histData.empty:
        dailyRaw = db.getDataFromOracle("dailyRawData", date)
        if not dailyRaw.empty:
            histData["ASONDATE"] = pd.to_datetime(histData["ASONDATE"], format="%Y-%m-%d")
            dailyRaw["ASONDATE"] = pd.to_datetime(dailyRaw["ASONDATE"], format="%Y-%m-%d")
            db.insertDataToMongo("rawdatareturns_nonnavport", dailyRaw, date)
        else:
            dailyRaw = histData
    else:
        log.warn("dataNotFound") """
    rawData = db.getDataFromOracle("dailyRawData", date)
    #db.insertDataToMongo("rawdatareturns_nonnavport", rawData, date)

    # Get latest Raw data from MONGO
    if not rawData.empty:
        retBalSheet = rawData#db.getDataFromMongo("rawdatareturns_nonnavport")
    else:
        retBalSheet=pd.DataFrame(columns=["ASONDATE", "PORTCODE", "CATEGORY", "OPERATION", "AMOUNT"])
    print(retBalSheet)

    # Calc and Store closing balance and returns into mongo
    # retBalSheet = groupAsset(retBalSheet)
    retBalSheet = cu.concatCategory(retBalSheet)
    closingBal = cu.calcDailyCloseReturns(retBalSheet).fillna(0)
    unstackClosingBal = closingBal.unstack().reset_index().rename(columns={0: "VALUE", "level_0": "OPERATION", "level_1": "PORTCODE", "level_2": "ASONDATE"})

    # Save Closing Balance sheet into mongo
    #db.insertDataToMongo("dailyclreturns_nonnavport", unstackClosingBal, date)
    #unstackClosingBal.to_csv(os.path.join(dir, "excel/output/closingBalance.csv"))

    # calculate returns for all frequencies from a date received from rabbitmq.
    returnGroups = cu.calcGroupReturns(date, closingBal, ["LOGGRRETURNS", "LOGNETRETURNS"])
    returnGroups["TYPE"] = "PORTFOLIO"
    returnGroups["ASONDATE"] = date
    # need to reset the multilevel index to save inside mongo.
    #db.insertDataToMongo("groupreturns", returnGroups.reset_index(), date)
    db.insertDataToOracle("groupreturns", returnGroups.reset_index(), type="PORTFOLIO")

    # dailyclreturns_nonnavport = closingBal["LOGNETRETURNS"]
    # # dailyclreturns_nonnavport.columns = dailyclreturns_nonnavport.columns.droplevel(0)
    # groupRatio = dailyclreturns_nonnavport.apply(lambda x: pyf.timeseries.perf_stats(x))
    # db.insertDataToMongo("groupratio_nonnavport", groupRatio)

    #returnGroups.to_csv(os.path.join(dir, "excel/output/dateReturns.csv"))

    log.info("nonNavPort event completed  in {} ".format(time.time() - start))
    print("nonNavPort event completed  in {} ".format(time.time() - start))



""" {
    "user": {
        "user_id": "sagarsp",
        "sessionid": "237260429126477122008302166698",
        "app_id": "xyzdas123"
    },
"data":{
           "event":"nonNavPort",
           "date":"2018-01-01"
        }
} """