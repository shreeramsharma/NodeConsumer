import numpy as np
import pandas as pd
import Src.dbUnit as db
import Src.computeUnit as cu
import logging
import pyfolio as pyf
import time
import os.path
from datetime import datetime

pd.options.mode.chained_assignment = None
log = logging.getLogger("processunit.returns.navPort")


def process(date):
    start = time.time()
    mydir = os.path.join("./imodules/returns/Working_Sheet/NavPort_Returns_Working_Sheet/", datetime.now().strftime('%Y-%m-%d_%H-%M-%S'))
    os.makedirs(mydir)
    histData=pd.DataFrame()
    if_file_exist=os.path.exists("./imodules/returns/Src/excel/input/priceData/Historical_navport.csv")
    if(if_file_exist==True):
        histData = pd.read_csv("./imodules/returns/Src/excel/input/priceData/Historical_navport.csv")
    # histData = db.getDataFromMongo("rawdatareturns_navport")
    if not histData.empty:
        #log.info("Raw_Nav_Port not Empty")
        # drop a day records
        # histData = histData.set_index("ASONDATE").stack().reset_index().rename(columns={0: "VALUE", "level_1": "PORTCODE"})
        # histData = histData[histData["ASONDATE"] != date]
        # get daily record
        dailyData = db.getDataFromOracle("nav_daily", date)
        if not dailyData.empty:
            rawBench = histData.append(dailyData, ignore_index=True)
        else:
            rawBench = histData
    else:
        #log.info("Raw_Nav_Port Empty")
        histData = db.getDataFromOracle("nav_all")
        histData.to_csv("./imodules/returns/Src/excel/input/priceData/Historical_navport.csv")
        # Save all historical data into Mongo
        rawBench = histData
    rawBench.PORTCODE = rawBench.PORTCODE.astype(str)
    rawBench['ASONDATE'] = pd.to_datetime(rawBench['ASONDATE'],dayfirst=True)
    rawBench = rawBench.pivot_table(index="ASONDATE", columns="PORTCODE", values="VALUE").reset_index()
    rawBench.to_csv(""+mydir+"/Raw_data_NavPort_1.csv")
    #db.insertDataToMongo("rawdatareturns_navport", rawBench)
    rawBench = rawBench.set_index("ASONDATE").stack().reset_index().rename(columns={0: "VALUE"})
    allDateRawBench = pd.DataFrame(index=pd.date_range(rawBench["ASONDATE"].min(), rawBench["ASONDATE"].max()))
    pivotRawBench = rawBench.pivot_table(index="ASONDATE", columns="PORTCODE", values="VALUE")

    rawPrices = allDateRawBench.merge(pivotRawBench, left_index=True, right_index=True, how="left").fillna(method="ffill")
    dividend = pd.DataFrame(index=rawPrices.index, columns=rawPrices.columns, data=0)
    splitRatio = pd.DataFrame(index=rawPrices.index, columns=rawPrices.columns, data=1)

    adjPrice = cu.calcAdjustedPrices(rawPrices, dividend, splitRatio).sort_index()
    adjPrice.to_csv(""+mydir+"/Daily_Adjusted_ClosePrice_2.csv")
    # db.insertDataToMongo("dailyadjclose_navport", adjPrice.reset_index())

    logReturns = np.log10(adjPrice.pct_change() + 1).replace([np.inf, -np.inf], np.nan).fillna(0)
    logReturns.to_csv(""+mydir+"/Daily_Log_Returns_3.csv")
    #db.insertDataToMongo("dailyreturns_navport", logReturns.reset_index())

    adjPrice = adjPrice.stack().reset_index().rename(columns={"level_0": "ASONDATE", "level_1": "PORTCODE", 0: "ADJPRICES"})
    logReturns = logReturns.stack().reset_index().rename(columns={"level_0": "ASONDATE", "level_1": "PORTCODE", 0: "LOGRETURNS"})

    dailyclreturns_navport = adjPrice.merge(logReturns, on=["ASONDATE", "PORTCODE"])
    dailyclreturns_navport = dailyclreturns_navport.set_index(["ASONDATE", "PORTCODE"]).stack().reset_index().rename(columns={"level_2": "OPERATION", 0: "VALUE"})

    # db.insertDataToMongo("dailyclreturns_navport", dailyclreturns_navport)

    dailyclreturns_navport = dailyclreturns_navport.pivot_table(index="ASONDATE", columns=["OPERATION", "PORTCODE"])
    dailyclreturns_navport = dailyclreturns_navport.iloc[:, dailyclreturns_navport.columns.get_level_values(1) == "LOGRETURNS"].fillna(0)

    dailyclreturns_navport.columns = dailyclreturns_navport.columns.droplevel(0)

    groupReturns = cu.calcGroupReturns(date, dailyclreturns_navport, ["LOGRETURNS"])
    groupReturns["TYPE"] = "PORTFOLIO"
    groupReturns["ASONDATE"] = date
    groupReturns.to_csv(""+mydir+"/Group_Returns_4.csv")

    db.insertDataToOracle("groupreturns", groupReturns.reset_index(), type="PORTFOLIO")
    groupReturns=groupReturns.reset_index()
    groupReturns.to_csv(""+mydir+"/GroupReturns_41.csv")
    groupReturns=groupReturns.rename(columns={"level_0":"return_type","level_1":"instrument_code","TYPE":"instrument_type"})
    groupReturns.to_csv(""+mydir+"/GroupReturns_42.csv")
    groupReturns=pd.melt(groupReturns, id_vars=["instrument_code","return_type","ASONDATE","instrument_type"],var_name="frequency")
    groupReturns.to_csv(""+mydir+"/GroupReturns_43.csv")

    db.insertDataToOracle("if_return_data", groupReturns.reset_index(), type="PORTFOLIO")
    db.insertDataToOracle("iwf_return", groupReturns.reset_index(), type="PORTFOLIO")
    # db.insertDataToMongo("groupreturns", groupReturns.reset_index(), date)
    # db.insertDataToOracle("groupreturns", groupReturns.reset_index(), type="PORTFOLIO")

    dailyclreturns_navport.columns = dailyclreturns_navport.columns.droplevel(0)
    groupRatio = dailyclreturns_navport.apply(lambda x: pyf.timeseries.perf_stats(x))
    groupRatio.to_csv(""+mydir+"/Group_Ratio_5.csv")
    # db.insertDataToMongo("groupratio_navport", groupRatio)

    #log.info("navPort event completed  in {} ".format(time.time() - start))
    print("navPort event completed  in {} ".format(time.time() - start))


""" {
    "eventname": "returns",
    "user": {
        "user_id": "sagarsp",
        "sessionid": "237260429126477122008302166698",
        "app_id": "xyzdas123"
    },
    "data": {
        "event": "navPort",
        "date": "2020-01-14"
    }
} """