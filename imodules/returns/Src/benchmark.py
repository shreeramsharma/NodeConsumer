# from pymongo import MongoClient
import numpy as np
import pandas as pd
import Src.dbUnit as db
import Src.computeUnit as cu
import Src.computeUnit as pcu
import logging
import pyfolio as pyf
import os.path
import time
from datetime import datetime

pd.options.mode.chained_assignment = None
log = logging.getLogger("processunit.returns.benchmark")


def process(date):
    start = time.time()
    mydir = os.path.join("./imodules/returns/Working_Sheet/Benchmark_Returns_Working_Sheet/", datetime.now().strftime('%Y-%m-%d_%H-%M-%S'))
    os.makedirs(mydir)
    histData=pd.DataFrame()
    if_file_exist=os.path.exists("./imodules/returns/Src/excel/input/priceData/Historical_benchmark.csv")
    if(if_file_exist==True):
        histData = pd.read_csv("./imodules/returns/Src/excel/input/priceData/Historical_benchmark.csv")
    if not histData.empty:
        #log.info("notEmpty")
        # drop a day records
        #histData = histData.set_index("ASONDATE").stack().reset_index().rename(columns={0: "VALUE", "level_1": "BENCHMARK"})
        #histData = histData[histData["ASONDATE"] != date]
        # get daily record
        dailyData = db.getDataFromOracle("trindex_daily", date)
        if not dailyData.empty:
            rawBench = histData.append(dailyData, ignore_index=True)
        else:
            rawBench = histData
    else:
        #log.info("Empty")
        histData = db.getDataFromOracle("trindex_all")
        histData.to_csv("./imodules/returns/Src/excel/input/priceData/Historical_benchmark.csv")
        # Save all historical data into Mongo
        rawBench = histData
    rawBench.BENCHMARK = rawBench.BENCHMARK.astype(str)
    rawBench = rawBench.pivot_table(index="ASONDATE", columns="BENCHMARK", values="VALUE").reset_index()
    rawBench.to_csv(""+mydir+"/Raw_data_Benchmark_1.csv")
    #db.insertDataToMongo("rawdatareturns_benchmark", rawBench)
    rawBench = rawBench.set_index("ASONDATE").stack().reset_index().rename(columns={0: "VALUE"})

    allDateRawBench = pd.DataFrame(index=pd.date_range(rawBench["ASONDATE"].min(), rawBench["ASONDATE"].max()))
    pivotRawBench = rawBench.pivot_table(index="ASONDATE", columns="BENCHMARK", values="VALUE")

    rawPrices = allDateRawBench.merge(pivotRawBench, left_index=True, right_index=True, how="left").fillna(method="ffill")
    dividend = pd.DataFrame(index=rawPrices.index, columns=rawPrices.columns, data=0)
    splitRatio = pd.DataFrame(index=rawPrices.index, columns=rawPrices.columns, data=1)

    adjPrice = cu.calcAdjustedPrices(rawPrices, dividend, splitRatio).sort_index()
    adjPrice.to_csv(""+mydir+"/Adjusted_Price_2.csv")
    #db.insertDataToMongo("dailyadjclose_benchmark", adjPrice.reset_index())

    logReturns = np.log10(adjPrice.pct_change() + 1).replace([np.inf, -np.inf], np.nan).fillna(0)
    logReturns.to_csv(""+mydir+"/logReturns_3.csv")
    #db.insertDataToMongo("dailyadjclose_benchmark", logReturns.reset_index())

    pcu.calcBucketReturns(logReturns, "BENCHMARK")

    adjPrice = adjPrice.stack().reset_index().rename(columns={"level_0": "ASONDATE", "level_1": "BENCHMARK", 0: "ADJPRICES"})
    logReturns = logReturns.stack().reset_index().rename(columns={"level_0": "ASONDATE", "level_1": "BENCHMARK", 0: "LOGRETURNS"})

    dailyclreturns_benchmark = adjPrice.merge(logReturns, on=["ASONDATE", "BENCHMARK"])
    dailyclreturns_benchmark = dailyclreturns_benchmark.set_index(["ASONDATE", "BENCHMARK"]).stack().reset_index().rename(columns={"level_2": "OPERATION", 0: "VALUE"})
    #dailyclreturns_benchmark=pd.DataFrame(dailyclreturns_benchmark).reset_index(drop = True)
    dailyclreturns_benchmark.to_csv(""+mydir+"/Daily_Calculated_returns_benchmark_4.csv")

    #db.insertDataToMongo("dailyclreturns_benchmark", dailyclreturns_benchmark)

    dailyclreturns_benchmark = dailyclreturns_benchmark.pivot_table(index="ASONDATE", columns=["OPERATION", "BENCHMARK"])
    dailyclreturns_benchmark = dailyclreturns_benchmark.iloc[:, dailyclreturns_benchmark.columns.get_level_values(1) == "LOGRETURNS"].fillna(0)

    dailyclreturns_benchmark.columns = dailyclreturns_benchmark.columns.droplevel(0)

    groupReturns = cu.calcGroupReturns(date, dailyclreturns_benchmark, ["LOGRETURNS"])
    groupReturns["TYPE"] = "BENCHMARK"
    groupReturns["ASONDATE"] = date
    groupReturns.to_csv(""+mydir+"/GroupReturns_5.csv")

    db.insertDataToOracle("groupreturns", groupReturns.reset_index(), type="BENCHMARK")
    groupReturns=groupReturns.reset_index()
    groupReturns.to_csv(""+mydir+"/GroupReturns_51.csv")
    groupReturns=groupReturns.rename(columns={"level_0":"return_type","level_1":"instrument_code","TYPE":"instrument_type"})
    groupReturns.to_csv(""+mydir+"/GroupReturns_52.csv")
    groupReturns=pd.melt(groupReturns, id_vars=["instrument_code","return_type","ASONDATE","instrument_type"],var_name="frequency")
    groupReturns.to_csv(""+mydir+"/GroupReturns_53.csv")

    db.insertDataToOracle("iwf_return", groupReturns.reset_index(), type="BENCHMARK")

    #db.insertDataToMongo("groupreturns", groupReturns.reset_index(), date)
    #db.insertDataToOracle("groupreturns", groupReturns.reset_index(), type="BENCHMARK")

    dailyclreturns_benchmark.columns = dailyclreturns_benchmark.columns.droplevel(0)
    dailyclreturns_benchmark.to_csv(""+mydir+"/Daily_Calculated_returns_benchmark_6.csv")
    groupRatio = dailyclreturns_benchmark.apply(lambda x: pyf.timeseries.perf_stats(x))
    groupRatio.to_csv(""+mydir+"/GroupRatio_7.csv")
    #db.insertDataToMongo("groupratio_benchmark", groupRatio)

    #log.info("benchmark event completed  in {} ".format(time.time() - start))
    print("benchmark event completed  in {} ".format(time.time() - start))


""" {
    "eventname": "returns",
    "user": {
        "user_id": "sagarsp",
        "sessionid": "237260429126477122008302166698",
        "app_id": "xyzdas123"
    },
    "data": {
        "event": "benchmark",
        "date": "2020-01-10"
    }
} """