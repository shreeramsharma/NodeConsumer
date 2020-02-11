# from pymongo import MongoClient
import Src.dbUnit as db
import Src.computeUnit as cu
import numpy as np
import pandas as pd
import logging
import pyfolio as pyf
import time
import os.path
import errno
from datetime import datetime

log = logging.getLogger("processunit.returns.securities")
pd.options.mode.chained_assignment = None




def process(Date):
    start = time.time()
    mydir = os.path.join("./imodules/returns/Working_Sheet/Securities_Returns_Working_Sheet/", datetime.now().strftime('%Y-%m-%d_%H-%M-%S'))
    os.makedirs(mydir)
    dailyData = db.getDataFromOracle("sec_ca_daily", Date)
    if not dailyData.empty:
        dailyData = dailyData.fillna({"BC_TO_DATE": dailyData["BSE_NSE_EX_DATE"]})
        dailyData.fillna(0, inplace=True)
        dailyData["BC_TO_DATE"] = pd.to_datetime(dailyData["BC_TO_DATE"], format="%d/%m/%Y")
        df_sec_ca_daily=pd.DataFrame(dailyData,columns=['ISIN','BC_TO_DATE','OLD_RATIO','NEW_RATIO','DIVIDEND_RATE','BSE_NSE_EX_DATE'])
        #print("Daily Data: ",dailyData)
    else:
        #log.info("Empty")
        df_sec_ca_daily=pd.DataFrame(dailyData,columns=['ISIN','BC_TO_DATE','OLD_RATIO','NEW_RATIO','DIVIDEND_RATE','BSE_NSE_EX_DATE'])
    #print("df_sec_ca_daily ",df_sec_ca_daily)
    # 2 Suppose the file of Historical_CA.csv does not exist then 
    # query the sec_ca_all and get the data and write to Historical_CA.csv file
    if_file_exist=os.path.exists("./imodules/returns/Src/excel/input/priceData/Historical_CA.csv")
    if(if_file_exist==True):
        caData = pd.read_csv("./imodules/returns/Src/excel/input/priceData/Historical_CA.csv")
    else:
        #log.info("Historical_CA.csv Doesn't Exist in ./imodules/returns/Src/excel/input/priceData/Historical_CA.csv")
        caData=db.getDataFromOracle("sec_ca_all", Date)
        caData.to_csv("./imodules/returns/Src/excel/input/priceData/Historical_CA.csv")
        caData = pd.read_csv("./imodules/returns/Src/excel/input/priceData/Historical_CA.csv")
    #print("caData ",caData)
    caData = caData.fillna({"BC_TO_DATE": caData["BSE_NSE_EX_DATE"]})
    caData.fillna(0, inplace=True)
    caData = caData[caData["BC_TO_DATE"] != 0]
    caData["BC_TO_DATE"] = pd.to_datetime(caData["BC_TO_DATE"],format="%d/%m/%Y")
    caData.to_csv(""+mydir+"/Corporate_Action_Data_1.csv")
    #db.insertDataToMongo("rawdatacadata_securities", caData)
    df_caData=caData[['ISIN','BC_TO_DATE','OLD_RATIO','NEW_RATIO','DIVIDEND_RATE','BSE_NSE_EX_DATE']]
    dfcaData=pd.concat([df_sec_ca_daily,df_caData])
#################################################################

    dailyData = db.getDataFromOracle("sec_price_daily", Date)
    if dailyData.empty:
        #log.info("dailyData Dataframe is Empty")
        dailyData=pd.DataFrame(columns=['ISIN','SECURITYSYMBOL','MTMPRICE','MTMDATE'])
        # make a empty column dataframe
    # 2 Suppose the file of Historical_Price_Data.csv does not exist then 
    # query the sec_price_all and get the data and write to Historical_Price_Data.csv file
    if_file_exist=os.path.exists("./imodules/returns/Src/excel/input/priceData/Historical_Price_Data.csv")
    if(if_file_exist==True):
        price = pd.read_csv("./imodules/returns/Src/excel/input/priceData/Historical_Price_Data.csv")
    else:
        #log.info("Historical_Price_Data.csv Doesn't Exist in ./imodules/returns/Src/excel/input/priceData/Historical_Price_Data.csv")
        price=db.getDataFromOracle("sec_price_all", Date)
        price.to_csv("Historical_Price_Data.csv Doesn't Exist in D:/My Projects/iwf-node-consumer/imodules/returns/Src/excel/input/priceData/Historical_Price_Data.csv")
        price = pd.read_csv("./imodules/returns/Src/excel/input/priceData/Historical_Price_Data.csv")
    # 1 concat the daily price dataframe with the historical prices
    price=pd.concat([dailyData,price])
    price[price["ISIN"] != 0]
    price["MTMPRICE"] = price["MTMPRICE"].astype(float)
    price = price.pivot_table(index="MTMDATE", columns="ISIN", values="MTMPRICE").reset_index()
    price.to_csv(""+mydir+"/Historical_price_Data_2.csv")
    #db.insertDataToMongo("rawdatareturns_securities", price)
    price = price.set_index("MTMDATE").stack().reset_index().rename(columns={0: "MTMPRICE", "level_1": "ISIN"})
    rawPrices, dividend, splitRatio = cu.xformation(dfcaData, price)
    #print("{}  {}  {} ".format(rawPrices,dividend,splitRatio))
    adjPrice = cu.calcAdjustedPrices(rawPrices, dividend, splitRatio).sort_index()
    adjPrice.to_csv(""+mydir+"/Adjusted_Price_3.csv")
    #db.insertDataToMongo("dailyadjclose_securities", adjPrice.reset_index())

    logReturns = np.log10(adjPrice.pct_change() + 1).replace([np.inf, -np.inf], np.nan).fillna(0)
    logReturns.to_csv(""+mydir+"/LogReturns_4.csv")
    #Mongo
    #db.insertDataToMongo("dailyreturns_securities", logReturns.reset_index())

    adjPrice = adjPrice.stack().reset_index().rename(columns={"Date": "ASONDATE", "ISIN": "SECURITY", 0: "ADJPRICES"})
    logReturns = logReturns.stack().reset_index().rename(columns={"Date": "ASONDATE", "ISIN": "SECURITY", 0: "LOGRETURNS"})

    dailyclreturns_securities = adjPrice.merge(logReturns, on=["ASONDATE", "SECURITY"])
    dailyclreturns_securities = dailyclreturns_securities.set_index(["ASONDATE", "SECURITY"]).stack().reset_index().rename(columns={"level_2": "OPERATION", 0: "VALUE"})
    #log.info(dailyclreturns_securities)
    dailyclreturns_securities.to_csv(""+mydir+"/Daily_returns_Calcuated_5.csv")
    #Mongo
    #db.insertDataToMongo("dailyclreturns_securities", dailyclreturns_securities)

    dailyclreturns_securities = dailyclreturns_securities.pivot_table(index="ASONDATE", columns=["OPERATION", "SECURITY"])
    dailyclreturns_securities = dailyclreturns_securities.iloc[:, dailyclreturns_securities.columns.get_level_values(1) == "LOGRETURNS"].fillna(0)
    dailyclreturns_securities.columns = dailyclreturns_securities.columns.droplevel(0)
    #print("dailyclreturns_securities ",dailyclreturns_securities)
    dailyclreturns_securities.to_csv(""+mydir+"/Daily_returns_Calcuated_6.csv")
    groupReturns = cu.calcGroupReturns(Date, dailyclreturns_securities, ["LOGRETURNS"])
    groupReturns["TYPE"] = "SECURITIES"
    groupReturns["ASONDATE"] = Date

    groupReturns.to_csv(""+mydir+"/GroupReturns_7.csv")
    db.insertDataToOracle("groupreturns", groupReturns.reset_index(), type="SECURITIES")
    groupReturns=groupReturns.reset_index()
    groupReturns=groupReturns.rename(columns={"level_0":"return_type","level_1":"instrument_code","TYPE":"instrument_type"})
    groupReturns=pd.melt(groupReturns, id_vars=["instrument_code","return_type","ASONDATE","instrument_type"],var_name="frequency")
    groupReturns.to_csv(""+mydir+"/GroupReturns_71.csv")

    db.insertDataToOracle("iwf_return", groupReturns.reset_index(), type="SECURITIES")
    #Mongo
    #db.insertDataToMongo("groupreturns", groupReturns.reset_index(), Date)

    dailyclreturns_securities.columns = dailyclreturns_securities.columns.droplevel(0)
    #print("dailyclreturns_securities ",dailyclreturns_securities)
    groupRatio = dailyclreturns_securities.apply(lambda x: pyf.timeseries.perf_stats(x))
    groupRatio.to_csv(""+mydir+"/GroupRatio_8.csv")
    #print("groupRatio ",groupRatio)
    #Mongo
    #db.insertDataToMongo("groupratio_securities", groupRatio)

    #log.info("Securities event completed in {} ".format(time.time() - start))
    print("Securities event completed in {} ".format(time.time() - start))





""" {
    "eventname": "returns",
    "user": {
        "user_id": "sagarsp",
        "sessionid": "237260429126477122008302166698",
        "app_id": "xyzdas123"
    },
    "data": {
        "event": "securities",
        "date": "2020-01-10"
    }
} """