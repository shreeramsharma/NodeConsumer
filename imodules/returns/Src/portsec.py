# from pymongo import MongoClient
import Src.dbUnit as db
import Src.computeUnit as cu
import numpy as np
import pandas as pd
import logging
import time
import os.path
from datetime import datetime
import traceback

log = logging.getLogger("processunit")
pd.options.mode.chained_assignment = None


def process(date):
    try:
        start = time.time()
        mydir = os.path.join("./imodules/returns/Working_Sheet/PortSec_Returns_Working_Sheet", datetime.now().strftime('%Y-%m-%d_%H-%M-%S'))
        os.makedirs(mydir)
        #log.info(mydir)
        portfolio = db.getDataFromOracle("port_daily", date).fillna(0).replace("NA", 0)
        portfolio.to_csv(""+mydir+"/Portfolio_Data_1.csv")
        
        if not portfolio.empty:
            portfolio["ASONDATE"] = pd.to_datetime(date, format="%Y-%m-%d")
            portfolio["ISIN"] = portfolio["ISIN"].astype(str)
            portfolio["AMOUNT"] = portfolio["AMOUNT"].astype(int)

            portWeights = portfolio.pivot_table(index="PORTCODE", columns="ISIN", values="AMOUNT").apply(lambda x: x / (x.sum()), axis=1).stack().reset_index().rename(columns={0: "WEIGHTS"})
            portWeights = portfolio.merge(portWeights, on=["PORTCODE", "ISIN"], how="left")
            
            # dailyAdjPrice = db.getDataFromMongo("dailyadjclose_securities", date)
            #get the securities return for the date and frequency 1D
            dailyAdjPrice=db.getDataFromOracle("port_daily_return", date)
            dailyAdjPrice=pd.DataFrame(dailyAdjPrice)
            dailyAdjPrice.to_csv(""+mydir+"/Securities_Data_2.csv")
            dailyAdjPrice.set_index("ISIN", inplace=True)
            filteredDailyAdjPrice = pd.DataFrame(portWeights)
            filteredDailyAdjPrice.set_index("ISIN",inplace=True)
            dailyAdjPrice = filteredDailyAdjPrice.merge(dailyAdjPrice, left_index=True, right_index=True, how="left").replace(np.inf, np.nan).fillna(0)
            dailyAdjPrice.to_csv(""+mydir+"/Portfolio_Weight_3.csv")

            dailyAdjPrice["Weight_X_Return"]= dailyAdjPrice["WEIGHTS"].values * dailyAdjPrice.RETURN_VALUE.values
            dailyAdjPrice=dailyAdjPrice.groupby(["PORTCODE","ASONDATE"]).sum()
            dailyAdjPrice["Log10(Weight_X_Return)"]=np.log10(dailyAdjPrice["Weight_X_Return"]).replace([np.inf, -np.inf], np.nan).fillna(0)
            dailyAdjPrice.reset_index(inplace=True)
            dailyAdjPrice=pd.DataFrame(dailyAdjPrice,columns=["ASONDATE","PORTCODE","Log10(Weight_X_Return)"])
            dailyAdjPrice.rename(columns={"PORTCODE":"ISIN"}, inplace=True)
            dailyAdjPrice.to_csv(""+mydir+"/Portfolio_log10_4.csv")
            
            portfolio_1D = db.getDataFromOracle("port_1D_return", date).fillna(0).replace("NA", 0)
            portfolio_1D=pd.DataFrame(portfolio_1D)
            if portfolio_1D.empty:
                portfolio_1D=pd.DataFrame(portfolio_1D, columns=["ASONDATE","ISIN","RETURN_VALUE"])
            portfolio_1D.rename(columns={"RETURN_VALUE":"Log10(Weight_X_Return)"}, inplace=True)
            portfolio_1D["ISIN"]=portfolio_1D["ISIN"].astype("int64")
            New_dailyAdjPrice=dailyAdjPrice.append(portfolio_1D, ignore_index=True)
            
            New_dailyAdjPrice_pivot = New_dailyAdjPrice.pivot_table(index="ASONDATE", columns=["ISIN"])
            New_dailyAdjPrice=New_dailyAdjPrice.fillna(0)
            New_dailyAdjPrice_pivot.to_csv(""+mydir+"/Portfolio_Returns_5.csv")

            groupReturns = cu.calcGroupReturns(date, New_dailyAdjPrice_pivot, ["Log10(Weight_X_Return)"])
            groupReturns = groupReturns.reset_index()

            groupReturns.rename(columns = {"level_1":"ISIN"}, inplace=True)
            groupReturns.drop(columns=["level_0"], inplace=True)
            groupReturns.to_csv(""+mydir+"/Group_Returns_6.csv")

            portGroupReturns = dailyAdjPrice.merge(groupReturns, on="ISIN", how="left")
            portGroupReturns["instrument_type"]="PORT-SECURITIES"
            portGroupReturns["return_type"]="LOGRETURNS"

            portGroupReturns=pd.DataFrame(portGroupReturns,columns=["ASONDATE","1D","1W","1FN","1M","3M","6M","9M","1Y","YTD","2Y","2Y_ANN","3Y","3Y_ANN","5Y","5Y_ANN","INCEPTION","INCEPTION_ANN","ISIN","return_type","instrument_type"])
            portGroupReturns=pd.melt(portGroupReturns, id_vars=["ISIN","return_type","ASONDATE","instrument_type"],var_name="frequency")
            portGroupReturns.rename(columns = {"ISIN":"instrument_code"}, inplace=True)
            portGroupReturns=portGroupReturns.fillna(0)
            portGroupReturns['ASONDATE'] = portGroupReturns['ASONDATE'].dt.date
            portGroupReturns["ASONDATE"]=portGroupReturns["ASONDATE"].astype(str)
            portGroupReturns.to_csv(""+mydir+"/Portfolio_Group Returns_7.csv")

            #db.insertDataToOracle("portfolioreturns", portGroupReturns, type="PORT-SECURITIES")
            db.insertDataToOracle("iwf_return", portGroupReturns.reset_index(), type="PORT-SECURITIES")

            #log.info("portSec event completed  in {} ".format(time.time() - start))
            print("portSec event completed  in {} ".format(time.time() - start))
        else:
            log.warn(f"Calculation skipped for {date}.")
    except db.cx_Oracle.DatabaseError as dberror:
        log.error("restart the program. to fix this error: ", dberror)
    except AttributeError as ae:
        log.error("Attribute Error", ae)
    except Exception as exp:
        log.error(traceback.format_exc(exp))






""" {
    "eventname": "returns",
    "user": {
        "user_id": "sagarsp",
        "sessionid": "237260429126477122008302166698",
        "app_id": "xyzdas123"
    },
    "data": {
        "event": "portsec",
        "date": "2020-01-10"
    }
} """