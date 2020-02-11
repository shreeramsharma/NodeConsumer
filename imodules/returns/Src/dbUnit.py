from pymongo import MongoClient
import pandas as pd
import json
import cx_Oracle
import os
import sys
import logging
import numpy as np

log = logging.getLogger("rabbit.returns.dbUnit")

dir = os.path.dirname(os.path.abspath(__file__))
configFilename = os.path.join(dir, "dbConfig.json")

if not os.path.isfile(configFilename):
    log.info("Please check database configuration filename provided: " + configFilename)
    sys.exit()

with open(configFilename) as conf:
    config = json.load(conf)

# Mongo Config
print(config["mongo"]["host"])
client = MongoClient(config["mongo"]["host"])
db = client[config["mongo"]["database"]]

# Oracle Config
oracleConfig = config["oracle"]
oraUser = oracleConfig["oraUser"]
oraPassword = oracleConfig["oraPassword"]
oraHost = oracleConfig["oraHost"]
oraPort = oracleConfig["oraPort"]
oraInstance = oracleConfig["oraInstance"]
queries = oracleConfig["queries"]

try:
    dsnStr = cx_Oracle.makedsn(oraHost, oraPort, oraInstance)
    con = cx_Oracle.connect(user=oraUser, password=oraPassword, dsn=dsnStr)
except cx_Oracle.DatabaseError as e:
    error, = e.args
    if error.code == 1017:
        log.error("Please check credentials provided in the configuration file.")
        raise
    elif error.code == 12170:
        log.error("Please check database host details provided in the configuration file.")
        raise
    elif error.code == 12541:
        log.error("Please check database host and port details provided in the configuration file.")
        raise
    elif error.code == 12505:
        log.error("Please check SID details provided in the configuration file.")
        raise
    else:
        log.error("Database connection error")
        raise


def getDataFromMongo(collectionName, date=""):
    collection = db.get_collection(collectionName)
    if collection.count_documents({}) > 0:
        if collectionName == "groupreturns":
            data = pd.DataFrame(list(collection.find({"ASONDATE": date}))).drop(columns="_id")
        else:
            data = pd.DataFrame(list(collection.find())).drop(columns="_id")
    else:
        data = pd.DataFrame()
    return data


def insertDataToMongo(collectionName, df, date=""):
    """
    specify date if you insert into groupreturns.

    """
    collection = db.get_collection(collectionName)
    if collectionName == "groupreturns":
        # data = {"Date": date, "Data": df}
        df["ASONDATE"] = date
        df = df.rename(columns={"level_0": "RETURNTYPE", "level_1": "CODE"})
        df = df.to_dict("records")
        for row in df:
            collection.update_one({"ASONDATE": date, "CODE": row["CODE"]}, {"$set": row}, upsert=True)
    elif collectionName == "groupreturns_portsec":
        df = df.to_dict("records")
        for row in df:
            collection.update_one({"ASONDATE": date, "PORTCODE": row["PORTCODE"], "ISIN": row["ISIN"]}, {"$set": row}, upsert=True)
    elif (collectionName == "rawdatareturns_nonnavport") or (collectionName == "dailyclreturns_nonnavport"):
        df["ASONDATE"] = df["ASONDATE"].astype(str) 
        df = df.to_dict("records")
        collection.delete_many({"ASONDATE": date})
        collection.insert_many(df)
    else:
        df = df.to_dict("records")
        collection.drop()
        collection.insert_many(df)


def getDataFromOracle(option, date=""):
    cur = con.cursor()
    cur.execute("ALTER SESSION SET nls_Date_format = 'yyyy/mm/dd'")
    # cur.execute("set define off")
    query = queries[option]

    if option == "trindex_daily":
        cur.execute(query.format(date, date))
    elif "daily" in option:
        if option == "dailyRawData":
            cur.execute(query.format(date, date))
        else:
            cur.execute(query.format(date))
    else:
        cur.execute(query.format(date))
    colNames = [x[0] for x in cur.description]
    rows = cur.fetchall()
    if len(rows) == 0:
        log.warn("No data found for {} inside oracle for {}".format(option, date))
        return pd.DataFrame()
    if "trindex" in option:
        return pd.DataFrame(rows, columns=colNames).rename(columns={"INDEX_CODE": "BENCHMARK", "INDEX_DATE": "ASONDATE", "TRINDEX_VALUE": "VALUE"})
    elif "nav" in option:
        return pd.DataFrame(rows, columns=colNames).rename(columns={"CALCDATE": "ASONDATE", "CALCVALUE": "VALUE"})
    # elif "price" in option:
    #     return pd.DataFrame(rows, columns=colNames).rename(columns={"MTMDATE": "ASONDATE", "MTMPRICE": "VALUE"})
    else:
        return pd.DataFrame(rows, columns=colNames).rename(columns={"PORTFOLIO": "PORTCODE"})


def insertDataToOracle(dfName, dataframe, type=""):
    cur = con.cursor()
    if dfName == "groupreturns":
        dataframe = dataframe.replace(np.inf, np.nan).fillna(0)
        dataframe['level_1'] = dataframe['level_1'].astype(str)
        cur.execute("delete from IF_GROUPRETURNS where TYPE='{}' AND ASONDATE='{}'".format(type,dataframe['ASONDATE'].unique()[0]))
        cur.prepare("INSERT INTO IF_GROUPRETURNS (RETURNTYPE,CODE,D1,W1,FN1,M1,M3,M6,M9,Y1,YTD,Y2,Y2_ANN,Y3,Y3_ANN,Y5,Y5_ANN,INCEPTION,INCEPTION_ANN,TYPE,ASONDATE) values (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,to_date(:21,'yyyy-mm-dd'))")
    if dfName == "iwf_return":
        dataframe = dataframe.replace(np.inf, np.nan).fillna(0)
        dataframe=dataframe.iloc[:,1:]
        cur.execute("delete from iwf_return where instrument_type='{}' AND ASONDATE=to_date('{}','yyyy-mm-dd')".format(type,dataframe['ASONDATE'].unique()[0]))
        cur.prepare("INSERT INTO iwf_return (instrument_code,return_type,asondate,instrument_type,frequency,return_value) values (:1,:2,to_date(:3,'yyyy-mm-dd'),:4,:5,:6)")
        #cur.prepare("INSERT INTO if_return_data (matrixcode, asondate, from_date, to_date, portfolio, portfolio_return, portfolio_from_date, portfolio_to_date, sub_category, sub_category_value) values(999999,to_date(:3,'yyyy-mm-dd'),to_date(:3,'yyyy-mm-dd'),to_date(:3,'yyyy-mm-dd'),:1,:6,to_date(:3,'yyyy-mm-dd'),to_date(:3,'yyyy-mm-dd'),:4,:1)")
    
    if dfName == "if_return_data":
        dataframe = dataframe.replace(np.inf, np.nan).fillna(0)
        dataframe=dataframe.iloc[:,1:]
        dataframe["matrix"]=dataframe['frequency']
        dataframe["from_date"]=dataframe["ASONDATE"]
        dataframe["to_date"]=dataframe["ASONDATE"]
        dataframe["portfolio"]=dataframe["instrument_code"]
        dataframe.drop(['frequency'], axis = 1, inplace=True)
        #print(dataframe)
        cur.execute("delete from if_return_data where sub_category_value='{}' AND asondate=to_date('{}','yyyy-mm-dd')".format(type,dataframe['ASONDATE'].unique()[0]))
        cur.prepare("INSERT INTO if_return_data ( sub_category, return_type, asondate, sub_category_value,portfolio_return, matrixcode, from_date, to_date, portfolio) values(:1,:2,to_date(:3,'yyyy-mm-dd'),:4,:5,:6,to_date(:7,'yyyy-mm-dd'),to_date(:8,'yyyy-mm-dd'),:9)")
    elif dfName == "portfolioreturns":
        cur.execute("delete from if_portholdingpercreturns")
        cur.prepare("insert into if_portholdingpercreturns (PORTCODE,ISIN,AMOUNT,ASSETCLASS,SECTOR,ASONDATE,WEIGHTS,D1,FN1,M1,W1,Y1,Y2,M3,Y3,Y5,M6,M9,INCEPTION) values (:1,:2,:3,:4,:5,to_date(:6,'yyyy-mm-dd'),:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19)")
    # cur.executemany(None, dataframe.values.tolist())
    cur.executemany(None, dataframe.values.tolist())
    con.commit()
