import numpy as np
import pandas as pd
import logging
import Src.dbUnit as db
import os

log = logging.getLogger("rabbit.returns.processUnit")
pd.options.mode.chained_assignment = None
dir = os.path.dirname(os.path.abspath(__file__))

def calcAdjustedPrices(rawPrices, dividend, splitRatio):
    rawPrices.sort_index(ascending=False, inplace=True)
    dividend.sort_index(ascending=False, inplace=True)
    splitRatio.sort_index(ascending=False, inplace=True)

    pivot_adj_price_df = pd.DataFrame(index=rawPrices.index, data=np.zeros(rawPrices.shape), columns=rawPrices.columns)
    pivot_adj_price_df.iloc[0] = round(rawPrices.iloc[0], 4)

    for i in range(1, len(rawPrices)):
        pivot_adj_price_df.iloc[i] = pivot_adj_price_df.iloc[i - 1].values * (((rawPrices.iloc[i].values * splitRatio.iloc[i - 1].values) - dividend.iloc[i - 1].values) / rawPrices.iloc[i - 1].values)

    #     adjPrice = pivot_adj_price_df.stack().reset_index().rename(columns={0:'adjPrice','level_1':'ISIN'})

    return pivot_adj_price_df


def calcHistGroupReturns(returns, save=False, asonDate="", identifier=""):
    returns.index = pd.to_datetime(returns.index)
    """
    Calc Historic Group returns grouped by month, quater and year.
    :param returns : log returns of securities
    :param format:
        |-------+------+------+------|
        |  Date | Sec1 | Sec2 | Sec3 |
        |-------+------+------+------|
        | 01-01 | r1   | r2   | r3   |
        | 02-01 | r1   | r2   | r3   |
    :type data: DataFrame
    :return: anti-log Group returns for monthy, quaterly and yearly.
    :rtype: DataFrames.
    """
    dailyRet = np.exp(returns) - 1
    monthlyGrpRet = np.exp(returns.resample("1M").sum()) - 1
    quatGrpRet = np.exp(returns.resample("3M").sum()) - 1
    yearlyGrpRet = np.exp(returns.resample("1Y").sum()) - 1

    if save:
        db.saveToDisk(dailyRet, "{}_{}_1D".format(identifier, asonDate))
        db.saveToDisk(monthlyGrpRet, "{}_{}_1Mgrp".format(identifier, asonDate))
        db.saveToDisk(quatGrpRet, "{}_{}_3Mgrp".format(identifier, asonDate))
        db.saveToDisk(yearlyGrpRet, "{}_{}_1Ygrp".format(identifier, asonDate))

    if identifier == "ratefeed":
        db.insertDataToMongo(f"{identifier}_{asonDate}_1D", dailyRet.stack().reset_index().rename(columns={"Date": "ASONDATE", "level_1": "CODE", 0: "RETURNS"}))
    return dailyRet, monthlyGrpRet, quatGrpRet, yearlyGrpRet


def calcGroupReturns(date, closingBal, cols):
    """
    Calculate Rolling Group Returns from a specified Date
    :param date : <str> in "%Y-%m-%d" format
    :param closingBal: Log Returns in DataFrame. 
                 +-------------+
                 | LOGRETURNS  |
    |------------+-------------+-------------+-------------|
    |   ASONDATE | PORT1:ISIN1 | PORT1:ISIN2 | PORT2:ISIN1 |
    |------------+-------------+-------------+-------------|
    | 2018-01-08 | logRet1     | logRet2     | logRet3     |

    :return: anti-log Group returns for monthy, quaterly and yearly.
    :rtype: DataFrames.
    """
    # ### Calculate Group Returns
    # **Input** : date {Str} AND closingbal {DataFrame}
    # **Output** : returnGroup {DataFrame}

    # - From the specified date this function gives log Returns for all frequencies on closing balance.
    date = pd.to_datetime(date, format="%Y-%m-%d")
    dateData = {
        "1D": closingBal[cols][:date].resample("1D").sum().iloc[-1:].sum().values,
        "1W": closingBal[cols][:date][-7:].sum().values,
        "1FN": closingBal[cols][:date][-7*2:].sum().values,
        "1M": closingBal[cols][:date][-30:].sum().values,
        "3M": closingBal[cols][:date][-30*3:].sum().values,
        "6M": closingBal[cols][:date][-30*6:].sum().values,
        "9M": closingBal[cols][:date][-30*9:].sum().values,
        "1Y": closingBal[cols][:date][-365:].sum().values,
        "FY": closingBal[cols][:date].resample('BYS').sum().iloc[-1].values,
        "2Y": closingBal[cols][:date][-365*2:].sum().values,
        "2Y_ANN": pow(closingBal[cols][:date][-365*2:].sum().values, (365/(closingBal[cols][:date].resample("2Y").size().sum()))),
        "3Y": closingBal[cols][:date][-365*3:].sum().values,
        "3Y_ANN": pow(closingBal[cols][:date][-365*3:].sum().values, (365/(closingBal[cols][:date].resample("3Y").size().sum()))),
        "5Y": closingBal[cols][:date][-365*5:].sum().values,
        "5Y_ANN": pow(closingBal[cols][:date][-365*5:].sum().values, (365/(closingBal[cols][:date].resample("5Y").size().sum()))),
        "INCEPTION": closingBal[cols][:date].sum().values,
        "INCEPTION_ANN": pow(closingBal[cols][:date].sum().values, (365/(closingBal[cols][:date].shape[0])))

    }
    dateRangeReturns = pd.DataFrame(index=pd.MultiIndex.from_product([cols, closingBal.columns.levels[1]]), data=dateData)
    #     dateRangeReturns = pd.DataFrame(index=['PORTCODE','PORTCODE:Bond','PORTCODE:Equity'],data=dateData)
    #return np.exp(dateRangeReturns) - 1
    dateRangeReturns=pow(10,dateRangeReturns)-1
    dateRangeReturns.replace(-1,0, inplace=True)
    dateRangeReturns.fillna(0, inplace=True)

    return dateRangeReturns


def concatCategory(retBalSheet):
    # ### Groupby Asset
    # **input** : balanceSheet with PORTCODE and assetclass columns {DataFrame}
    # **output** : transformed balance sheet {DataFrame}

    # - balance sheet can contain multiple PORTCODEs and asset class,
    # - this function simplifies the balance sheet
    # - by combining PORTCODE and asset into a single column which consists of all possible combination of PORTCODEs and assetclasses.

    # GROUPING balance sheet as per PORTCODE and assetclass

    # for PORTCODE
    retBalSheet["PORTCODE"] = retBalSheet["PORTCODE"].astype(str)
    portBalSheet = retBalSheet.pivot_table(index=["ASONDATE", "PORTCODE"], columns=["OPERATION"], values="AMOUNT", aggfunc="sum")
    # for assets
    retBalSheet["PORTCODE"] = retBalSheet["PORTCODE"].astype(str)
    retBalSheet["PORTCODE"] += ":" + retBalSheet["CATEGORY"]
    portAssetBalSheet = retBalSheet.pivot_table(index=["ASONDATE", "PORTCODE"], columns=["OPERATION"], values="AMOUNT", aggfunc="sum")
    retBalSheet = portBalSheet.append(portAssetBalSheet)

    return retBalSheet.unstack().stack()


def sumXactions(retBalSheet):
    # ### Sum of all Transaction operations
    # **input** : raw dailyreturns balanceSheets {Dataframe}
    # **output** : raw dailyreturns balanceSheets with calculated sum of specific Xactions {Dataframe}

    # - accepts raw Xactions and sum all Xactions according to specified conditions.
    rettest = retBalSheet.reset_index().set_index(["ASONDATE"])

    forPort = rettest[~rettest["PORTCODE"].str.contains(":", na=False)].reset_index().set_index(["ASONDATE", "PORTCODE"])
    forNonPort = rettest[rettest["PORTCODE"].str.contains(":", na=False)].reset_index().set_index(["ASONDATE", "PORTCODE"])

    # netSUM:
    netPortSum = forPort.sum(1)
    netNonPortSum = forNonPort.drop(["CASH IN-OUT", "MAN FEES", "ADMIN FEES"], 1).sum(1)
    netSum = pd.concat([netPortSum, netNonPortSum], axis=1).fillna(0).sum(1)
    # GrossSUM:
    grPortSum = forPort.sum(1)
    grNonPortSum = forNonPort.drop(["CASH IN-OUT"], 1).sum(1)
    grSum = pd.concat([grPortSum, grNonPortSum], axis=1).fillna(0).sum(1)

    rettest = rettest.reset_index().set_index(["ASONDATE", "PORTCODE"])
    rettest["NETSUMXACTION"] = netSum
    rettest["GRSUMXACTION"] = grSum
    rettest.to_csv(os.path.join(dir, "excel/output/sumTransaction.csv"))
    return rettest


def calcDailyCloseReturns(retBalSheet):
    """
    # **input** : Balance Sheet {DataFrame}
    # *cols = [Date, Group, Asset, Operation, Amount]*
    # **output** : closing Balance Sheet {DataFrame}
    # *cols = [GROPEN, NETOPEN, GRCLOSE, NETCLOSE, GRRETURNS, NETRETURNS, LOGGRRETURNS, LOGNETRETURNS]*

    # - Calculates gross clossing, net closing, gross returns, net returns, log gross returns, log net returns.

    # balSheet = retBalSheet.apply(lambda row: sumXactions(row), axis=1)  # balSheet = pivotRetBalSheet.reset_index().set_index('DATE')
    """
    balSheet = sumXactions(retBalSheet)
    # Initialize closing balance df
    closingBal = pd.DataFrame(index=balSheet.index)
    closingBal = closingBal.assign(**{"GROPEN": np.zeros(balSheet.index.size), "NETOPEN": np.zeros(balSheet.index.size), "GRCLOSE": np.zeros(balSheet.index.size), "NETCLOSE": np.zeros(balSheet.index.size), "NETCLWOCASH": np.zeros(balSheet.index.size), "CASHINOUT": balSheet["CASH IN-OUT"], "GRRETURNS": np.zeros(balSheet.index.size), "NETRETURNS": np.zeros(balSheet.index.size), "LOGGRRETURNS": np.zeros(balSheet.index.size), "LOGNETRETURNS": np.zeros(balSheet.index.size)})
    closingBal = closingBal.unstack("PORTCODE")

    closingBal["GROPEN"].iloc[0] = 0
    closingBal["NETOPEN"].iloc[0] = 0

    balSheetUnstacked = balSheet.unstack()
    closingBal["NETCLOSE"].values[:] = closingBal["NETOPEN"].values[0] + balSheetUnstacked["NETSUMXACTION"].cumsum().values
    closingBal["NETCLWOCASH"] = closingBal["NETCLOSE"].values - closingBal["CASHINOUT"].values

    closingBal["GRCLOSE"].values[:] = closingBal["GROPEN"].values[0] + balSheetUnstacked["GRSUMXACTION"].cumsum().values
    closingBal["NETOPEN"].values[1:] = closingBal["NETCLOSE"].values[:-1]
    closingBal["GROPEN"].values[1:] = closingBal["GRCLOSE"].values[:-1]

    # closingBal["NETRETURNS"] = closingBal["NETCLOSE"].pct_change()
    closingBal["NETRETURNS"] = closingBal[["NETOPEN", "NETCLWOCASH"]].apply(lambda x: (x['NETCLWOCASH']/x['NETOPEN'])-1, axis=1).replace(np.inf,np.nan).fillna(0)
    closingBal["GRRETURNS"] = closingBal["GRCLOSE"].pct_change()
    
    closingBal["LOGGRRETURNS"] = np.log(closingBal["GRCLOSE"].pct_change() + 1)
    closingBal["LOGNETRETURNS"] = np.log(closingBal["NETRETURNS"] + 1)
    #closingBal["LOGNETRETURNS"] = np.log(closingBal["NETCLOSE"].pct_change() + 1)

    allDateClosing = pd.DataFrame(index=pd.date_range(closingBal.index.min(), closingBal.index.max()))
    closingBal = allDateClosing.merge(closingBal,left_index=True,right_index=True,how="left")
    closingBal.columns = pd.MultiIndex.from_tuples(closingBal.columns)
    closingBal = closingBal.fillna(method="ffill")
    return closingBal


def xformation(caData, price):
    price["ISIN"] = price["ISIN"].fillna(0)
    price["MTMDATE"] = pd.to_datetime(price["MTMDATE"], format="%d-%m-%Y")

    # getting all dates prices by merge and ffill/bfill
    dateDf = pd.DataFrame(index=pd.date_range(price["MTMDATE"].min(), price["MTMDATE"].max()))

    distPrice = price.drop_duplicates(subset=["ISIN", "MTMDATE"]).reset_index().drop(columns="index")
    distPrice["MTMPRICE"] = distPrice["MTMPRICE"].astype(float)
    pivotDistPrice = distPrice.pivot_table(index="MTMDATE", columns="ISIN", values="MTMPRICE")

    cols = distPrice["ISIN"].unique()
    cols = cols[~pd.isna(cols)]

    pivotDf = pd.merge(dateDf, pivotDistPrice, left_index=True, right_index=True, how="left").fillna(method="ffill").fillna(method="bfill")
    pivotDf = pivotDf[cols]
    pivotDf = pivotDf.rename_axis(None, axis=1).stack().reset_index().rename(columns={"level_0": "Date", "level_1": "ISIN", 0: "MTMPRICE"})

    distCaData = caData.drop_duplicates(subset=["ISIN", "BC_TO_DATE"]).reset_index().drop(columns="index")
    mergeDF = pd.merge(pivotDf, distCaData, left_on=["ISIN", "Date"], right_on=["ISIN", "BC_TO_DATE"], how="left")
    mergeDF["OLD_RATIO"] = mergeDF["OLD_RATIO"].fillna(1)
    mergeDF["NEW_RATIO"] = mergeDF["NEW_RATIO"].fillna(1)

    finalDf = pd.DataFrame()
    finalDf["Date"] = mergeDF["Date"]
    finalDf["ISIN"] = mergeDF["ISIN"]
    finalDf["close"] = mergeDF["MTMPRICE"]
    finalDf["dividend"] = mergeDF["DIVIDEND_RATE"].fillna(0)
    finalDf["split_ratio"] = mergeDF["OLD_RATIO"] / mergeDF["NEW_RATIO"]
    finalDf.set_index("Date", inplace=True)
    finalDf.sort_index(inplace=True)

    rawPrices = finalDf.pivot_table(index="Date", columns="ISIN", values="close")[cols]
    dividend = finalDf.pivot_table(index="Date", columns="ISIN", values="dividend")[cols]
    splitRatio = finalDf.pivot_table(index="Date", columns="ISIN", values="split_ratio")[cols]

    return rawPrices, dividend, splitRatio


def calcBucketReturns(logReturns, identifier, scenerioName=""):
    dailyRet, monthlyGrpRet, quatGrpRet, yearlyGrpRet = calcHistGroupReturns(logReturns)
    bucketReturns = formatReturnsForBucket(dailyRet, "1D", identifier, scenerioName).append(formatReturnsForBucket(monthlyGrpRet, "1M", identifier, scenerioName), ignore_index=True).append(formatReturnsForBucket(quatGrpRet, "3M", identifier, scenerioName), ignore_index=True).append(formatReturnsForBucket(yearlyGrpRet, "1Y", identifier, scenerioName), ignore_index=True)
    return '';

def formatReturnsForBucket(returns, bucket, identifier, scenerioName):
    if identifier == "SECURITIES":
        returns = returns.stack().reset_index().rename(columns={"Date": "DATE", "level_1": "SECURITYCODE", 0: "RETURNS"})
        returns["PRODUCT"] = returns.iloc[-1]["SECURITYCODE"]
        returns["BUCKET"] = bucket
        returns["SCENERIONAME"] = scenerioName

    elif identifier == "BENCHMARK":
        returns = returns.stack().reset_index().rename(columns={"level_0": "DATE", "level_1": "BENCHMARK", 0: "RETURNS"})
        returns["BUCKET"] = bucket
    return returns
