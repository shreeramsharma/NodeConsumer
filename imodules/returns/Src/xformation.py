import pandas as pd
import time

start = time.time()
caData = pd.read_csv('excel/input/priceData/CA_DATA_11.csv')
priceJanJune17 = pd.read_csv('excel/input/priceData/Price Data Jan to Jun 17.csv')
priceJulDec17 = pd.read_csv('excel/input/priceData/Price Data Jul to Dec 17.csv')

# replace missing dates of BC_TO_DATE <-- BSE_NSE_EX_DATE

caData = caData.fillna({
    "BC_TO_DATE": caData["BSE_NSE_EX_DATE"]
})
caData['BC_TO_DATE'] = pd.to_datetime(caData['BC_TO_DATE'], format='%d/%m/%Y')


def xformation(portfolio, price):
    price['ISIN'] = price['ISIN'].fillna(0)
    price['MTMDATE'] = pd.to_datetime(price['MTMDATE'], format='%d-%m-%Y')

    # getting all dates prices by merge and ffill/bfill
    dateDf = pd.DataFrame(index=pd.date_range(price['MTMDATE'].min(), price['MTMDATE'].max()))

    distPrice = price.drop_duplicates(subset=["ISIN", "MTMDATE"]).reset_index().drop(columns='index')
    pivotDistPrice = distPrice.pivot_table(index="MTMDATE", columns="ISIN", values="MTMPRICE")

    cols = distPrice['ISIN'].unique()
    cols = cols[~pd.isna(cols)]

    pivotDf = pd.merge(dateDf, pivotDistPrice, left_index=True, right_index=True, how="left").fillna(method='ffill').fillna(method='bfill')
    pivotDf = pivotDf[cols]
    pivotDf = pivotDf.rename_axis(None, axis=1).stack().reset_index().rename(columns={'level_0': "Date", 'level_1': "ISIN", 0: "MTMPRICE"})

    distCaData = caData.drop_duplicates(subset=["ISIN", "BC_TO_DATE"]).reset_index().drop(columns='index')
    mergeDF = pd.merge(pivotDf, distCaData, left_on=["ISIN", "Date"], right_on=["ISIN", "BC_TO_DATE"], how='left')
    mergeDF['OLD_RATIO'] = mergeDF['OLD_RATIO'].fillna(1)
    mergeDF['NEW_RATIO'] = mergeDF['NEW_RATIO'].fillna(1)

    finalDf = pd.DataFrame()
    finalDf['Date'] = mergeDF['Date']
    finalDf['ISIN'] = mergeDF['ISIN']
    finalDf['close'] = mergeDF['MTMPRICE']
    finalDf['dividend'] = mergeDF['DIVIDEND_RATE'].fillna(0)
    finalDf['split_ratio'] = (mergeDF['OLD_RATIO'] / mergeDF['NEW_RATIO'])
    finalDf.set_index('Date', inplace=True)
    finalDf.sort_index(inplace=True)

    rawPrices = finalDf.pivot_table(index='Date', columns='ISIN', values='close')[cols]
    dividend = finalDf.pivot_table(index='Date', columns='ISIN', values='dividend')[cols]
    splitRatio = finalDf.pivot_table(index='Date', columns='ISIN', values='split_ratio')[cols]

    return rawPrices, dividend, splitRatio


# get all 2017 prices
price = priceJanJune17.append(priceJulDec17)

r, d, sr = xformation(caData, price)


print(time.time() - start)
# sys.stdout.flush()
