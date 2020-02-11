const { Parser } = require('json2csv');

function tocsv(fields, data) {
    const toParse = new Parser({ fields });
    const csv = toParse.parse(data);
    return csv;
}

module.exports = async function (handler) {
    // handler.on function registers the event handlers "csv" inside email consumer.
    handler.on("csv", async msg => {
        try {
            var callrest = funclib['callRest'];
            var servicename = msg.dataobj.filter.servicename;
            var query = await callrest(`${config.applicationIp}/funds_sc/historical/app/hist/${servicename}`, msg)
            var updateOraDB = funclib['updateOraDB']
            var result = await updateOraDB('funds', query);
            var head = Object.keys(result.data.rows[0]);
            var csvdata = tocsv(head, result.data.rows);
            var buffer = Buffer.from(csvdata);
            var servicefilenamemap = {
                his_acct_entries: "Account Entries.csv",
                his_amortisation: "Amorttisation.csv",
                his_bankbalances: "Bank Balance.csv",
                his_bank_transaction: "Bank Transaction.csv",
                his_deallisting: "Deal Listing.csv",
                his_interestaccrual: "Interest Accrual.csv",
                his_limit_status: "Limit Status.csv",
                his_nav: "NAV.csv",
                his_pas: "PAS.csv",
                his_trade_position: "Trade Position.csv",
                his_trailbalance: "Trail Balance.csv",
                his_settle_position: "Settle Position.csv"
            };
            var filename = servicefilenamemap[servicename];
            logger.info(`inserting csv data for ${filename} with servicename ${servicename}`)
            result = await updateOraDB('funds', "DELETE FROM his_csvdata WHERE FILENAME='" + filename + "'");
            result = await updateOraDB('funds', "insert into his_csvdata values(:filedata,:filename,:servicename)", { "filedata": buffer, "filename": filename, servicename: servicename });
            var notify = funclib['notify'];
            if (result.status) {
                notify({ url: "historical/app/hist/getcsv", type: "csv" }, filename);
                logger.info("FileOperation: Done");
            } else {
                logger.warn("settleprocess: update query failed");
            }
            return null;
        } catch (error) {
            logger.error(`FileOperation: Csv: Error: ${error}`);
        }
    })
}

let messageObject = {
    "eventname": "csv",
    "user": {
        "user_id": "tarannum",
        "sessionid": "tarannum",
        "app_id": "xyzdas123"
    },
    "dataobj": {
        "filter": {
            "fromdate": "21/08/2018",
            "todate": "22/08/2018",
            "issql": false,
            "servicename": "his_settle_position"
        }
    }
}