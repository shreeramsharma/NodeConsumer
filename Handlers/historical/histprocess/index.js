const fs = require("fs");
const path = require("path");
const moment = require("moment");
var sendToExchange = funclib['sendToExchange']
let notify = funclib['notify']
let queryModels = require("./queryModels")
const qryconfig = JSON.parse(fs.readFileSync(path.join(__dirname, "qryconfig.json")));
const dateformat = (config.DateFormat) ? config.DateFormat : "DD/MM/YYYY"
const historicalProcess = async function (msg) {
  try {
    let eventName = msg.eventname;
    let mObj = msg.data
    let userObj = msg.userObj
    if (mObj.fdate && mObj.tdate) {
      let fromdate = moment(mObj.fdate, dateformat);
      let todate = moment(mObj.tdate, dateformat);
      logger.info(`HistProcess: Executing Hist Jobs for ${fromdate.format(dateformat)} to ${todate.format(dateformat)}`)
      for (var m = moment(fromdate); m.isSameOrBefore(todate); m.add(1, 'days')) {
        let promiseArr = []
        let models = qryconfig[eventName]
        for (let i of models) {
          logger.info(`HistProcess: preparing ${i} query`)
          promiseArr.push(queryModels[i](m.format(dateformat), userObj.sessionid));
        }
        logger.info(`HistProcess: executing all queries for date : ${m.format(dateformat)}`)
        await Promise.all(promiseArr);
      }
      logger.info(`HistProcess: All Queries Executed for date ${fromdate} - ${todate}`);
      notify(`historical data posted for ${mObj.fdate} to ${mObj.tdate}`, "historical");
      sendToExchange("sysevents", {
        eventName: eventName,
        data: {
          fromdate: mObj.fdate,
          todate: mObj.tdate
        }
      }, "etl")
      logger.info("HistProcess: Message sent to exchange.");
    } else {
      logger.warn("HistProcess: fdate and tdate not found")
    }
  } catch (error) {
    logger.error(`HistProcess Error: ${error}`);
    notify(`historical Failed, Error: ${error}`, "historical");
  }
}

module.exports = emitter => {
  emitter.on("eod", async msg => {
    await historicalProcess(msg);
  });

  emitter.on("intraday", async msg => {
    await historicalProcess(msg);
  })
};

a = {
  "eventname": "eod",
  "data": {
    "fdate": "26-07-2019",
    "tdate": "30-07-2019",
    "portcode": "",
    "securitysymbol": ""
  },
  "userObj": {
    "sessionid": "session12345",
    "userid": "u01"
  }
}