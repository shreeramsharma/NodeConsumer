
async function updateEodDetails(event, response, processid, srno, date) {
    if (response.status == "success") {
        logger.info(`Eod Process: Updating Eod Details for ${event}`)
        var processprogressqry = `SELECT * FROM iwz_process_progress WHERE process = '${processid}'`;
        logger.debug(`Eod Process: processprogressqry: ${processprogressqry}`);
        var processresp = await funclib["updateOraDB"]('funds', processprogressqry);
        if (!processresp.data.rows.length > 0) {
            throw { status: "unsuccess", message: `No records for processid ${processid} found of ${event}` }
        }
        var updatedetailsqry = `UPDATE opd_eod_details SET PROCESS_DETAILS = 'Successfully Generated for ${processresp.data.rows[0].SUCCESS} records and ${processresp.data.rows[0].ERRORS} has error. Please view the exception for error.', process_date = nvl(process_date+1,sysdate) WHERE srno = '${srno}'`;
        logger.debug(`Eod Process: updatedetailsqry: ${updatedetailsqry}`);
        var updateresp = await funclib["updateOraDB"]('funds', updatedetailsqry);
        if (updateresp) {
            funclib['notify']({
                status: "success",
                message: `${event} process completed for ${date}`,
                processid: `${processid}`,
                events: `${event}`,
                type: "process"
            }, `${event}`);
            logger.info(`Eod Process: ${event} Done`);
        }
        else throw {
            status: "unsuccess",
            message: `update eod details failed for ${event}`
        }
    }
    else {
        throw {
            status: response.status,
            message: `${event} process failed for ${date}: ${response}`,
            processid: `${processid}`,
            events: event,
            type: "process"
        };
    }
}

module.exports = async function (handler) {
    handler.on("accrual", async msg => {
        try {
            var processid = `accruals${msg.dataobj.processId}`;
            var accrualprocessurl = `${config.applicationIp}/Framewrk/Event.jsp?m=P1&v=V98&process=accruals&event=startProcessThreadBatch&JString={"portcode":"","security":"","accdate":"${msg.dataobj.date}","entitynameinterest":"","fwkParams":"[]","processId":"${processid}","iscalandsave":1,"asset_class_code":"0","group5":""}&sessionid=${msg.user.sessionid}&processId=${processid}`
            logger.info(`Eod Process: Calling accrual process url: ${accrualprocessurl}`)
            var response = await funclib['callRest'](accrualprocessurl)
            await updateEodDetails("accrual", response, processid, msg.dataobj.srno, msg.dataobj.date);
        } catch (error) {
            funclib['notify'](error, "accrual");
            logger.error("Eod Process: Accrual process failed: " + JSON.stringify(error));
        }
    });

    handler.on("valuation", async msg => {
        try {
            var processid = `valuation${msg.dataobj.processId}`;
            var valuationprocessurl = `${config.applicationIp}/Framewrk/Event.jsp?m=P1&v=V336&process=valuation&event=startProcessThreadBatch&JString={"valuation_date":"${msg.dataobj.date}","fwkParams":"[]","processId":"${processid}","assetclass":"0","iscalandsave":"1"}&sessionid=${msg.user.sessionid}&processId=${processid}`;
            logger.info(`Eod Process: Calling valuation process url: ${valuationprocessurl}`)
            var response = await funclib['callRest'](valuationprocessurl)
            await updateEodDetails("valuation", response, processid, msg.dataobj.srno, msg.dataobj.date);
        } catch (error) {
            funclib['notify'](error, "Valuation");
            logger.error("Eod Process: Valuation process failed: " + JSON.stringify(error));
        }
    });

    handler.on("ammortisation", async msg => {
        try {
            var processid = `ammortisation${msg.dataobj.processId}`;
            var ammortisationprocess = `${config.applicationIp}/Framewrk/Event.jsp?m=P1&v=V349A&process=ammortisations&event=startProcessThreadBatch&JString={"jPortfolio":"","jSecurity":"","jAssetClass":"ALL","jAsOnDate":"${msg.dataobj.date}","entitynameammortise":"","fwkParams":"[]","processId":"${processid}","iscalandsave":"calculate_save"}&sessionid=${msg.user.sessionid}&processId=${processid}`
            logger.info(`Eod Process: Calling ammortisation process url: ${ammortisationprocessurl}`)
            var response = await funclib['callRest'](ammortisationprocess)
            await updateEodDetails("ammortisation", response, processid, msg.dataobj.srno, msg.dataobj.date);
        } catch (error) {
            funclib['notify'](error, "ammortisation");
            logger.error("Eod Process: ammortisation process failed: " + JSON.stringify(error));
        }
    });
}

let msg2 = {
    "eventname": "accrual",
    "user": {
        "user_id": "tarannum",
        "sessionid": "tarannum",
        "app_id": "xyzdas123"
    },
    "dataobj": {
        "date": "13-08-2019"
    }
}