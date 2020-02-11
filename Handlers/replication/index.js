//const followRedirects = require('follow-redirects');
//followRedirects.maxRedirects = 10;
//followRedirects.maxBodyLength = 500 * 1024 * 1024;
const axios = require('axios');
const https = require('https');
const fs = require('fs');
const _ = require('lodash');

// query Jobs from settings.js
const settings = require('./jobs');
const jobs = settings.jobs;
const queuename = process.env.QUEUE;

// let connection, db;
let obj = {
    queuename: queuename,
    handler: async function (message) {

        logger.info(start + ` : [x] message received from "${queuename}"`);
        try {
            let mObj = JSON.parse(message.content.toString());
            if (mObj.data)
                mObj = mObj.data;
            var processJob = async function (job, index) {

                let srcQuery = job[mObj.operation].source;

                let queryObj = _.template(srcQuery);
                // query = (job[mObj.operation].templatefn(mObj.variables));
                let query = queryObj(mObj.variables);

                if (mObj.operation != 'delete') {
                    logger.info(start + ` : Executing Query for ${job.collectionName} to ${mObj.operation} @ ${index + 1}`);
                    let result = await connection.execute(query)

                    // logger.debug(start + " : Rows length: " + result.rows.length + " for job " + (index + 1));
                    if (result.rows.length == 0) {
                        // while update: if query returns no rows than delete the same row inside mongo. 
                        if (mObj.operation == "update") {
                            logger.info(start + ' : no records found in query- deleting from mongo.')
                            await insertToMongo([], mObj, job);
                        } else {
                            logger.warn(start + ` : Query executed but No data found! @ job ${index + 1}`);
                        }
                        return;
                    } else {
                        console.time('MapReduce');
                        let mDatav2 = result.rows.map(row => row.reduce((rowobj, colvalue, index) => { rowobj[result.metaData[index].name] = colvalue; return rowobj; }, {}));
                        console.timeEnd('MapReduce');

                        console.time('forloop');
                        let data2 = []
                        let rows = result.rows
                        for (let i = 0; i < rows.length; i++) {
                            rowObj = {}
                            for (let j = 0; j < (result.metaData).length; j++) {
                                rowObj[result.metaData[j].name] = rows[i][j];
                            }
                            data2.push(rowObj);
                        }
                        console.timeEnd('forloop');

                        await insertToMongo(mDatav2, mObj, job);
                        return;
                    }
                } else {
                    logger.info(start + ` : Only filters found for ${job.collectionName} to ${mObj.operation} @ job ${index + 1}`)
                    await insertToMongo({}, mObj, job);
                    return;
                }
            }
            // TODO: instead of iterating every jobs; filter/find the jobs by collection name using lodash/underscore. 
            for (let i = 0; i < jobs.length; i++) {
                if (jobs[i].collectionName == mObj.collectionName) {
                    await processJob(jobs[i], i);
                    return;
                }
                else if (i == (jobs.length - 1)) {
                    logger.warn(start + " : Collection not found in Jobs.js");
                }
            }
        }
        catch (error) {
            logger.error(start + " : ERROR : " + error);
        }
    }
}

async function insertToMongo(data, mObj, job) {
    try {
        logger.debug(start + ` : Insert to Mongo`);
        let mVariables = mObj.variables;
        let msgFilters = job[mObj.operation].filter
        let filter = msgFilters.reduce((output, currVal) => { { output[currVal] = mVariables[currVal] } return output; }, {});

        // Validate filters if all are present in the message
        // logger.info("filter:::"+JSON.stringify(filter));
        for (let i in filter) {
            if (typeof filter[i] == "undefined" & mObj.operation != "refresh" & mObj.operation != "create") {
                logger.warn(start + ` : Filter missing for ${i} in message`);
            }
        }
        // logger.debug(filter);

        // if query returns no data than delete that record from mongo
        if (data.length != 0) {
            await mongoConn[mObj.operation](mObj, data, filter);
        } else {
            await mongoConn.delete(mObj, data, filter);
        }
        //if (mObj.operation != "refresh") {
            let filteredData = _.map(data, function (object) {
                return _.pick(object, job.notificationFilter)
            });
           
		   if ((mObj.collectionName).indexOf("_deallisting")>0)
		   {
			   logger.info("mObj.operation::"+mObj.operation)
			   logger.info("data.length:::"+data.length)
			   if (mObj.operation=="delete" || (mObj.operation=="update" && data.length==0))
			   {
				   data={};
				   data.contractcode = mObj.variables.CONTRACTCODE;
				   mObj.operation = "delete";
			   }
		   }
		   if (mObj.operation == "refresh") 
			   data=[];
		   //For https
		   const httpsAgent = new https.Agent({
			  rejectUnauthorized: false,
			  cert: fs.readFileSync(process.env.NODE_SECURED_CERT),
			  key: fs.readFileSync(process.env.NODE_SECURED_KEY),
			  passphrase: process.env.NODE_PASSPHRASE
			});

//		   axios.post(`$process.env["NODE_TLS_REJECT_UNAUTHORIZED"] = 0`)
			//For https
           axios.post(`${process.env.NOTIFICATION_URL}/${mObj.collectionName}/${mObj.operation}`, {"dataset":mObj.collectionName, "body":data} ,{httpsAgent})
           //For http
           // axios.post(`${process.env.NOTIFICATION_URL}/${mObj.collectionName}/${mObj.operation}`, {"dataset":mObj.collectionName, "body":data} )
                .then(res => logger.info(start + " : notification sent."))
                .catch(err => logger.error(start + " : Failed sending notification " + err));          
        return;
    }
    catch (error) {
        logger.error(start + " : " + error);
    }
};

// mongo CRUD operations
mongoConn = {}

mongoConn['create'] = async function (mObj, data, filter) {
    let result = await db.collection(mObj.collectionName).insertMany(data);
    logger.debug(start + ' : ' + result + ' inserted documents in ' + mObj.collectionName);
    return;
}
mongoConn['refresh'] = async function (mObj, data, filter) {

    let result = await db.collection(mObj.collectionName).deleteMany({});
    logger.debug(start + ` : dropping collection ${mObj.collectionName} : ${result}`);

    result = await db.collection(mObj.collectionName).insertMany(data);
    logger.debug(start + ' : documents refreshed at ' + mObj.collectionName);
    return;
}
mongoConn['update'] = async function (mObj, data, filter) {
    await data.forEach(async function (row) {
        let result = await db.collection(mObj.collectionName).updateOne(filter, { $set: row }, { upsert: true });
        logger.debug(start + ' : ' + result + ' updated documents in ' + mObj.collectionName);
    });
    return;
}
mongoConn['delete'] = async function (mObj, data, filter) {
    let result = await db.collection(mObj.collectionName).deleteMany(filter);
    logger.debug(start + ' : ' + result + ' deleted documents in ' + mObj.collectionName);
    return;
}

module.exports = obj;