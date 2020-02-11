const oracledb = require('oracledb');
oracledb.outFormat = oracledb.OBJECT;
oracledb.autoCommit = true;
oracledb.poolMax = 5;

const MongoClient = require('mongodb').MongoClient;
let dbConn = {}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function retry(maxRetries, fn, config, dbName) {
    return fn(config).catch(async function (error) {
        logger.error(`Init : Error in ${dbName} connection: ${(error.stack) ? error.stack : error}: Retrying...`);
        await sleep(1000)
        if (maxRetries < 0) {
            logger.error("Max Retries reached.");
            throw error;
        }
        return retry(maxRetries - 1, fn, config, dbName);
    });
}

initOracle = async function (config) {
    let connection = null;
    try {
        if (config.oracledb) {
            let dateformat = config.NLS_DATE_FORMAT || "DD/MM/YYYY";
            var initSession = async function initSession(connection, requestedTag, cb) {
                let result = await connection.execute(
                    "alter session set nls_date_format = '" + dateformat + "'"
                );
                cb();
            };
            for (let index = 0; index < config.oracledb.length; index++) {
                const item = config.oracledb[index];
                logger.info(`InitDb: Creating pool for: ${item.alias}`)
                let connpool = await oracledb.createPool({
                    user: item.username,
                    password: item.password,
                    connectString: item.connectString,
                    sessionCallback: initSession
                });
                dbConn[item.alias] = connpool;
                // to check if db credentials are correct
                connection = await connpool.getConnection()
            }
            logger.info("InitDb: Oracle Connected");
            return { status: true, message: "Oracle Connected" };
        } else {
            logger.error("InitDb: No oracledb config found")
            return { status: false, message: "No oracledb config found" }
        }
    } catch (error) {
        throw error
    } finally {
        try {
            if (connection) await connection.close();
        } catch (error) {
            logger.error(`InitDb: Error: ${(error.stack) ? error.stack : error}`)
        }
    }
};

initMongo = async function (config) {
    if (config.mongodb) {
        for (let index = 0; index < config.mongodb.length; index++) {
            const item = config.mongodb[index];
            let aclient = await MongoClient.connect(item.connectString, {
                useNewUrlParser: true,
                poolSize: 10
            });
            dbConn[item.alias] = aclient.db(item.db);
            logger.info("Init: Mongo connected");
            return { status: true, message: "Mongodb Connected" };
        }
    } else {
        logger.error("Init: No Mongo config found")
        return { status: false, message: "No Mongo config found" };
    }
}

module.exports = async function initConnection(config) {
    await retry(3, initOracle, config, "Oracle");
    await retry(3, initMongo, config, "Mongo");
    return dbConn;
}