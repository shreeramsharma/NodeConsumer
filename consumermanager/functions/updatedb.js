
function sleep(ms) {
    return new Promise(resolve => {
        setTimeout(resolve, ms)
    })
}
module.exports = (library) => {
    library.add('updateOraDB', async (pool, stmt, data={}) => {
        let connection = null;
        try {
            if (!global.dbconn[pool]) return { "status": false, "message": `${pool} alias not found inside config file` }
            connection = await global.dbconn[pool].getConnection();
            result = await connection.execute(stmt,data);
            return { "status": true, "data": result, "message": "query executed" }
        } catch (error) {
            throw `Updatedb: Error for query :${stmt} : ${(error.stack) ? error.stack : error}`;
        } finally {
            try {
                if (connection) await connection.close();
            } catch (error) {
                logger.error(`Updatedb: ${(error.stack) ? error.stack : error}`)
            }
        }
    })
}