let axios = require("axios")
let https = require("https")

module.exports = async (library) => {
    library.add('callRest', async (url, data) => {
        try {
            logger.debug(`REST: Calling url: ${url} for data: ${data}`)
            const agent = new https.Agent({
                rejectUnauthorized: false
            });
            result = await axios.post(url, data, { httpsAgent: agent })
            return result.data;
        } catch (error) {
            throw (`REST: Error: ${(error.stack) ? error.stack : error}`)
        }
    });
}