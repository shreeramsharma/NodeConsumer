module.exports = async function (handler) {
    try {
        handler.on("sendMail", function (msg) {
            logger.info(`Send mail: Msg : ${msg.data.message}`)
        })
        return null
    } catch (error) {
        logger.error(error)
    }
}

testMsg = {
    "eventname": "sendMail",
    "data": {
        "message": "testing sendmail event"
    }
}