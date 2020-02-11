module.exports = (library) => {
    library.add('notify', async (dealmsg, dealevent) => {
        try {
            let callRest = funclib['callRest']
            let response = await callRest(`${config.notification.url}nc/user/msg/operationdashboard/create`, {
                "userid": "credence",
                "data": {
                    "id": "das",
                    "msg": dealmsg,
                    "event": dealevent
                },
                "bcc": "_ALL"
            })
            logger.info(`Notification: Response: ${response}`)
        } catch (error) {
            logger.error("Notification: Error: " + (error.stack) ? error.stack : error);
        }
    })
}