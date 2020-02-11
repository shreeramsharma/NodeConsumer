module.exports = emitter => {
    emitter.on("#", (msg, routingKey) => {
        logger.info(`Message Log: routing key: ${routingKey}, Msg: ${JSON.stringify(msg)}`)
    });
};