module.exports = async (library) => {
    library.add('sendToExchange', async (exchange, msg, key) => {
        try {
            ch.assertExchange(exchange, "topic", { durable: true })
            logger.info(`SendToExchange: Sending msg ${JSON.stringify(msg)} to exchange with key ${key}`)
            ch.publish(exchange, key, Buffer.from(JSON.stringify(msg)));
        } catch (error) {
            logger.error(`SendToExchange: Error: ${(error.stack) ? error.stack : error}`)
        }
    });
}