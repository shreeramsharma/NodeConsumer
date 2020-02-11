const path = require("path");
const amqp = require("amqplib");
const fs = require("fs");
// eventemitter2 supports wildcards.
var EventEmitter2 = require('eventemitter2').EventEmitter2;

var conn = null;
global.ch = null;

module.exports = async (config, dbConn, rootPath) => {
  global.dbpool = dbConn;
  try {
    conn = await amqp.connect({
      protocol: "amqp",
      hostname: config.rabbitmq.serverip,
      port: config.rabbitmq.port || 5672,
      username: config.rabbitmq.username,
      password: config.rabbitmq.password,
      locale: "en_US",
      frameMax: 0,
      heartbeat: 0,
      vhost: "/"
    });
    let queueFolders = fs.readdirSync(rootPath);
    ch = await conn.createChannel();
    /**
     * Loop through all Handlers
     * create queues with the same name as handler folder
     * and starts consuming messages for the same
     */
    for (let i = 0; i < queueFolders.length; i++) {
      if (fs.statSync(path.join(rootPath, queueFolders[i])).isDirectory()) {
        // create new event emmiter object for every handlers
        // so that, same eventname wont get triggered of other consumer/handler.
        let qHandler = new EventEmitter2({ wildcard: true });

        // create queue for handlers if not present.
        await ch.assertQueue(queueFolders[i], { durable: true, maxPriority: 10 });

        // Bind queue/handler to exchange with respective keys
        if (fs.existsSync(path.join(rootPath, queueFolders[i], "exchangekeys.json"))) {
          let exchangekeys = require(path.join(rootPath, queueFolders[i], "exchangekeys.json"));
          for (let index = 0; index < exchangekeys.keys.length; index++) {
            ch.assertExchange(exchangekeys.exchange, 'topic');
            ch.bindQueue(queueFolders[i], exchangekeys.exchange, exchangekeys.keys[index]);
          }
        }

        logger.info(`InitConsumer: Waiting for messages in ${queueFolders[i]}`);
        let eventFolders = fs.readdirSync(path.join(rootPath, queueFolders[i]));
        /**
         * loop inside handler folder
         * if any js file found, then require the same as eventhandler
         * if any directory found, then require the index file of the same as event handler
         */
        for (let j = 0; j < eventFolders.length; j++) {
          let eventPath = path.join(rootPath, `${queueFolders[i]}/${eventFolders[j]}`);
          if (fs.statSync(eventPath).isDirectory() || path.extname(eventFolders[j]) == ".js") {
          //  await require(eventPath)(qHandler);
            await ch.prefetch(1);
            // starts consuming messages for the handlers
            await ch.consume(queueFolders[i], async function (msg) {
              try {
                if (msg !== null) {
                  let st = new Date();
                  let msgobj = JSON.parse(msg.content);
                  logger.info(`InitConsumer: Message received for: ${queueFolders[i]} consumer, msg: ${JSON.stringify(msgobj)}`)
                  await qHandler.emit(msgobj.eventname, queueFolders[i], msg.fields.routingKey, msgobj);
                  ch.ack(msg);
                  logger.debug("InitConsumer: Completion time: " + (Date.now() - st) / 1000);
                }
              } catch (error) {
                ch.ack(msg);
                logger.error(`InitConsumer: Error while consuming: ${(error.stack) ? error.stack : error}`);
              }
            });
          }
        }
      }
    }
  } catch (error) {
    logger.error(`InitConsumer: Error: ${(error.stack) ? error.stack : error}`);
    process.exit(1);
  }
};
