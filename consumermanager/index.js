const logger = require("./logger");
const path = require("path");
const decrypt = require("./decrypt");
global.logger = logger;
global.dbconn = null;
global.config = null;
global.config = require('../Handlers/config')
module.exports = { 
    init: async opt => {
        try {
            // config = JSON.parse(decrypt(path.join(opt.path, 'config.json'), path.join(opt.path, 'public.pem')));
            logger.debug(`ConsumerManager: config: ${JSON.stringify(config)}`);

            dbconn = await require("./initDb")(config);
            require("./initConsumers")(config, dbconn, opt.path);
            require("./loadfunctions")(path.join(__dirname, 'functions'))
        } catch (error) {
            logger.error(`ConsumerManager: Error occurred : ${(error.stack) ? error.stack : error}`);
            process.exit(1);
        }
    }
};
