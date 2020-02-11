const env = process.env.NODE_ENV === "development" ? "debug" : "info";
const fs = require("fs");
const logDir = "Logs";
const winston = require("winston");
require("winston-daily-rotate-file");

var logger;

if (!fs.existsSync(logDir)) {
    fs.mkdirSync(logDir);
}

const dailyRotateFileTransport = new winston.transports.DailyRotateFile({
    filename: `${logDir}/%DATE%-result.log`,
    datePattern: 'YYYY-MM-DD',
    maxSize: '10m',
    maxFiles: '3d'
});

logger = winston.createLogger({
    level: env,
    format: winston.format.combine(
        winston.format.timestamp({
            format: "YYYY-MM-DD HH:mm:ss"
        }),
        winston.format.printf(
            info => `${info.timestamp} ${info.level}: ${info.message}`
        )
    ),

    transports: [
        new winston.transports.Console({
            level: "info",
            format: winston.format.combine(
                winston.format.colorize(),
                winston.format.printf(
                    info => `${info.timestamp} ${info.level}: ${info.message}`
                )
            )
        }),
        dailyRotateFileTransport
    ]
});


module.exports = logger