const { createLogger, transports, format } = require('winston');
const priceLogger = createLogger({
  transports: [
    new transports.File({
      filename: "logs/price-error.log",
      level: "error",
      format: format.combine(
        format.json(),
        format.timestamp(),
        format.prettyPrint()
      )
    }),
    new transports.File({
      filename: "logs/price.log",
      level: "info",
      format: format.combine(
        format.json(),
        format.timestamp(),
        format.prettyPrint()
      ),
    })
  ]
});
module.exports = priceLogger;