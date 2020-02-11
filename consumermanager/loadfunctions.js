const path = require("path");
const fs = require("fs");

global.funclib = {};

module.exports = foldername => {
  let funcmodules = fs.readdirSync(foldername);
  for (let i = 0; i < funcmodules.length; i++) {
    if (path.extname(funcmodules[i]) == ".js") {
      let filepath = path.join(foldername, funcmodules[i]);
      require(filepath)({
        add(name, funccode) {
          funclib[name] = funccode;
        }
      });
    }
  }
};
