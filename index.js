const path = require("path");
const consumermgr = require("./consumermanager");

consumermgr.init({
    path: path.join(__dirname, "Handlers") //< --Notable changes
});
                                                                                                                        