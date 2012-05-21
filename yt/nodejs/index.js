var application = require("./lib/application");

for (var key in application) {
    exports[key] = application[key];
}

