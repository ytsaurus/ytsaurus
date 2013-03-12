var __DBG = require("./debug").that("R", "Registry");

var jspath = require("jspath");

function YtRegistry() {
    __DBG("New");
    this.registry = {};
}

YtRegistry.prototype.register = function(name, instance) {
    if (this.registry.hasOwnProperty(name)) {
        throw new Error("An instance under name '" + name + "' is already registered.");
    } else {
        __DBG("Registering '" + name + "'");
        this.registry[name] = instance;
    }
};

YtRegistry.prototype.unregister = function(name) {
    if (!this.registry.hasOwnProperty(name)) {
        throw new Error("An instance under name '" + name + "' is not registered yet.");
    } else {
        __DBG("Unregistering '" + name + "'");
        delete this.registry[name];
    }
};

YtRegistry.prototype.get = function(name) {
    return this.registry[name];
};

YtRegistry.prototype.query = function(name, query, context) {
    return jspath.apply(query, this.registry[name], context);
};

exports.that = new YtRegistry;
