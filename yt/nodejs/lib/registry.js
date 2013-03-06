var __DBG = require("./debug").that("R", "Registry");

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

exports.that = new YtRegistry;
