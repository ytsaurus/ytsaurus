////////////////////////////////////////////////////////////////////////////////

var __DBG = require("./debug").that("R", "Registry");

////////////////////////////////////////////////////////////////////////////////

function YtRegistry()
{
    __DBG("New");
    this.registry = {};
}

YtRegistry.prototype.get = function()
{
    var path = Array.prototype.slice.call(arguments);
    var context = this.registry;
    for (var i = 0; i < path.length; ++i) {
        context = context && context[path[i]];
    }
    return context;
};

YtRegistry.prototype.has = function()
{
    return typeof(this.get.apply(this, arguments)) !== "undefined";
};

YtRegistry.prototype.set = function(name, instance)
{
    if (this.registry.hasOwnProperty(name)) {
        throw new Error(
            "An instance under name '" + name + "' is already registered.");
    } else {
        __DBG("Registering '" + name + "'");
        this.registry[name] = instance;
    }
};

YtRegistry.prototype.del = function(name)
{
    if (!this.registry.hasOwnProperty(name)) {
        throw new Error(
            "An instance under name '" + name + "' is not registered yet.");
    } else {
        __DBG("Unregistering '" + name + "'");
        delete this.registry[name];
    }
};

YtRegistry.prototype.clear = function()
{
    __DBG("Reset");
    this.registry = {};
};

// This is a singleton object.
exports.that = new YtRegistry;
