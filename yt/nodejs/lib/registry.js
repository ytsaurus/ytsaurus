////////////////////////////////////////////////////////////////////////////////

var __DBG = require("./debug").that("R", "Registry");

////////////////////////////////////////////////////////////////////////////////

function YtRegistry()
{
    "use strict";
    __DBG("New");
    this.registry = {};
}

YtRegistry.prototype.get = function()
{
    "use strict";
    var path = Array.prototype.slice.call(arguments);
    var context = this.registry;
    for (var i = 0; i < path.length; ++i) {
        context = context && context[path[i]];
    }
    return context;
};

YtRegistry.prototype.has = function()
{
    "use strict";
    return typeof(this.get.apply(this, arguments)) !== "undefined";
};

YtRegistry.prototype.set = function(name, instance)
{
    "use strict";
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
    "use strict";
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
    "use strict";
    __DBG("Reset");
    this.registry = {};
};

// This is a singleton object.
exports.that = new YtRegistry;
