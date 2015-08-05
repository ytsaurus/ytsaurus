var binding = require("./ytnode");

var Q = require("bluebird");

var YtError = require("./error").that;
var YtApplicationVersions = require("./application_versions").that;

////////////////////////////////////////////////////////////////////////////////

if (typeof(Object.values) === "undefined") {
    Object.values = function(object) {
        var result = [];
        for (var p in object) {
            if (object.hasOwnProperty(p)) {
                result.push(object[p]);
            }
        }
        return result;
    };
}

function YtDriverFacadeV3(driver)
{
    if (!(this instanceof YtDriverFacadeV3)) {
        return new YtDriverFacadeV3(driver);
    }

    var custom_commands = {}; 
    function defineCustomCommand(name, callback) 
    { 
        custom_commands[name] = { 
            name: name, 
            input_type: "null", 
            output_type: "structured", 
            is_volatile: false, 
            is_heavy: false
        };
        Object.defineProperty( 
            custom_commands[name], 
            "compression",
            { enumerable: false, value: false });

        Object.defineProperty( 
            custom_commands[name], 
            "input_type_as_integer", 
            { enumerable: false, value: binding.EDataType_Null }); 
 
        Object.defineProperty( 
            custom_commands[name], 
            "output_type_as_integer", 
            { enumerable: false, value: binding.EDataType_Structured }); 
 
        Object.defineProperty( 
            custom_commands[name], 
            "execute", 
            { enumerable: false, value: callback }); 
    }   
 
    var application_versions = new YtApplicationVersions(driver); 
 
    defineCustomCommand("_discover_versions", function(output_stream) { 
        return application_versions.get_versions().then(function(result) { 
            output_stream.write(JSON.stringify(result));
        }); 
    }); 
 
    this.custom_commands = custom_commands;
    this.driver = driver;
}

YtDriverFacadeV3.prototype.execute = function(name, user,
    input_stream, input_compression,
    output_stream, output_compression,
    parameters, request_id, pause, response_parameters_consumer)
{
    if (typeof(this.custom_commands[name]) !== "undefined") {
        return this.custom_commands[name].execute(output_stream, parameters.Get())
        .then(function () {
            return [new YtError(), 0, 0];
        }, function (error) {
            return [error, 0, 0];
        });
    }

    return this.driver.execute(
        name, user,
        input_stream, input_compression,
        output_stream, output_compression,
        parameters, request_id, pause, response_parameters_consumer);
};

YtDriverFacadeV3.prototype.find_command_descriptor = function(name)
{
    return this.custom_commands[name] || this.driver.find_command_descriptor(name);
};

YtDriverFacadeV3.prototype.get_command_descriptors = function()
{
    return []
        .concat(this.driver.get_command_descriptors())
        .concat(Object.values(this.custom_commands));
};

////////////////////////////////////////////////////////////////////////////////

exports.that = YtDriverFacadeV3;
