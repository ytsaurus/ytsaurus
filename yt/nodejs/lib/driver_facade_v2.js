var binding = require("./ytnode");

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

var TRUE_NODE = binding.CreateV8Node(true);
var FALSE_NODE = binding.CreateV8Node(false);

function clone(object) {
    if (object === null || typeof(object) !== "object") {
       return object;
    }
    var result = object.constructor();
    for (var p in object) {
        if (object.hasOwnProperty(p)) {
            result[p] = object[p];
        }
    }
    return result;
}

function YtDriverFacadeV2(driver)
{
    if (!(this instanceof YtDriverFacadeV2)) {
        return new YtDriverFacadeV2(driver);
    }

    var mapping = {
        "read": "read_table",
        "write": "write_table",
        "download": "read_file",
        "upload": "write_file"
    };

    var descriptors = {};

    driver.get_command_descriptors().forEach(function(item) {
        descriptors[item.name] = item;
    });

    // Make aliases for old commands.

    for (var p in mapping) {
        if (mapping.hasOwnProperty(p)) {
            descriptors[p] = clone(descriptors[mapping[p]]);
            descriptors[p].name = p;
            Object.defineProperty(
                descriptors[p],
                "input_type_as_integer",
                { enumerable: false, value: descriptors[mapping[p]].input_type_as_integer });
            Object.defineProperty(
                descriptors[p],
                "output_type_as_integer",
                { enumerable: false, value: descriptors[mapping[p]].output_type_as_integer });
        }
    }

    // Remove new commands that are not part of V2.

    delete descriptors.read_file;
    delete descriptors.read_journal;
    delete descriptors.read_table;

    delete descriptors.write_file;
    delete descriptors.write_journal;
    delete descriptors.write_table;

    delete descriptors.mount_table;
    delete descriptors.unmount_table;
    delete descriptors.remount_table;
    delete descriptors.reshard_table;

    delete descriptors.delete_rows;
    delete descriptors.insert_rows;
    delete descriptors.lookup_rows;
    delete descriptors.select_rows;
    delete descriptors.select;

    delete descriptors.dump_job_context;
    delete descriptors.strace_job;

    this.driver = driver;
    this.mapping = mapping;
    this.descriptors = descriptors;
}

YtDriverFacadeV2.prototype.execute = function(name, user,
    input_stream, input_compression,
    output_stream, output_compression,
    parameters, request_id, pause,
    response_parameters_consumer,
    result_interceptor)
{
    if (typeof(this.mapping[name]) !== "undefined") {
        name = this.mapping[name];
    }

    parameters.GetByYPath("/input_format").SetAttribute("boolean_as_string", TRUE_NODE);
    parameters.GetByYPath("/output_format").SetAttribute("boolean_as_string", TRUE_NODE);

    return this.driver.execute(
        name, user,
        input_stream, input_compression,
        output_stream, output_compression,
        parameters, request_id, pause,
        response_parameters_consumer,
        result_interceptor);
};

YtDriverFacadeV2.prototype.find_command_descriptor = function(name)
{
    return this.descriptors[name] || null;
};

YtDriverFacadeV2.prototype.get_command_descriptors = function()
{
    return Object.values(this.descriptors);
};

////////////////////////////////////////////////////////////////////////////////

exports.that = YtDriverFacadeV2;
