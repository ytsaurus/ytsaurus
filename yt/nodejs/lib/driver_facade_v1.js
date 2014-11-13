var binding = require("./ytnode");
var utils = require("./utils");

////////////////////////////////////////////////////////////////////////////////

// Mock internals.
var FAKE_UPLOAD_DESCRIPTOR = {
    name: "upload",
    input_type: "binary",
    output_type: "structured",
    is_volatile: true,
    is_heavy: true
};

Object.defineProperty(
    FAKE_UPLOAD_DESCRIPTOR,
    "input_type_as_integer",
    { enumerable: false, value: binding.EDataType_Binary });
Object.defineProperty(
    FAKE_UPLOAD_DESCRIPTOR,
    "output_type_as_integer",
    { enumerable: false, value: binding.EDataType_Structured });

var FAKE_UPLOAD_PREPARAMETERS = binding.CreateV8Node({ type: "file" });

function YtDriverFacadeV1(driver)
{
    "use strict";

    // Create an implicit wrapper..
    if (!(this instanceof YtDriverFacadeV1)) {
        return new YtDriverFacadeV1(driver);
    }

    this.driver = driver;
}

YtDriverFacadeV1.prototype.execute = function(name, user,
    input_stream, input_compression,
    output_stream, output_compression,
    parameters, pause, response_parameters_consumer)
{
    "use strict";

    // We only have to deal with upload.
    if (name !== "upload") {
        return this.driver.execute.apply(this.driver, arguments);
    }

    var self = this;

    var slack_input = new utils.MemoryInputStream();
    var slack_pause = utils.Pause(slack_input);
    var slack_output_create = new utils.MemoryOutputStream();
    var slack_output_upload = new utils.MemoryOutputStream();
    var slack_bytes;

    return self.driver.execute(
        "create", user,
        slack_input, binding.ECompression_None,
        slack_output_create, output_compression,
        binding.CreateMergedNode(parameters, FAKE_UPLOAD_PREPARAMETERS),
        slack_pause,
        response_parameters_consumer)
    .spread(function(result, bytes_in, bytes_out) {
        slack_bytes = bytes_out;

        return self.driver.execute(
            "upload", user,
            input_stream, input_compression,
            slack_output_upload, output_compression,
            parameters, pause, response_parameters_consumer);
    })
    .spread(function(result, bytes_in, bytes_out) {
        for (var i = 0; i < slack_output_create.chunks.length; ++i) {
            output_stream.write(slack_output_create.chunks[i]);
        }

        return [ result, bytes_in, slack_bytes ];
    });
};

YtDriverFacadeV1.prototype.find_command_descriptor = function(name)
{
    "use strict";

    if (name !== "upload") {
        return this.driver.find_command_descriptor.apply(this.driver, arguments);
    } else {
        return FAKE_UPLOAD_DESCRIPTOR;
    }
};

YtDriverFacadeV1.prototype.get_command_descriptors = function()
{
    "use strict";

    return this.driver
        .get_command_descriptors.apply(this.driver, arguments)
        .map(function(descriptor) {
            if (descriptor.name !== "upload") {
                return descriptor;
            } else {
                return FAKE_UPLOAD_DESCRIPTOR;
            }
        });
};

////////////////////////////////////////////////////////////////////////////////

exports.that = YtDriverFacadeV1;
