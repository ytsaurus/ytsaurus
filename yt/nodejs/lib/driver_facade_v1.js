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

var FAKE_UPLOAD_PREPARAMETERS = new binding.TNodeWrap({ type: "file" });

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
    parameters)
{
    "use strict";

    // We only have to deal with upload.
    if (name !== "upload") {
        return this.driver.execute.apply(this.driver, arguments);
    }

    var self = this;

    var input_pause = utils.Pause(input_stream);
    var output_pause = new utils.MemoryOutputStream();

    var slack_input = new utils.MemoryInputStream();
    var slack_output = new utils.MemoryOutputStream();
    var slack_bytes;

    return self.driver.execute(
        "create", user,
        slack_input, binding.ECompression_None,
        output_pause, output_compression,
        binding.CreateMergedNode(parameters, FAKE_UPLOAD_PREPARAMETERS))
    .spread(function(result, bytes_in, bytes_out) {
        slack_bytes = bytes_out;

        process.nextTick(function() { input_pause.unpause(); input_pause = null; });

        return self.driver.execute(
            "upload", user,
            input_stream, input_compression,
            slack_output, binding.ECompression_None,
            parameters);
    })
    .spread(function(result, bytes_in, bytes_out) {
        for (var chunk in output_pause.chunks) {
            output_stream.write(chunk);
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
}

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
}

////////////////////////////////////////////////////////////////////////////////

exports.that = YtDriverFacadeV1;
