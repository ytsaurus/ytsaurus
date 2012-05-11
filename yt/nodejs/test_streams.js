var expect = require("chai").expect;
var assert = require("chai").assert;

var binding = require("./build/Release/yt_test");
var yt_streams = require("./yt_streams");

////////////////////////////////////////////////////////////////////////////////

function TraceUVTicks(n) {
    var k = n;
    function TraceUVTick() {
        if (k > 0) {
            console.error("=== UV TICK ====================================================================");
            process.nextTick(TraceUVTick);
            --k;
        }
        if (k == 0) {
            console.error("=== UV TICK (hiding...) ========================================================");
        }
    }
    TraceUVTick();
}

// TraceUVTicks(24);

////////////////////////////////////////////////////////////////////////////////

describe("high-level streams", function() {
    it("should play nice together", function(done) {
        var readable = new yt_streams.YtReadableStream();
        var writable = new yt_streams.YtWritableStream();

        var reader = new binding.TTestInputStream(writable._binding);
        var writer = new binding.TTestOutputStream(readable._binding);

        readable.pipe(writable);

        writer.WriteSynchronously("hello");
        writer.WriteSynchronously(" ");
        writer.WriteSynchronously("dolly");
        writer.Finish();

        process.nextTick(function() {
            expect(reader.ReadSynchronously(1000)).to.be.equal("hello dolly");
            done();
        })
    });
});