var expect = require("chai").expect;
var assert = require("chai").assert;

var ybinding = require("./build/Release/yt_streams");
var tbinding = require("./build/Release/test_streams");
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

var binding = require("./build/Release/yt_streams");

var expect = require("chai").expect;
var assert = require("chai").assert;

////////////////////////////////////////////////////////////////////////////////

describe("input stream interface", function() {
    beforeEach(function() {
        this.stream = new ybinding.TNodeJSInputStream();
        this.reader = new tbinding.TTestInputStream(this.stream);
    });

    it("should be able to read whole input byte-by-byte", function() {
        this.stream.Push(new Buffer("foo"), 0, 3);
        this.stream.Push(new Buffer("bar"), 0, 3);

        expect(this.reader.ReadSynchronously(1)).to.be.a("string").and.to.eql("f");
        expect(this.reader.ReadSynchronously(1)).to.be.a("string").and.to.eql("o");
        expect(this.reader.ReadSynchronously(1)).to.be.a("string").and.to.eql("o");
        expect(this.reader.ReadSynchronously(1)).to.be.a("string").and.to.eql("b");
        expect(this.reader.ReadSynchronously(1)).to.be.a("string").and.to.eql("a");
        expect(this.reader.ReadSynchronously(1)).to.be.a("string").and.to.eql("r");
    });

    it("should be able to read whole input at-a-time", function() {
        this.stream.Push(new Buffer("foo"), 0, 3);
        this.stream.Push(new Buffer("bar"), 0, 3);

        expect(this.reader.ReadSynchronously(6))
            .to.be.a("string").and.to.eql("foobar");
    });

    it("should be able to return if there is any data", function() {
        this.stream.Push(new Buffer("foo"), 0, 3);
        this.stream.Push(new Buffer("bar"), 0, 3);

        expect(this.reader.ReadSynchronously(1000))
            .to.be.a("string").and.to.eql("foobar");
    });

    it("should wait for new data while not closed", function(done) {
        this.stream.Push(new Buffer("foo"), 0, 3);
        this.stream.Push(new Buffer("bar"), 0, 3);

        var add_new_data = (function() {
            this.stream.Push(new Buffer("12345"), 0, 5);
        }).bind(this);
        var sweep_stream = (function() {
            this.stream.Sweep();
        }).bind(this);
        var close_stream = (function() {
            this.stream.Close();
        }).bind(this);

        setTimeout(add_new_data, 100);
        setTimeout(sweep_stream, 150);
        setTimeout(close_stream, 200);

        expect(this.reader.ReadSynchronously(3)).to.be.a("string").and.to.eql("foo");
        expect(this.reader.ReadSynchronously(3)).to.be.a("string").and.to.eql("bar");

        this.reader.Read(3, (function(length, buffer) {
            expect(length).to.be.eql(3);
            expect(buffer).to.be.equal("123");

            this.reader.Read(3, (function(length, buffer) {
                expect(length).to.be.eql(2);
                expect(buffer).to.be.equal("45");

                this.reader.Read(3, (function(length, buffer) {
                    expect(length).to.be.eql(0);
                    expect(buffer).to.be.empty;

                    done();
                }).bind(this));
            }).bind(this));
        }).bind(this));
    });

    it("should be able to read whole input if the stream was closed", function(done) {
        this.stream.Push(new Buffer("foo"), 0, 3);
        this.stream.Push(new Buffer("bar"), 0, 3);
        this.stream.Close();

        expect(this.reader.ReadSynchronously(2))
            .to.be.a("string").and.to.eql("fo");
        expect(this.reader.ReadSynchronously(2))
            .to.be.a("string").and.to.eql("ob");
        expect(this.reader.ReadSynchronously(2))
            .to.be.a("string").and.to.eql("ar");
        expect(this.reader.ReadSynchronously(2))
            .to.be.a("string").and.to.be.empty;
        expect(this.reader.ReadSynchronously(2))
            .to.be.a("string").and.to.be.empty;

        this.reader.Read(100, (function(length, buffer) {
            expect(length).to.be.eql(0);
            expect(buffer).to.be.empty;
            done();
        }).bind(this));
    });
});

describe("output stream interface", function() {
    beforeEach(function() {
        this.stream = new ybinding.TNodeJSOutputStream();
        this.writer = new tbinding.TTestOutputStream(this.stream);

        this.stream.on_write  = function(){};
        this.stream.on_flush  = function(){};
        this.stream.on_finish = function(){};
    });

    it("should be able to write one chunk", function(done) {
        this.stream.on_write = (function(chunk) {
            expect(chunk.toString()).to.be.equal("hello");
            done();
        }).bind(this);

        this.writer.WriteSynchronously("hello");
    });

    it("should be able to write many chunks", function(done) {
        this.stream.on_write_calls = 0;
        this.stream.on_write = (function(chunk) {
            switch (this.stream.on_write_calls) {
                case 0:
                    expect(chunk.toString()).to.be.equal("hello");
                    break;
                case 1:
                    expect(chunk.toString()).to.be.equal("dolly");
                    break;
            }
            if (++this.stream.on_write_calls > 1) {
                done();
            }
        }).bind(this);

        this.writer.WriteSynchronously("hello");
        this.writer.WriteSynchronously("dolly");
    });

    it("should fire Flush() callback", function(done) {
        this.stream.on_flush = function() { done(); };
        this.writer.Flush();
    });

    it("should fire Finish() callback", function(done) {
        this.stream.on_finish = function() { done(); };
        this.writer.Finish();
    });
});

describe("high-level streams", function() {
    it("should play nice together", function(done) {
        var readable = new yt_streams.YtReadableStream();
        var writable = new yt_streams.YtWritableStream();

        var reader = new tbinding.TTestInputStream(writable._binding);
        var writer = new tbinding.TTestOutputStream(readable._binding);

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