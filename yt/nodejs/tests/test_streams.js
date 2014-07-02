var zlib = require("zlib");

var buffertools = require("buffertools");

var binding = require("../lib/ytnode");
process.on("exit", binding.ShutdownSingletons);

var YtReadableStream = require("../lib/readable_stream.js").that;
var YtWritableStream = require("../lib/writable_stream.js").that;

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

function GenerateString(target_length) {
    var alphabet = "abcdefghijklmnopqrstuvwxyz01234567890";
    var result = "";

    for (var i = 0; i < Math.floor(target_length / alphabet.length); ++i) {
        result += alphabet;
    }

    for (var i = 0; i < target_length % alphabet.length; ++i) {
        result += alphabet.charAt(i % alphabet.length);
    }

    return result;
}

// TraceUVTicks(1);

////////////////////////////////////////////////////////////////////////////////

describe("input stream interface", function() {
    beforeEach(function() {
        this.stream = new binding.TInputStreamWrap(100, 1000);
        this.reader = new binding.TInputStreamStub();
        this.reader.Reset(this.stream);

        this.stream.on_drain = sinon.spy();
    });

    afterEach(function() {
        this.stream = null;
        this.reader = null;

        if (this.clock) {
            this.clock.restore();
        }
    });

    it("should set low_watermark and high_watermark properties", function() {
        this.stream.low_watermark.should.eql(100);
        this.stream.high_watermark.should.eql(1000);
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
        var add_new_data = (function() {
            this.stream.Push(new Buffer("12345"), 0, 5);
        }).bind(this);
        var close_stream = (function() {
            this.stream.End();
        }).bind(this);

        setTimeout(add_new_data, 20);
        setTimeout(close_stream, 40);

        this.reader.Read(5, (function(error, length, buffer) {
            expect(error.code).to.eql(0);
            expect(length).to.eql(5);
            expect(buffer).to.equal("12345");

            this.reader.Read(5, (function(error, length, buffer) {
                expect(error.code).to.eql(0);
                expect(length).to.eql(0);
                expect(buffer).to.be.empty;

                done();
            }).bind(this));
        }).bind(this));
    });

    it("should wait for new data while not destroyed", function(done) {
        var add_new_data = (function() {
            this.stream.Push(new Buffer("12345"), 0, 5);
        }).bind(this);
        var close_stream = (function() {
            this.stream.Destroy();
        }).bind(this);

        setTimeout(add_new_data, 20);
        setTimeout(close_stream, 40);

        this.reader.Read(5, (function(error, length, buffer) {
            expect(error.code).to.eql(0);
            expect(length).to.eql(5);
            expect(buffer).to.be.equal("12345");

            this.reader.Read(5, (function(error, length, buffer) {
                expect(error.code).not.to.eql(0);

                done();
            }).bind(this));
        }).bind(this));
    });

    it("should not be able to read whole input if the stream was closed", function(done) {
        this.stream.Push(new Buffer("foo"), 0, 3);
        this.stream.Push(new Buffer("bar"), 0, 3);

        expect(this.reader.ReadSynchronously(2))
            .to.be.a("string").and.to.eql("fo");
        expect(this.reader.ReadSynchronously(2))
            .to.be.a("string").and.to.eql("ob");

        this.stream.End();

        expect(this.reader.ReadSynchronously(2))
            .to.be.a("string").and.to.eql("ar");
        expect(this.reader.ReadSynchronously(2))
            .to.be.a("string").and.to.be.empty;
        expect(this.reader.ReadSynchronously(2))
            .to.be.a("string").and.to.be.empty;

        this.reader.Read(100, (function(error, length, buffer) {
            expect(error.code).to.eql(0);
            expect(length).to.eql(0);
            expect(buffer).to.be.empty;
            done();
        }).bind(this));
    });

    it("should not be able to read whole input if the stream was destroyed", function(done) {
        this.stream.Push(new Buffer("foo"), 0, 3);
        this.stream.Push(new Buffer("bar"), 0, 3);

        expect(this.reader.ReadSynchronously(2))
            .to.be.a("string").and.to.eql("fo");
        expect(this.reader.ReadSynchronously(2))
            .to.be.a("string").and.to.eql("ob");

        this.stream.Destroy();

        this.reader.Read(100, (function(error, length, buffer) {
            expect(error.code).not.to.eql(0);

            done();
        }).bind(this));
    });

    it("should properly read sliced buffers", function() {
        var data_1 = new Buffer("foobar");
        var data_2 = data_1.slice(3);

        this.stream.Push(data_1, 0, 6); // foobar
        this.stream.Push(data_1, 3, 3); // bar
        this.stream.Push(data_2, 0, 3); // bar
        this.stream.Push(data_1, 0, 3); // foo
        this.stream.End();

        expect(this.reader.ReadSynchronously(100))
            .to.be.a("string").and.to.eql("foobarbarbarfoo");
    });

    it("should not allow to put more than high watermark");
    it("should fire |on_drain| callback after flushing down to low watermark");

    it("should support 'identity' compression", function() {
        this.reader.AddCompression(binding.ECompression_None);
        this.stream.Push(new Buffer("hello"), 0, 5)
        this.reader.ReadSynchronously(5).should.eql("hello");
    });

    // These are multi-parametric cases.
    [
        [ "gzip",    binding.ECompression_Gzip,    zlib.gzip    ],
        [ "deflate", binding.ECompression_Deflate, zlib.deflate ],
    ].forEach(function(test_setting) {
        [ 
            [ "empty string",  ""                              ],
            [ "tiny string",   "hello"                         ],
            [ "small string",  GenerateString(8 * 1024)        ],
            [ "medium string", GenerateString(8 * 1024 * 8)    ],
            [ "large string",  GenerateString(8 * 1024 * 1024) ],
        ].forEach(function(test_case) {
            var method      = test_setting[0];
            var compression = test_setting[1];
            var compressor  = test_setting[2];
            var case_name   = test_case[0];
            var case_data   = test_case[1];

            it("should support '" + method + "' compression for " + case_name, function(done) {
                var self = this;

                self.reader.AddCompression(compression);
                compressor(case_data, function(err, compressed) {
                    expect(err).to.be.null;
                    self.stream.Push(compressed, 0, compressed.length);
                    self.stream.End();
                    var decompressed = "";
                    while (true) {
                        var chunk = self.reader.ReadSynchronously(1 * 1024 * 1024);
                        if (chunk.length === 0) {
                            break;
                        }
                        decompressed += chunk;
                    }

                    decompressed.length.should.eql(case_data.length);
                    decompressed.should.be.equal(case_data);
                    done();
                });
            });
        });
    });
});

describe("output stream interface", function() {
    beforeEach(function() {
        this.stream = new binding.TOutputStreamWrap(100, 1000);
        this.writer = new binding.TOutputStreamStub();
        this.writer.Reset(this.stream);

        this.stream.on_data = sinon.spy();
    });

    afterEach(function() {
        this.stream = null;
        this.writer = null;

        if (this.clock) {
            this.clock.restore();
        }
    });

    it("should be able to write one chunk", function(done) {
        this.stream.on_data = (function() {
            var pulled_chunks = this.stream.Pull();
            var chunks = pulled_chunks.filter(function(x) { return !!x; });
            expect(chunks.length).to.be.equal(1);
            expect(chunks[0].toString()).to.be.equal("hello");
            done();
        }).bind(this);

        this.writer.WriteSynchronously("hello");
    });

    it("should be able to write many chunks", function(done) {
        this.stream.on_data = (function() {
            // Since this is an off-V8-scheduled callback all data should be in place.
            var pulled_chunks = this.stream.Pull();
            var chunks = pulled_chunks.filter(function(x) { return !!x; });
            expect(chunks.length).to.be.equal(2);
            expect(chunks[0].toString()).to.be.equal("hello");
            expect(chunks[1].toString()).to.be.equal("dolly");
            done();
        }).bind(this);

        this.writer.WriteSynchronously("hello");
        this.writer.WriteSynchronously("dolly");
    });

    it("should be able to die on request");

    it("should be able to report its emptiness");

    it("should block on buffer overflow");

    // These are multi-parametric cases.
    [
        [ "gzip",    binding.ECompression_Gzip,    zlib.gunzip  ],
        [ "deflate", binding.ECompression_Deflate, zlib.inflate ],
    ].forEach(function(test_setting) {
        [
            [ "empty string",  ""                              ],
            [ "tiny string",   "hello"                         ],
            [ "small string",  GenerateString(8 * 1024)        ],
            [ "medium string", GenerateString(8 * 1024 * 8)    ],
            [ "large string",  GenerateString(8 * 1024 * 1024) ],
        ].forEach(function(test_case) {
            var method        = test_setting[0];
            var decompression = test_setting[1];
            var decompressor  = test_setting[2];
            var case_name     = test_case[0];
            var case_data     = test_case[1];

            it("should support '" + method + "' compression for " + case_name, function(done) {
                var self = this;
                var chunks = [];

                self.stream.on_data = function() {
                    while (true) {
                        var pulled_chunks = self.stream.Pull().filter(function(x) { return !!x; })
                        if (pulled_chunks.length) {
                            chunks = chunks.concat(pulled_chunks);
                        } else {
                            break;
                        }
                    }
                    var compressed = buffertools.concat.apply(undefined, chunks);
                    decompressor(compressed, function(err, decompressed) {
                        expect(err).to.be.null;
                        decompressed.length.should.be.equal(case_data.length);
                        decompressed.toString().should.be.equal(case_data);
                        done();
                    });
                };

                self.writer.AddCompression(decompression);
                self.writer.WriteSynchronously(case_data);
                self.writer.Finish();
            });
        });
    });
});

describe("high-level interoperation", function() {
    it("should properly compress/decompress data");

    it("should play nice together", function(done) {
        var readable = new YtReadableStream(100, 1000);
        var writable = new YtWritableStream(100, 1000);

        var reader = new binding.TInputStreamStub(); reader.Reset(writable._binding);
        var writer = new binding.TOutputStreamStub(); writer.Reset(readable._binding);

        readable.pipe(writable);

        writer.WriteSynchronously("hello");
        writer.WriteSynchronously(" ");
        writer.WriteSynchronously("dolly");
        writer.Finish();

        // Skip two ticks to allow streams pipe content between them.
        process.nextTick(function() {
            process.nextTick(function() {
                reader.Read(1000, function(error, length, buffer) {
                    expect(error.code).to.eql(0);
                    expect(length).to.be.equal(11);
                    expect(buffer).to.be.equal("hello dolly");
                    done();
                });
            });
        });
    });
});
