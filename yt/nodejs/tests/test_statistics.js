var YtStatistics = require("../lib/statistics").that;

describe("YtStatistics", function() {
    it("should properly aggregate counters", function() {
        var profiler = new YtStatistics();
        profiler.inc("foo", undefined, 1);
        profiler.inc("foo", null, 1);
        profiler.inc("foo", {}, 1);
        expect(profiler.dump()).to.eql("foo 3");
        profiler.inc("foo", {a: 1}, 1);
        profiler.inc("foo", {a: 2}, 1);
        expect(profiler.dump()).to.eql("foo 3\nfoo a=1 1\nfoo a=2 1");
        profiler.inc("bar", {a: 1}, 0);
        expect(profiler.dump()).to.eql("bar a=1 0\nfoo 3\nfoo a=1 1\nfoo a=2 1");
    });

    it("should support negative values in counters", function() {
        var profiler = new YtStatistics();
        profiler.inc("foo", {}, -1);
        expect(profiler.dump()).to.eql("foo 18446744073709551615");
        profiler.inc("foo", {}, 1);
        expect(profiler.dump()).to.eql("foo 0");
        profiler.inc("foo", {}, 1);
        expect(profiler.dump()).to.eql("foo 1");
        profiler.inc("foo", {}, -1);
        expect(profiler.dump()).to.eql("foo 0");
    });

    it("should properly aggregate digests", function() {
        var profiler = new YtStatistics();
        var i;
        for (i = 0; i < 5; ++i) {
            profiler.upd("zzz", {}, 0.99);
        }
        for (i = 0; i < 15; ++i) {
            profiler.upd("zzz", {}, 1.01);
        }
        expect(profiler.dump()).to.eql("zzz.q50 1.005\nzzz.q90 1.01\nzzz.q95 1.01\nzzz.q99 1.01\nzzz.max 1.01");
    });

    it("should produce Solomon-compatible dump", function() {
        var profiler = new YtStatistics();
        var i;
        for (i = 0; i < 5; ++i) {
            profiler.upd("zzz", {}, 0.99);
        }
        for (i = 0; i < 15; ++i) {
            profiler.upd("zzz", {}, 1.01);
        }

        profiler.inc("foo", {}, 1);
        profiler.set("bar", {a: "7"}, 1);

        expect(profiler.dump()).to.eql("bar a=7 1\nfoo 1\nzzz.q50 1.005\nzzz.q90 1.01\nzzz.q95 1.01\nzzz.q99 1.01\nzzz.max 1.01");
        expect(JSON.parse(profiler.dumpSolomon())).to.eql(
            {"sensors":[
                {"labels":{"sensor":"bar","a":"7"},"value":1},
                {"labels":{"sensor":"foo"},"value":"1","mode":"deriv"},
                {"labels":{"sensor":"zzz.q50"},"value":1.005},
                {"labels":{"sensor":"zzz.q90"},"value":1.01},
                {"labels":{"sensor":"zzz.q95"},"value":1.01},
                {"labels":{"sensor":"zzz.q99"},"value":1.01},
                {"labels":{"sensor":"zzz.max"},"value":1.01}]});
    });

    it("should properly save last gauge value", function() {
        var profiler = new YtStatistics();
        profiler.set("foo", {}, 1);
        profiler.set("foo", {}, 2);
        profiler.set("foo", {}, 3);
        expect(profiler.dump()).to.eql("foo 3");
    });

    it("should properly merge counters", function() {
        var profiler = new YtStatistics();
        var lhs = new YtStatistics();
        var rhs = new YtStatistics();

        lhs.inc("foo", {a: 1}, 10);
        lhs.inc("foo", {a: 2}, 20);
        lhs.inc("bar", null, 1);
        lhs.inc("xxx", null, -1);
        rhs.inc("foo", {a: 1}, 10);
        rhs.inc("foo", {a: 3}, 20);
        rhs.inc("baz", null, 1);
        rhs.inc("xxx", null, -1);

        expect(profiler.dump()).to.eql("");

        lhs.mergeTo(profiler);
        rhs.mergeTo(profiler);

        expect(lhs.dump()).to.eql("");
        expect(rhs.dump()).to.eql("");

        expect(profiler.dump()).to.eql("bar 1\nbaz 1\nfoo a=1 20\nfoo a=2 20\nfoo a=3 20\nxxx 18446744073709551614");
    });

    it("should properly merge digests", function() {
        var profiler = new YtStatistics();
        var lhs = new YtStatistics();
        var rhs = new YtStatistics();

        for (var i = 0; i < 15; ++i) {
            lhs.upd("zzz", {a: 1}, 1.0);
            rhs.upd("zzz", {a: 1}, 2.0);
        }

        expect(profiler.dump()).to.eql("");

        lhs.mergeTo(profiler);
        rhs.mergeTo(profiler);

        expect(lhs.dump()).to.eql("");
        expect(rhs.dump()).to.eql("");

        expect(profiler.dump()).to.eql("zzz.q50 a=1 1.5\nzzz.q90 a=1 2\nzzz.q95 a=1 2\nzzz.q99 a=1 2\nzzz.max a=1 2");
    });

    it("should properly merge gauges", function() {
        var profiler = new YtStatistics();
        var lhs = new YtStatistics();
        var rhs = new YtStatistics();

        lhs.set("foo", {a: 1}, 1);
        lhs.set("foo", {a: 2}, 2);
        rhs.set("foo", {a: 2}, 3);
        rhs.set("foo", {a: 3}, 4);

        expect(profiler.dump()).to.eql("");

        lhs.mergeTo(profiler);
        rhs.mergeTo(profiler);

        expect(lhs.dump()).to.eql("");
        expect(rhs.dump()).to.eql("");

        expect(profiler.dump()).to.eql("foo a=1 1\nfoo a=2 3\nfoo a=3 4");
    });

    it("should expire old entries", function(done) {
        var profiler = new YtStatistics(100);

        profiler.inc("foo", null, 1);
        setTimeout(function() {
            profiler.clearExpiredGauges();
            if (profiler.dump() === "") {
                done();
            } else {
                done(new Error(":("));
            }
        }, 200);
    });
});

