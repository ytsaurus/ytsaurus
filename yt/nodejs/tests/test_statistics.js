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
});

