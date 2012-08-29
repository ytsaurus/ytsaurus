var binding = require("../lib/ytnode");

////////////////////////////////////////////////////////////////////////////////

describe("v8 to yson", function() {
    var toYson = binding.GetYsonRepresentation;

    it("should not translate null", function() {
        (function() { toYson(null) }).should.throw(TypeError);
    });

    it("should properly translate numbers", function() {
        toYson(0).should.eql("0");
        toYson(1).should.eql("1");
        toYson(1.25).should.eql("1.25");

        toYson({ $value : 0 }).should.eql("0");
        toYson({ $value : 1 }).should.eql("1");
        toYson({ $value : 1.25 }).should.eql("1.25");
    });

    it("should properly translate strings", function() {
        toYson("hello").should.eql('"hello"');
        toYson("world").should.eql('"world"');
        toYson("a\"'hell'\"").should.eql('"a\\"\'hell\'\\""');

        // TODO(sandello): Fixme.
        // (toYson("&")).to.eql("&Jg==");
        // (toYson("&&")).to.eql("&JiY=");
        // (toYson("&hello")).to.eql("&JmhlbGxv");
        // (toYson("&world")).to.eql("&Jndvcmxk");

        toYson({ $value : "hello" }).should.eql('"hello"');
        toYson({ $value : "world" }).should.eql('"world"');
    });

    it("should properly translate lists", function() {
        toYson([])
            .should.eql('[]');
        toYson([[]])
            .should.eql('[[]]');
        toYson([ 1, "hello", 2, "world" ])
            .should.eql('[1;"hello";2;"world"]');
        toYson([[1],[[2]],[[3],[4]],[]])
            .should.eql('[[1];[[2]];[[3];[4]];[]]');
    });

    it("should properly translate maps", function() {
        expect([
            '{"bar"="xyz";"foo"=1}',
            '{"foo"=1;"bar"="xyz"}'
        ]).to.include(
            toYson({ foo : 1, bar : "xyz" })
        );
    });

    it("should support attributes", function() {
        toYson({
            $value : "yson",
            $attributes : { format : "pretty", enable_raw : "true" }
        }).should.eql('<"enable_raw"="true";"format"="pretty">"yson"');

        toYson({
            $attributes : {
                foo : {
                    $attributes : {
                        another_foo : "another_bar"
                    },
                    $value : "bar"
                }
            },
            $value : "some_string"
        }).should.eql('<"foo"=<"another_foo"="another_bar">"bar">"some_string"');
    });
});

