var expect = require("chai").expect;
var assert = require("chai").assert;

var binding = require("ytnode");

describe("v8 to yson", function() {
    it("should not translate null", function() {
        // TODO(sandello): Fixme.
        // expect(binding.DebugFromV8ToYson(null));
    });

    it("should properly translate numbers", function() {
        expect(binding.DebugFromV8ToYson(0)).to.be.eql("0");
        expect(binding.DebugFromV8ToYson(1)).to.be.eql("1");
        expect(binding.DebugFromV8ToYson(1.25)).to.be.eql("1.25");

        expect(binding.DebugFromV8ToYson({ $value : 0 })).to.be.eql("0");
        expect(binding.DebugFromV8ToYson({ $value : 1 })).to.be.eql("1");
        expect(binding.DebugFromV8ToYson({ $value : 1.25 })).to.be.eql("1.25");
    });

    it("should properly translate strings", function() {
        expect(binding.DebugFromV8ToYson("hello")).to.be.eql('"hello"');
        expect(binding.DebugFromV8ToYson("world")).to.be.eql('"world"');
        expect(binding.DebugFromV8ToYson("a\"'hell'\"")).to.be.eql('"a\\"\'hell\'\\""');

        // TODO(sandello): Fixme.
        // expect(binding.DebugFromV8ToYson("&")).to.be.eql("&Jg==");
        // expect(binding.DebugFromV8ToYson("&&")).to.be.eql("&JiY=");
        // expect(binding.DebugFromV8ToYson("&hello")).to.be.eql("&JmhlbGxv");
        // expect(binding.DebugFromV8ToYson("&world")).to.be.eql("&Jndvcmxk");

        expect(binding.DebugFromV8ToYson({ $value : "hello" })).to.be.eql('"hello"');
        expect(binding.DebugFromV8ToYson({ $value : "world" })).to.be.eql('"world"');
    });

    it("should properly translate lists", function() {
        expect(binding.DebugFromV8ToYson([]))
            .to.be.eql('[]');
        expect(binding.DebugFromV8ToYson([[]]))
            .to.be.eql('[[]]');
        expect(binding.DebugFromV8ToYson([ 1, "hello", 2, "world" ]))
            .to.be.eql('[1;"hello";2;"world"]');
        expect(binding.DebugFromV8ToYson([[1],[[2]],[[3],[4]],[]]))
            .to.be.eql('[[1];[[2]];[[3];[4]];[]]');
    });

    it("should properly translate maps", function() {
        expect(binding.DebugFromV8ToYson({ foo : 1, bar : "xyz" }))
            .to.be.eql('{"bar"="xyz";"foo"=1}');
    });

    it("should support attributes", function() {
        expect(binding.DebugFromV8ToYson({
            $value : "yson",
            $attributes : { format : "pretty", enable_raw : "true" }
        })).to.be.eql('<"enable_raw"="true";"format"="pretty">"yson"');

        expect(binding.DebugFromV8ToYson({
            $attributes : {
                foo : {
                    $attributes : {
                        another_foo : "another_bar"
                    },
                    $value : "bar"
                }
            },
            $value : "some_string"
        })).to.be.eql('<"foo"=<"another_foo"="another_bar">"bar">"some_string"');
    });
});

