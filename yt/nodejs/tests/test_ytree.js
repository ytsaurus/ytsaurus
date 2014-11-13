var binding = require("../lib/ytnode");

////////////////////////////////////////////////////////////////////////////////

[
    [ "v8 to yson", function(value) {
        return binding.CreateV8Node(value).Print();
    } ],
    [ "json to yson", function(value) {
        return (new binding.TNodeWrap(
            JSON.stringify(value),
            binding.ECompression_None,
            binding.CreateV8Node("json"))).Print();
    } ]
].forEach(function(pair) {
    var suite = pair[0];
    var toYson = pair[1];

    describe(suite, function() {
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
            toYson("\u0080").should.eql('"\\x80"');

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
});

describe("yson conversion specifics", function() {
    it("should properly convert i64 via our internals", function() {
        function generateVariants() {
            var s1 = "\"key\":5000000000";
            var s2 = "\"min\":-9223372036854775807";
            var s3 = "\"max\":9223372036854775807";
            function j(a, b, c) {
                return "{" + a + "," + b + "," + c + "}";
            };
            return [
                j(s1, s2, s3), j(s2, s3, s1), j(s3, s1, s2),
                j(s1, s3, s2), j(s3, s2, s1), j(s2, s1, s3)
            ];
        };
        var variants = generateVariants();
        var node = new binding.TNodeWrap(
            variants[0],
            binding.ECompression_None,
            binding.CreateV8Node("json"));
        node.GetByYPath("/key").Print().should.eql("5000000000");
        node.GetByYPath("/min").Print().should.eql("-9223372036854775807");
        node.GetByYPath("/max").Print().should.eql( "9223372036854775807");
        expect(variants).to.include(node.Print(
            binding.ECompression_None,
            binding.CreateV8Node("json")));
    });

    it("should properly pass strings back and forth", function() {
        var node = new binding.TNodeWrap(
            "{\"a\":\"hello\",\"b\":\"world\"}",
            binding.ECompression_None,
            binding.CreateV8Node("json"));
        node.GetByYPath("/a").Get().should.eql("hello");
        node.GetByYPath("/b").Get().should.eql("world");
    });

    it("should properly pass integers back and forth", function() {
        var node = new binding.TNodeWrap(
            "{\"a\":0,\"b\":2147483647,\"c\":-2147483648}",
            binding.ECompression_None,
            binding.CreateV8Node("json"));
        node.GetByYPath("/a").Get().should.eql(0);
        node.GetByYPath("/b").Get().should.eql(2147483647);
        node.GetByYPath("/c").Get().should.eql(-2147483648);
    });

    it("should properly pass lists back and forth", function() {
        var node = new binding.TNodeWrap(
            "[13,\"hello\",42,\"world\"]",
            binding.ECompression_None,
            binding.CreateV8Node("json"));
        node.Get().should.eql([13, "hello", 42, "world"]);
    });

    it("should properly pass lists back and forth", function() {
        var node = new binding.TNodeWrap(
            "{\"a\":0,\"b\":1}",
            binding.ECompression_None,
            binding.CreateV8Node("json"));
        node.Get().should.eql({ a : 0, b : 1 });
    });
});

describe("ytree & ypath", function() {
    it("should throw exception on non-existing key", function() {
        var node = binding.CreateV8Node({ a: 1 });
        expect(function() {
            node.GetByYPath("/nonexistent");
        }).to.throw(require("../lib/error").that);
        // Apparently, Error is not enough.
    });
    it("should expose setter", function() {
        var node_a = binding.CreateV8Node({ b: 1, c: 2 });
        var node_b = binding.CreateV8Node("foo");
        var node_c = binding.CreateV8Node("bar");
        var result = node_a.SetByYPath("/b", node_b).SetByYPath("/c", node_c);
        result.GetByYPath("/b").Print().should.eql("\"foo\"");
        result.GetByYPath("/c").Print().should.eql("\"bar\"");
        result.Get().should.eql({ b: "foo", c: "bar" });
    });
});

describe("ytree merging", function() {
    it("should properly merge disjoint key sets", function() {
        var node_a = binding.CreateV8Node({ a: 1, b: 2 });
        var node_b = binding.CreateV8Node({ c: 3, d: 4 });
        var node_c = binding.CreateV8Node({ e: 5, f: 6 });
        var result = binding.CreateMergedNode(node_a, node_b, node_c);
        result.GetByYPath("/a").Print().should.eql("1");
        result.GetByYPath("/b").Print().should.eql("2");
        result.GetByYPath("/c").Print().should.eql("3");
        result.GetByYPath("/d").Print().should.eql("4");
        result.GetByYPath("/e").Print().should.eql("5");
        result.GetByYPath("/f").Print().should.eql("6");
    });
    it("should properly merge overlapping key sets", function() {
        var node_a = binding.CreateV8Node({ a: 1, b: 2 });
        var node_b = binding.CreateV8Node({ b: 3, c: 4 });
        var node_c = binding.CreateV8Node({ c: 5, d: 6 });
        var result = binding.CreateMergedNode(node_a, node_b, node_c);
        result.GetByYPath("/a").Print().should.eql("1");
        result.GetByYPath("/b").Print().should.eql("3");
        result.GetByYPath("/c").Print().should.eql("5");
        result.GetByYPath("/d").Print().should.eql("6");
    });
});

