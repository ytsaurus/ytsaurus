var binding = require("../lib/ytnode");

////////////////////////////////////////////////////////////////////////////////

[
    [ "v8 to yson", function(value) {
        return (binding.CreateV8Node(value)).Print();
    } ],
    [ "json to yson", function(value) {
        return (new binding.TNodeJSNode(
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

            // XXX(sandello): Fix 'v8 to yson' one day.
            if (suite !== "v8 to yson") {
                toYson("&").should.eql("\"\"");
                toYson("&Jg==").should.eql("\"&\"");
                toYson("&JiY=").should.eql("\"&&\"");
                toYson("&JmhlbGxv").should.eql("\"&hello\"");
                toYson("&Jndvcmxk").should.eql("\"&world\"");
                toYson("&aGVsbG8=").should.eql("\"hello\"");
                toYson("&d29ybGQ=").should.eql("\"world\"");
            }

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

describe("conversion specifics", function() {
    it("should properly convert i64 via our internals", function() {
        var node = new binding.TNodeJSNode(
            "{\"key\":5000000000,\"min\":-9223372036854775807,\"max\":9223372036854775807}",
            binding.ECompression_None,
            binding.CreateV8Node("json"));
        node.Get("/key").Print().should.eql("5000000000");
        node.Get("/min").Print().should.eql("-9223372036854775807");
        node.Get("/max").Print().should.eql( "9223372036854775807");
    });
});

describe("merging", function() {
    it("should properly merge disjoint key sets", function() {
        var node_a = new binding.TNodeJSNode({ a: 1, b: 2});
        var node_b = new binding.TNodeJSNode({ c: 3, d: 4});
        var node_c = new binding.TNodeJSNode({ e: 5, f: 6});
        var result = binding.CreateMergedNode(node_a, node_b, node_c);
        result.Get("/a").Print().should.eql("1");
        result.Get("/b").Print().should.eql("2");
        result.Get("/c").Print().should.eql("3");
        result.Get("/d").Print().should.eql("4");
        result.Get("/e").Print().should.eql("5");
        result.Get("/f").Print().should.eql("6");
    });
    it("should properly merge overlapping key sets", function() {
        var node_a = new binding.TNodeJSNode({ a: 1, b: 2});
        var node_b = new binding.TNodeJSNode({ b: 3, c: 4});
        var node_c = new binding.TNodeJSNode({ c: 5, d: 6});
        var result = binding.CreateMergedNode(node_a, node_b, node_c);
        result.Get("/a").Print().should.eql("1");
        result.Get("/b").Print().should.eql("3");
        result.Get("/c").Print().should.eql("5");
        result.Get("/d").Print().should.eql("6");
    });
});
