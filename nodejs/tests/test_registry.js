var YtRegistry = require("../lib/registry").that;

describe("YtRegistry", function() {
    beforeEach(function() {
        this.test_service = { x : 1, y : { foo : "bar" } };
    });

    afterEach(function() {
        this.test_service = undefined;
    });

    it("should work properly", function() {
        expect(YtRegistry.get("test_service")).to.be.undefined;
        YtRegistry.set("test_service", this.test_service);
        expect(YtRegistry.get("test_service")).not.to.be.undefined;
        YtRegistry.get("test_service").should.eql(this.test_service);

        YtRegistry.get("test_service").x.should.eql(1);
        ++this.test_service.x;
        YtRegistry.get("test_service").x.should.eql(2);

        expect(YtRegistry.get("test_service")).not.to.be.undefined;
        YtRegistry.del("test_service");
        expect(YtRegistry.get("test_service")).to.be.undefined;
    });

    it("should support nested gets", function() {
        YtRegistry.set("test_service", this.test_service);
        expect(YtRegistry.get("test_service", "y", "foo")).to.eql("bar");
        expect(YtRegistry.get("test_service", "y", "bar")).to.be.undefined;
        expect(YtRegistry.get("test_service", "?", "?"  )).to.be.undefined;
        YtRegistry.del("test_service");
    });

    it("should support clear", function() {
        YtRegistry.set("test_service", this.test_service);
        expect(YtRegistry.get("test_service")).not.to.be.undefined;
        YtRegistry.clear();
        expect(YtRegistry.get("test_service")).to.be.undefined;
    });
});
