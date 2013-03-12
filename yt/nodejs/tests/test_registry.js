var YtRegistry = require("../lib/registry").that;

describe("YtRegistry", function() {
    beforeEach(function() {
        this.test_service = { x : 1 };
    });

    afterEach(function() {
        this.test_service = undefined;
    });

    it("should work properly", function() {
        expect(YtRegistry.get("test_service")).to.be.undefined;
        YtRegistry.register("test_service", this.test_service);
        expect(YtRegistry.get("test_service")).not.to.be.undefined;
        YtRegistry.get("test_service").should.eql(this.test_service);

        YtRegistry.get("test_service").x.should.eql(1);
        ++this.test_service.x;
        YtRegistry.get("test_service").x.should.eql(2);

        expect(YtRegistry.get("test_service")).not.to.be.undefined;
        YtRegistry.unregister("test_service");
        expect(YtRegistry.get("test_service")).to.be.undefined;
    });

    it("should support jspath", function() {
        YtRegistry.register("test_service", this.test_service);
        YtRegistry.query("test_service", ".x").should.eql([1]);
        ++this.test_service.x;
        YtRegistry.query("test_service", ".x").should.eql([2]);
        YtRegistry.unregister("test_service");
    });
});
