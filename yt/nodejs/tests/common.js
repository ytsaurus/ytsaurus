global.sinon  = require("sinon");
global.chai   = require("chai");

global.should = chai.should();
global.expect = chai.expect;

var sinonChai = require("sinon-chai");
chai.use(sinonChai);
