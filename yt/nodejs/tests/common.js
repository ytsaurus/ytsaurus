global.sinon  = require("sinon");
global.chai   = require("chai");

global.should = chai.should();
global.expect = chai.expect;

global.HTTP_PORT = 40000;
global.HTTP_HOST = "localhost";

var sinonChai = require("sinon-chai");
chai.use(sinonChai);
