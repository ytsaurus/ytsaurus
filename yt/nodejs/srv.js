var http = require("http");

var srv;

srv = http.createServer(function(req, rsp) {
    rsp.on('data', function(x) {
        console.log("CHUNK: " + x);
    });
    rsp.writeHead(200, {'Content-Type': 'text/plain'});
    rsp.end('Voila!');
});

srv.listen(8080, '127.0.0.1');