var yt = require('./build/Release/yt_test');

if (1) {
    var x = new yt.DriverHost();
    x.write_to_input(new Buffer("foo"), 0, 3);
    x.write_to_input(new Buffer("bar"), 0, 3);

    x.test(1);
    x.test(1);
    x.test(1);
    x.test(1);
    x.test(1);
    x.test(1);
}

if (1) {
    var x = new yt.DriverHost();
    var y = x;

    x.write_to_input(new Buffer("foo"), 0, 3);
    x.write_to_input(new Buffer("bar"), 0, 3);

    setTimeout(function() { y.write_to_input(new Buffer("12345"), 0, 5); }, 100);
    setTimeout(function() { y.close_input(); }, 200);

    x.test(3);
    x.test(3);

    x.test(4);
    x.test(4);
    x.test(4);
    x.test(4);
    x.test(4);
}

if (1) {
    var x = new yt.DriverHost();

    x.write_to_input(new Buffer("12345"), 0, 5);
    x.close_input();

    x.test(5);
    x.test(5);
    x.test(5);
}

//x.foo();
