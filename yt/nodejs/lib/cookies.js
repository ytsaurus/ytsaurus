var _CACHED_REGEXPS = {};

function _compile_cookie_pattern(name)
{
    if (!_CACHED_REGEXPS[name]) {
        _CACHED_REGEXPS[name] = new RegExp("(?:^|;) *" + name + "=([^;]*)");
    }
    return _CACHED_REGEXPS[name];
}

function Cookies(req, rsp)
{
    this.req = req;
    this.rsp = rsp;
}

Cookies.prototype.get = function(name)
{
    var header = this.req.headers["cookie"];
    if (!header) { return; }

    var match = header.match(_compile_cookie_pattern(name));
    if (!match) { return; }

    return match[1];
}

Cookies.prototype.set = function(name, value, options)
{
    var headers = res.getHeader("Set-Cookie");
    if (typeof(headers) === "undefined") {
        headers = [];
    } else if (typeof(headers) === "string") {
        headers = [headers];
    }

    headers = headers
        .filter(function(header) {
            header.indexOf(name + "=") === -1;
        })
        .concat(
            name + "=" + value
            + "; path=/ui/"
            + "; expires=Wed, 01 Jul 2020 00:00:00 GMT"
            + "; domain=yt.yandex.net"
        );

    rsp.setHeader("Set-Cookie", headers);
    return this;
}

exports.that = Cookies;
