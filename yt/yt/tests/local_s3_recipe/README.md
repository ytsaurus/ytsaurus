# Local S3 Server

This directory contains everything you need to test S3 functionality in an indedependent environment. It's based on the `s3proxy` program, taken as a binary from this Github repo's releases: https://github.com/gaul/s3proxy. There are two options to use it:
1. Launch independently [downloading s3proxy](https://github.com/gaul/s3proxy/releases/download/s3proxy-2.6.0/s3proxy) and invoking `./s3proxy --properties s3proxy.conf`; the `s3proxy.conf` example can be found as a string in `__main__.py`
2. Use as a recipe in your test by adding `INCLUDE(${ARCADIA_ROOT}/yt/yt/tests/local_s3_recipe/recipe.inc)` into `ya.make`

If you use it as a recipe, you can configure the setup by setting one or more of the following variables right before the `INCLUDE` statement in your `ya.make` file:
- `SET(AWS_ENDPOINT_URL http://mys3.com:8080)`
- `SET(AWS_ACCESS_KEY_ID foobar)`
- `SET(AWS_SECRET_ACCESS_KEY barfoo)`

The region variables (`AWS_REGION` or `AWS_DEFAULT_REGION`) are ignored by `s3proxy` but can be used by some other code as environment variables.
