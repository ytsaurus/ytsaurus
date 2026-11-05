"""Python companion entry point mirroring the C++ #THttpGetFunction example.

Fetches the configured URL with the companion-hosted HTTP(S) client and emits
the response status code per input message.
"""

from yt.yt.flow.library.python.companion import Pipeline


def http_get(message, output, ctx):
    if ctx.parameters.get("use_https_client"):
        client = ctx.https_client
    else:
        client = ctx.http_client
    response = client.get(ctx.parameters["url"])

    builder = ctx.message_builder("responses")
    builder.set("key", message.payload["key"])
    builder.set("status_code", response.status_code)
    output.add_message(builder.finish())


def main():
    pipeline = Pipeline()
    pipeline.add("http-get", http_get)
    pipeline.run()


if __name__ == "__main__":
    main()
