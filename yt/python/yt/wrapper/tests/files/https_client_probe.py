"""Connect the yt client to an HTTPS proxy and perform a single request.

Run in a subprocess by test_https.py so each scenario builds a fresh client session against the
chosen proxy configuration. Exits 0 on success, non-zero (with a traceback on stderr) on any
failure, e.g. a TLS verification error.
"""

import argparse

import yt.wrapper as yt


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--proxy-url", required=True)
    parser.add_argument("--use-system-ca", action="store_true")
    parser.add_argument("--ca-bundle-path", default=None)
    args = parser.parse_args()

    client = yt.YtClient(config={
        "backend": "http",
        "proxy": {
            "url": args.proxy_url,
            "enable_proxy_discovery": False,
            "use_system_ca": args.use_system_ca,
            "ca_bundle_path": args.ca_bundle_path,
            # Fail fast: TLS errors are otherwise retried until the request timeout.
            "retries": {"enable": False},
        },
    })
    client.exists("/")


if __name__ == "__main__":
    main()
