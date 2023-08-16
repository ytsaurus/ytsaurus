import argparse
import pprint
import warnings

import lib.spec
import lib.runner
import yt.yt.experiments.new_stress_test.check_runner as check_runner

import yt.wrapper as yt
import yt.yson as yson


def main():
    warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)

    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    with open(args.config, "rb") as f:
        config = yson.load(f)

    raw_spec = lib.spec.spec_from_yson(config["preset"], config["mixins"], config["spec"])
    specs = lib.spec.variate_modes(raw_spec)
    print(len(specs))
    assert len(specs) == 1
    spec = specs[0][0]
    pprint.pprint(spec)

    lib.runner.configure_client(yt)

    if "pool" in config:
        yt.config["spec_defaults"]["pool"] = config["pool"]

    def callback(path):
        class MockArgs():
            force = False
        lib.runner.run_with_spec(path, lib.spec.Spec(spec), MockArgs())

    check_runner.run_and_track_success(
        callback,
        config["path"],
        config["timeout"],
        config.get("transaction_title"),
        config.get("expiration_timeout"))


if __name__ == "__main__":
    main()
