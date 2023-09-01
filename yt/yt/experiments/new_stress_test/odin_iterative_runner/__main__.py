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
    parser.add_argument("--preset", type=str)
    parser.add_argument("--mixin-overrides", type=str, nargs="*")
    parser.add_argument("--spec-patch", type=lambda x: yson.loads(x.encode()))
    args = parser.parse_args()

    with open(args.config, "rb") as f:
        config = yson.load(f)

    raw_spec = lib.spec.spec_from_yson(
        args.preset or config["preset"],
        config["mixins"] if args.mixin_overrides is None else args.mixin_overrides,
        config["spec"])
    if args.spec_patch:
        raw_spec = lib.spec.merge_specs(raw_spec, args.spec_patch)

    specs = lib.spec.variate_modes(raw_spec)
    assert len(specs) == 1, "Variating specs are not allowed"
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
        config.get("failure_expiration_timeout"),
        config.get("success_expiration_timeout", config.get("expiration_timeout")))


if __name__ == "__main__":
    main()
