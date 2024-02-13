from dataclasses import fields
from dacite import from_dict

import jinja2
import argparse
import yaml
import os
import pathlib

try:
    from yt.record_codegen_helpers import Manifest
except ImportError:
    # Doing this because open-source build forced my hand.
    from record_codegen_helpers import Manifest


def get_template(name):
    try:
        from library.python import resource
        content = resource.find(name)
    except ImportError:
        template_path = os.path.join(os.path.dirname(__file__), "templates", name)
        with open(template_path, "rb") as fin:
            content = fin.read()
    env = jinja2.Environment(keep_trailing_newline=True, undefined=jinja2.StrictUndefined)
    return env.template_class.from_code(env, env.compile(content.decode("utf-8"), filename=name), env.globals, None)


def render_template(name, context, output):
    template = get_template(name)
    context_dict = {field.name: getattr(context, field.name, field.default) for field in fields(context)}
    content = template.render(**context_dict)
    output.write(content)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Name of input YAML")
    parser.add_argument("--output-root", required=True, help="Path to generated directory root")
    parser.add_argument("--output-cpp", required=True, help="Path to generated .cpp file")
    parser.add_argument("--output-include", action="append", help="Files to include in output .h file")
    args = parser.parse_args()

    with open(args.input) as input_file:
        manifest_dict = yaml.safe_load(input_file)
        manifest = from_dict(Manifest, manifest_dict)
        output_h = os.path.splitext(args.output_cpp)[0] + ".h"
        manifest.h_path = str(pathlib.Path(output_h).relative_to(args.output_root))
        if not manifest.includes:
            manifest.includes = []
        if args.output_include is not None:
            manifest.includes.extend(args.output_include)
        with open(output_h, "w") as output_file:
            render_template("h.j2", manifest, output_file)
        with open(args.output_cpp, "w") as output_file:
            render_template("cpp.j2", manifest, output_file)


if __name__ == "__main__":
    main()
