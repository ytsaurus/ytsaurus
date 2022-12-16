from library.python import resource
from dataclasses import dataclass, fields
from dacite import from_dict
from typing import Optional, List

import jinja2
import argparse
import yaml
import os


@dataclass
class Field:
    cpp_name: str
    cpp_type: str
    column_name: str
    column_type: str
    sort_order: Optional[str]


@dataclass
class RecordType:
    type_name: str
    fields: List[Field]
    record_verbatim: Optional[str]
    key_verbatim: Optional[str]
    descriptor_verbatim: Optional[str]


@dataclass
class Manifest:
    namespace: str
    types: List[RecordType]
    h_verbatim: Optional[str]
    cpp_verbatim: Optional[str]
    h_path: Optional[str]


def get_template(name):
    content = resource.find(name)
    env = jinja2.Environment(keep_trailing_newline=True, undefined=jinja2.StrictUndefined)
    return env.template_class.from_code(env, env.compile(content.decode("utf-8"), filename=name), env.globals, None)


def render_template(name, context, output):
    template = get_template(name)
    context_dict = {field.name: getattr(context, field.name, field.default) for field in fields(context)}
    content = template.render(**context_dict)
    output.write(content)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--arcadia-root", required=True, help="Path to Arcadia source root")
    parser.add_argument("--input-root", required=True, help="Path to input root")
    parser.add_argument("--input", required=True, help="Name of input YAML")
    parser.add_argument("--output-root", required=True, help="Path to generated files root")
    args = parser.parse_args()

    with open(args.input) as input_file:
        manifest_dict = yaml.safe_load(input_file)
        manifest = from_dict(Manifest, manifest_dict)
        relative_input = os.path.relpath(os.path.splitext(args.input)[0], args.input_root)
        manifest.h_path = os.path.relpath(os.path.splitext(args.input)[0] + ".record.h", args.arcadia_root)
        with open(args.output_root + "/" + relative_input + ".record.h", "w") as output_file:
            render_template("h.j2", manifest, output_file)
        with open(args.output_root + "/" + relative_input + ".record.cpp", "w") as output_file:
            render_template("cpp.j2", manifest, output_file)


if __name__ == "__main__":
    main()
