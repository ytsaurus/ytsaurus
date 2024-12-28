import jinja2
from pathlib import Path

from library.python import resource

from .model import Snapshot


_TEMPLATES = [
    "objects-inl.h",
    "objects.cpp",
    "objects.h",
    "public.h",
    "snapshot.cpp",
    "snapshot.h",
    "ya.make",
]


def render_snapshot(
    snapshot: Snapshot,
    output_dir: Path,
    autogen_marker: str = "AUTOMATICALLY GENERATED. DO NOT EDIT!",
) -> list[Path]:
    env = jinja2.Environment(
        keep_trailing_newline=True,
        lstrip_blocks=True,
        undefined=jinja2.StrictUndefined,
        loader=jinja2.FunctionLoader(_load_template),
    )

    return [_render_template(env, snapshot, output_dir, template, autogen_marker) for template in _TEMPLATES]


def _render_template(
    env: jinja2.Environment,
    snapshot: Snapshot,
    output_dir: Path,
    template_name: str,
    autogen_marker: str,
):
    output_path = output_dir / template_name
    template = env.get_template(template_name)
    result = template.render(snapshot=snapshot, autogen_marker=autogen_marker)

    with output_path.open("w") as file:
        file.write(result)

    return output_path


def _load_template(name: str) -> str:
    content = resource.find(_template_path(name))
    assert content, f"unknown template {name}"
    return content.decode("utf-8")


def _template_path(name: str) -> str:
    return f"/yt/yt/orm/library/snapshot/codegen/templates/{name}.j2"
