from jinja2 import Template
from typing import Optional


INT_MAX = 10**9


def render(template_string: str, version: Optional[int]) -> str:
    """Render versioned template string. Use 'None' for the latest version."""
    template = Template(
        source=template_string,
        trim_blocks=True,
        lstrip_blocks=True)
    adjusted_version = version if version is not None else INT_MAX
    return template.render(version=adjusted_version)
