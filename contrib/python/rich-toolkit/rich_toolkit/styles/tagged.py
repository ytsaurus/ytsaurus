import re
from typing import Any, Dict, List, Optional, Tuple

from rich.console import Group, RenderableType
from rich.segment import Segment
from rich.style import Style
from rich.table import Column, Table
from typing_extensions import Literal

from rich_toolkit.container import Container
from rich_toolkit.element import CursorOffset, Element
from rich_toolkit.progress import Progress, ProgressLine

from .base import BaseStyle


def has_emoji(tag: str) -> bool:
    return bool(re.search(r"[\U0001F300-\U0001F9FF]", tag))


class TaggedStyle(BaseStyle):
    block = "█"
    block_length = 5

    def __init__(self, tag_width: int = 12, theme: Optional[Dict[str, str]] = None):
        self.tag_width = tag_width

        theme = theme or {
            "tag.title": "bold",
            "tag": "bold",
        }

        super().__init__(theme=theme)

    def _get_tag_segments(
        self,
        metadata: Dict[str, Any],
        is_animated: bool = False,
        done: bool = False,
    ) -> Tuple[List[Segment], int]:
        if tag := metadata.get("tag", ""):
            tag = f" {tag} "

        style_name = "tag.title" if metadata.get("title", False) else "tag"

        style = self.console.get_style(style_name)

        if is_animated:
            animation_status: Literal["started", "stopped", "error"] = (
                "started" if not done else "stopped"
            )

            tag = " " * self.block_length
            colors = self._get_animation_colors(
                steps=self.block_length, animation_status=animation_status
            )

            if done:
                colors = [colors[-1]]

            tag_segments = [
                Segment(
                    self.block,
                    style=Style(
                        color=colors[(self.animation_counter + i) % len(colors)]
                    ),
                )
                for i in range(self.block_length)
            ]
        else:
            tag_segments = [Segment(tag, style=style)]

        left_padding = self.tag_width - len(tag)
        left_padding = max(0, left_padding)

        return tag_segments, left_padding

    def _get_tag(
        self,
        metadata: Dict[str, Any],
        is_animated: bool = False,
        done: bool = False,
    ) -> Group:
        tag_segments, left_padding = self._get_tag_segments(metadata, is_animated, done)

        left = [Segment(" " * left_padding), *tag_segments]

        return Group(*left)

    def _tag_element(
        self,
        child: RenderableType,
        is_animated: bool = False,
        done: bool = False,
        **metadata: Dict[str, Any],
    ) -> RenderableType:
        table = Table.grid(
            # TODO: why do we add 2? :D we probably did this in the previous version
            Column(width=self.tag_width + 2, no_wrap=True),
            Column(no_wrap=False, overflow="fold"),
            padding=(0, 0, 0, 0),
            collapse_padding=True,
            pad_edge=False,
        )

        table.add_row(self._get_tag(metadata, is_animated, done), Group(child))

        return table

    def render_element(
        self,
        element: Any,
        is_active: bool = False,
        done: bool = False,
        parent: Optional[Element] = None,
        **kwargs: Any,
    ) -> RenderableType:
        is_animated = isinstance(element, Progress)
        should_tag = not isinstance(element, (ProgressLine, Container))

        rendered = super().render_element(
            element=element, is_active=is_active, done=done, parent=parent, **kwargs
        )

        metadata = kwargs
        if isinstance(element, Element) and element.metadata:
            metadata = {**element.metadata, **metadata}

        if should_tag:
            rendered = self._tag_element(
                rendered,
                is_animated=is_animated,
                done=done,
                **metadata,
            )

        return rendered

    def get_cursor_offset_for_element(
        self, element: Element, parent: Optional[Element] = None
    ) -> CursorOffset:
        return CursorOffset(
            top=element.cursor_offset.top,
            left=self.tag_width + element.cursor_offset.left + 2,
        )
