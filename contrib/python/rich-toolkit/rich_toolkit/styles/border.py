from typing import Any, Optional, Tuple, Union

from rich import box
from rich.color import Color
from rich.console import Group, RenderableType
from rich.style import Style
from rich.text import Text

from rich_toolkit._rich_components import Panel
from rich_toolkit.container import Container
from rich_toolkit.element import CursorOffset, Element
from rich_toolkit.form import Form
from rich_toolkit.input import Input
from rich_toolkit.menu import Menu
from rich_toolkit.progress import Progress

from .base import BaseStyle


class BorderedStyle(BaseStyle):
    box = box.SQUARE

    def empty_line(self) -> RenderableType:
        return ""

    def _box(
        self,
        content: RenderableType,
        title: Union[str, Text, None],
        is_active: bool,
        border_color: Color,
        after: Tuple[str, ...] = (),
    ) -> RenderableType:
        return Group(
            Panel(
                content,
                title=title,
                title_align="left",
                highlight=is_active,
                width=50,
                box=self.box,
                border_style=Style(color=border_color),
            ),
            *after,
        )

    def render_container(
        self,
        element: Container,
        is_active: bool = False,
        done: bool = False,
        parent: Optional[Element] = None,
    ) -> RenderableType:
        content = super().render_container(element, is_active, done, parent)

        if isinstance(element, Form):
            return self._box(content, element.title, is_active, Color.parse("white"))

        return content

    def render_input(
        self,
        element: Input,
        is_active: bool = False,
        done: bool = False,
        parent: Optional[Element] = None,
        **metadata: Any,
    ) -> RenderableType:
        validation_message: Tuple[str, ...] = ()

        if isinstance(parent, Form):
            return super().render_input(element, is_active, done, parent, **metadata)

        if message := self.render_validation_message(element):
            validation_message = (message,)

        if element.valid is False:
            border_color = self.console.get_style("error").color or Color.parse("red")

        title = self.render_input_label(
            element,
            is_active=is_active,
            parent=parent,
        )

        border_color = Color.parse("white")

        return self._box(
            self.render_input_value(element, is_active=is_active, parent=parent),
            title,
            is_active,
            border_color,
            after=validation_message,
        )

    def render_menu(
        self,
        element: Menu,
        is_active: bool = False,
        done: bool = False,
        parent: Optional[Element] = None,
        **metadata: Any,
    ) -> RenderableType:
        validation_message: Tuple[str, ...] = ()

        menu = Text(justify="left")

        selected_prefix = Text(element.current_selection_char + " ")
        not_selected_prefix = Text(element.selection_char + " ")

        separator = Text("\t" if element.inline else "\n")

        content: list[RenderableType] = []

        if done:
            content.append(
                Text(
                    element.options[element.selected]["name"],
                    style=self.console.get_style("result"),
                )
            )

        else:
            for id_, option in enumerate(element.options):
                if id_ == element.selected:
                    prefix = selected_prefix
                    style = self.console.get_style("selected")
                else:
                    prefix = not_selected_prefix
                    style = self.console.get_style("text")

                is_last = id_ == len(element.options) - 1

                menu.append(
                    Text.assemble(
                        prefix,
                        option["name"],
                        separator if not is_last else "",
                        style=style,
                    )
                )

            if not element.options:
                menu = Text("No results found", style=self.console.get_style("text"))

            filter = (
                [
                    Text.assemble(
                        (element.filter_prompt, self.console.get_style("text")),
                        (element.text, self.console.get_style("text")),
                        "\n",
                    )
                ]
                if element.allow_filtering
                else []
            )

            content.extend(filter)
            content.append(menu)

            if message := self.render_validation_message(element):
                validation_message = (message,)

        result = Group(*content)

        return self._box(
            result,
            self.render_input_label(element),
            is_active,
            Color.parse("white"),
            after=validation_message,
        )

    def render_progress(
        self,
        element: Progress,
        is_active: bool = False,
        done: bool = False,
        parent: Optional[Element] = None,
    ) -> RenderableType:
        content: str | Group | Text = element.current_message
        title: Union[str, Text, None] = None

        title = element.title

        if element.logs and element._inline_logs:
            lines_to_show = (
                element.logs[-element.lines_to_show :]
                if element.lines_to_show > 0
                else element.logs
            )

            content = Group(
                *[
                    self.render_element(
                        line,
                        index=index,
                        max_lines=element.lines_to_show,
                        total_lines=len(element.logs),
                    )
                    for index, line in enumerate(lines_to_show)
                ]
            )

        border_color = Color.parse("white")

        if not done:
            colors = self._get_animation_colors(
                steps=10, animation_status="started", breathe=True
            )

            border_color = colors[self.animation_counter % 10]

        return self._box(content, title, is_active, border_color=border_color)

    def get_cursor_offset_for_element(
        self, element: Element, parent: Optional[Element] = None
    ) -> CursorOffset:
        top_offset = element.cursor_offset.top
        left_offset = element.cursor_offset.left + 2

        if isinstance(element, Input) and element.inline:
            # we don't support inline inputs yet in border style
            top_offset += 1
            inline_left_offset = (len(element.label) - 1) if element.label else 0
            left_offset = element.cursor_offset.left - inline_left_offset

        if isinstance(parent, Form):
            top_offset += 1

        return CursorOffset(top=top_offset, left=left_offset)
