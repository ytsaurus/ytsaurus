from __future__ import annotations

from typing import Any, Dict, Optional, Type, TypeVar, Union

from rich.color import Color
from rich.console import Console, ConsoleRenderable, Group, RenderableType
from rich.text import Text
from rich.theme import Theme
from typing_extensions import Literal

from rich_toolkit.button import Button
from rich_toolkit.container import Container
from rich_toolkit.element import CursorOffset, Element
from rich_toolkit.input import Input
from rich_toolkit.menu import Menu
from rich_toolkit.progress import Progress, ProgressLine
from rich_toolkit.spacer import Spacer
from rich_toolkit.utils.colors import (
    fade_text,
    get_terminal_background_color,
    get_terminal_text_color,
    lighten,
)

ConsoleRenderableClass = TypeVar(
    "ConsoleRenderableClass", bound=Type[ConsoleRenderable]
)


class BaseStyle:
    brightness_multiplier = 0.1

    base_theme = {
        "tag.title": "bold",
        "tag": "bold",
        "text": "#ffffff",
        "selected": "green",
        "result": "white",
        "progress": "on #893AE3",
        "error": "red",
        "cancelled": "red",
        # is there a way to make nested styles?
        # like label.active uses active style if not set?
        "active": "green",
        "title.error": "white",
        "title.cancelled": "white",
        "placeholder": "grey62",
        "placeholder.cancelled": "grey62 strike",
    }

    _should_show_progress_title = True

    def __init__(
        self,
        theme: Optional[Dict[str, str]] = None,
        background_color: str = "#000000",
        text_color: str = "#FFFFFF",
    ):
        self.background_color = get_terminal_background_color(background_color)
        self.text_color = get_terminal_text_color(text_color)
        self.animation_counter = 0

        base_theme = Theme(self.base_theme)
        self.console = Console(theme=base_theme)

        if theme:
            self.console.push_theme(Theme(theme))

    def empty_line(self) -> RenderableType:
        return " "

    def _get_animation_colors(
        self,
        steps: int = 5,
        breathe: bool = False,
        animation_status: Literal["started", "stopped", "error"] = "started",
        **metadata: Any,
    ) -> list[Color]:
        animated = animation_status == "started"

        if animation_status == "error":
            base_color = self.console.get_style("error").color

            if base_color is None:
                base_color = Color.parse("red")

        else:
            base_color = self.console.get_style("progress").bgcolor

        if not base_color:
            base_color = Color.from_rgb(255, 255, 255)

        if breathe:
            steps = steps // 2

        if animated and base_color.triplet is not None:
            colors = [
                lighten(base_color, self.brightness_multiplier * i)
                for i in range(0, steps)
            ]

        else:
            colors = [base_color] * steps

        if breathe:
            colors = colors + colors[::-1]

        return colors

    def get_cursor_offset_for_element(
        self, element: Element, parent: Optional[Element] = None
    ) -> CursorOffset:
        return element.cursor_offset

    def render_element(
        self,
        element: Any,
        is_active: bool = False,
        done: bool = False,
        parent: Optional[Element] = None,
        **kwargs: Any,
    ) -> RenderableType:
        if isinstance(element, str):
            return self.render_string(element, is_active, done, parent)
        elif isinstance(element, Button):
            return self.render_button(element, is_active, done, parent)
        elif isinstance(element, Container):
            return self.render_container(element, is_active, done, parent)
        elif isinstance(element, Input):
            return self.render_input(element, is_active, done, parent)
        elif isinstance(element, Menu):
            return self.render_menu(element, is_active, done, parent)
        elif isinstance(element, Progress):
            self.animation_counter += 1

            return self.render_progress(element, is_active, done, parent)
        elif isinstance(element, ProgressLine):
            return self.render_progress_log_line(
                element.text,
                parent=parent,
                index=kwargs.get("index", 0),
                max_lines=kwargs.get("max_lines", -1),
                total_lines=kwargs.get("total_lines", -1),
            )
        elif isinstance(element, Spacer):
            return self.render_spacer()
        elif isinstance(element, ConsoleRenderable):
            return element

        raise ValueError(f"Unknown element type: {type(element)}")

    def render_string(
        self,
        string: str,
        is_active: bool = False,
        done: bool = False,
        parent: Optional[Element] = None,
    ) -> RenderableType:
        return string

    def render_button(
        self,
        element: Button,
        is_active: bool = False,
        done: bool = False,
        parent: Optional[Element] = None,
    ) -> RenderableType:
        style = "black on blue" if is_active else "white on black"
        return Text(f" {element.label} ", style=style)

    def render_spacer(self) -> RenderableType:
        return ""

    def render_container(
        self,
        container: Container,
        is_active: bool = False,
        done: bool = False,
        parent: Optional[Element] = None,
    ) -> RenderableType:
        content = []

        for i, element in enumerate(container.elements):
            content.append(
                self.render_element(
                    element,
                    is_active=i == container.active_element_index,
                    done=done,
                    parent=container,
                )
            )

        return Group(*content, "\n" if not done else "")

    def render_input(
        self,
        element: Input,
        is_active: bool = False,
        done: bool = False,
        parent: Optional[Element] = None,
    ) -> RenderableType:
        label = self.render_input_label(element, is_active=is_active, parent=parent)
        text = self.render_input_value(
            element, is_active=is_active, parent=parent, done=done
        )

        contents = []

        if element.inline or done:
            if done and element.password:
                text = "*" * len(element.text)
            if label:
                text = f"{label} {text}"

            contents.append(text)
        else:
            if label:
                contents.append(label)

            contents.append(text)

        if validation_message := self.render_validation_message(element):
            contents.append(validation_message)

        # TODO: do we need this?
        element._height = len(contents)

        return Group(*contents)

    def render_validation_message(self, element: Union[Input, Menu]) -> Optional[str]:
        if element._cancelled:
            return "[cancelled]Cancelled.[/]"

        if element.valid is False:
            return f"[error]{element.validation_message}[/]"

        return None

    # TODO: maybe don't reuse this for menus
    def render_input_value(
        self,
        input: Union[Menu, Input],
        is_active: bool = False,
        parent: Optional[Element] = None,
        done: bool = False,
    ) -> RenderableType:
        text = input.text

        if not text:
            placeholder = ""

            if isinstance(input, Input):
                placeholder = input.placeholder

                if input.password:
                    text = "*" * len(input.text)
                else:
                    if input.default_as_placeholder and input.default:
                        return f"[placeholder]{input.default}[/]"

            if input._cancelled:
                return f"[placeholder.cancelled]{placeholder}[/]"
            elif not done:
                return f"[placeholder]{placeholder}[/]"

        return f"[text]{text}[/]"

    def render_input_label(
        self,
        input: Union[Input, Menu],
        is_active: bool = False,
        parent: Optional[Element] = None,
    ) -> Union[str, Text, None]:
        from rich_toolkit.form import Form

        label: Union[str, Text, None] = None

        if input.label:
            label = input.label

            if isinstance(parent, Form):
                if is_active:
                    label = f"[active]{label}[/]"
                elif input.valid is False:
                    label = f"[error]{label}[/]"

        return label

    def render_menu(
        self,
        element: Menu,
        is_active: bool = False,
        done: bool = False,
        parent: Optional[Element] = None,
    ) -> RenderableType:
        menu = Text(justify="left")

        selected_prefix = Text(element.current_selection_char + " ")
        not_selected_prefix = Text(element.selection_char + " ")

        separator = Text("\t" if element.inline else "\n")

        if done:
            result_content = Text()

            result_content.append(
                self.render_input_label(element, is_active=is_active, parent=parent)
            )
            result_content.append(" ")

            result_content.append(
                element.options[element.selected]["name"],
                style=self.console.get_style("result"),
            )

            return result_content

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

        content: list[RenderableType] = []

        content.append(self.render_input_label(element))

        content.extend(filter)
        content.append(menu)

        if message := self.render_validation_message(element):
            content.append(Text(""))
            content.append(message)

        return Group(*content)

    def render_progress(
        self,
        element: Progress,
        is_active: bool = False,
        done: bool = False,
        parent: Optional[Element] = None,
    ) -> RenderableType:
        content: str | Group | Text = element.current_message

        if element.logs and element._inline_logs:
            lines_to_show = (
                element.logs[-element.lines_to_show :]
                if element.lines_to_show > 0
                else element.logs
            )

            start_content = [element.title, ""]

            if not self._should_show_progress_title:
                start_content = []

            content = Group(
                *start_content,
                *[
                    self.render_element(
                        line,
                        index=index,
                        max_lines=element.lines_to_show,
                        total_lines=len(element.logs),
                        parent=element,
                    )
                    for index, line in enumerate(lines_to_show)
                ],
            )

        return content

    def render_progress_log_line(
        self,
        line: str | Text,
        index: int,
        max_lines: int = -1,
        total_lines: int = -1,
        parent: Optional[Element] = None,
    ) -> Text:
        line = Text.from_markup(line) if isinstance(line, str) else line
        if max_lines == -1:
            return line

        shown_lines = min(total_lines, max_lines)

        # this is the minimum brightness based on the max_lines
        min_brightness = 0.4
        # but we want to have a slightly higher brightness if there's less than max_lines
        # otherwise you could get the something like this:

        # line 1 -> very dark
        # line 2 -> slightly darker
        # line 3 -> normal

        # which is ok, but not great, so we we increase the brightness if there's less than max_lines
        # so that the last line is always the brightest
        current_min_brightness = min_brightness + abs(shown_lines - max_lines) * 0.1
        current_min_brightness = min(max(current_min_brightness, min_brightness), 1.0)

        brightness_multiplier = ((index + 1) / shown_lines) * (
            1.0 - current_min_brightness
        ) + current_min_brightness

        return fade_text(
            line,
            text_color=Color.parse(self.text_color),
            background_color=self.background_color,
            brightness_multiplier=brightness_multiplier,
        )
