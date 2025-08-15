from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from rich.console import Console, RenderableType
from rich.theme import Theme

from .input import Input
from .menu import Menu, Option, ReturnValue
from .progress import Progress
from .styles.base import BaseStyle


class RichToolkitTheme:
    def __init__(self, style: BaseStyle, theme: Dict[str, str]) -> None:
        self.style = style
        self.rich_theme = Theme(theme)


class RichToolkit:
    def __init__(
        self,
        style: Optional[BaseStyle] = None,
        theme: Optional[RichToolkitTheme] = None,
        handle_keyboard_interrupts: bool = True,
    ) -> None:
        # TODO: deprecate this

        self.theme = theme
        if theme is not None:
            self.style = theme.style
            self.style.theme = theme.rich_theme
            self.style.console = Console(theme=theme.rich_theme)
        else:
            assert style is not None

            self.style = style

        self.console = self.style.console

        self.handle_keyboard_interrupts = handle_keyboard_interrupts

    def __enter__(self):
        self.console.print()
        return self

    def __exit__(
        self, exc_type: Any, exc_value: Any, traceback: Any
    ) -> Union[bool, None]:
        if self.handle_keyboard_interrupts and exc_type is KeyboardInterrupt:
            # we want to handle keyboard interrupts gracefully, instead of showing a traceback
            # or any other error message
            return True

        self.console.print()

        return None

    def print_title(self, title: str, **metadata: Any) -> None:
        self.console.print(self.style.render_element(title, title=True, **metadata))

    def print(self, *renderables: RenderableType, **metadata: Any) -> None:
        self.console.print(
            *[
                self.style.render_element(renderable, **metadata)
                for renderable in renderables
            ]
        )

    def print_as_string(self, *renderables: RenderableType, **metadata: Any) -> str:
        with self.console.capture() as capture:
            self.print(*renderables, **metadata)

        return capture.get().rstrip()

    def print_line(self) -> None:
        self.console.print(self.style.empty_line())

    def confirm(self, label: str, **metadata: Any) -> bool:
        return self.ask(
            label=label,
            options=[
                Option({"value": True, "name": "Yes"}),
                Option({"value": False, "name": "No"}),
            ],
            inline=True,
            **metadata,
        )

    def ask(
        self,
        label: str,
        options: List[Option[ReturnValue]],
        inline: bool = False,
        allow_filtering: bool = False,
        **metadata: Any,
    ) -> ReturnValue:
        return Menu(
            label=label,
            options=options,
            console=self.console,
            style=self.style,
            inline=inline,
            allow_filtering=allow_filtering,
            **metadata,
        ).ask()

    def input(
        self,
        title: str,
        default: str = "",
        placeholder: str = "",
        password: bool = False,
        required: bool = False,
        required_message: str = "",
        inline: bool = False,
        **metadata: Any,
    ) -> str:
        return Input(
            name=title,
            label=title,
            default=default,
            placeholder=placeholder,
            password=password,
            required=required,
            required_message=required_message,
            inline=inline,
            style=self.style,
            **metadata,
        ).ask()

    def progress(
        self,
        title: str,
        transient: bool = False,
        transient_on_error: bool = False,
        inline_logs: bool = False,
        lines_to_show: int = -1,
    ) -> Progress:
        return Progress(
            title=title,
            console=self.console,
            style=self.style,
            transient=transient,
            transient_on_error=transient_on_error,
            inline_logs=inline_logs,
            lines_to_show=lines_to_show,
        )
