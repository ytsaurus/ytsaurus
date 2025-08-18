from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

from ._input_handler import TextInputHandler
from .element import CursorOffset, Element

if TYPE_CHECKING:
    from pydantic import TypeAdapter

    from .styles.base import BaseStyle


class Input(TextInputHandler, Element):
    label: Optional[str] = None

    def __init__(
        self,
        label: Optional[str] = None,
        placeholder: Optional[str] = None,
        default: Optional[str] = None,
        default_as_placeholder: bool = True,
        required: bool = False,
        required_message: Optional[str] = None,
        password: bool = False,
        inline: bool = False,
        name: Optional[str] = None,
        style: Optional[BaseStyle] = None,
        validator: Optional[TypeAdapter] = None,
        **metadata: Any,
    ):
        self.name = name
        self.label = label
        self._placeholder = placeholder
        self.default = default
        self.default_as_placeholder = default_as_placeholder
        self.required = required
        self.password = password
        self.inline = inline

        self.text = ""
        self.valid = None
        self.required_message = required_message
        self._validation_message: Optional[str] = None
        self._validator: Optional[TypeAdapter] = validator

        Element.__init__(self, style=style, metadata=metadata)
        super().__init__()

    @property
    def placeholder(self) -> str:
        if self.default_as_placeholder and self.default:
            return self.default

        return self._placeholder or ""

    @property
    def validation_message(self) -> Optional[str]:
        if self._validation_message:
            return self._validation_message

        assert self.valid

        return None

    @property
    def cursor_offset(self) -> CursorOffset:
        top = 1 if self.inline else 2

        left_offset = 0

        if self.inline and self.label:
            left_offset = len(self.label) + 1

        return CursorOffset(top=top, left=self.cursor_left + left_offset)

    @property
    def should_show_cursor(self) -> bool:
        return True

    def on_blur(self):
        self.on_validate()

    def on_validate(self):
        value = self.value.strip()

        if not value and self.required:
            self.valid = False
            self._validation_message = self.required_message or "This field is required"

            return

        if self._validator:
            from pydantic import ValidationError

            try:
                self._validator.validate_python(value)
            except ValidationError as e:
                self.valid = False

                self._validation_message = e.errors()[0]["ctx"]["reason"]

                return

        self._validation_message = None
        self.valid = True

    @property
    def value(self) -> str:
        return self.text or self.default or ""

    def ask(self) -> str:
        from .container import Container

        container = Container(style=self.style)

        container.elements = [self]

        container.run()

        return self.value
