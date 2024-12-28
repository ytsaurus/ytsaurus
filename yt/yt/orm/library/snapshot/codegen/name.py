from dataclasses import dataclass
from typing import Optional
import re


@dataclass(frozen=True, order=True)
class SnapshotEntityName:
    snake_case: str
    pascal_case: str
    camel_case: str
    human_readable: str

    def __init__(
        self,
        snake_case: Optional[str] = None,
        pascal_case: Optional[str] = None,
        camel_case: Optional[str] = None,
        human_readable: Optional[str] = None,
    ):
        tokens: Optional[list[str]] = None

        if snake_case is not None:
            _validate_snake_case_name(snake_case)
            if tokens is None:
                tokens = _tokenize_snake_case_name(snake_case)

        if pascal_case is not None:
            _validate_pascal_case_name(pascal_case)
            if tokens is None:
                tokens = _tokenize_pascal_case_name(pascal_case)

        if camel_case is not None:
            _validate_camel_case_name(camel_case)
            if tokens is None:
                tokens = _tokenize_camel_case_name(camel_case)

        if human_readable is not None and tokens is None:
            tokens = _tokenize_human_readable_name(human_readable)

        if tokens:
            if not snake_case:
                snake_case = _make_snake_case_name(tokens)

            if not pascal_case:
                pascal_case = _make_pascal_case_name(tokens)

            if not camel_case:
                camel_case = _make_camel_case_name(tokens)

            if not human_readable:
                human_readable = _make_human_readable_name(tokens)

        assert (snake_case is None and pascal_case is None and camel_case is None and human_readable is None) or (
            snake_case is not None and pascal_case is not None and camel_case is not None and human_readable is not None
        )

        object.__setattr__(self, "snake_case", snake_case)
        object.__setattr__(self, "pascal_case", pascal_case)
        object.__setattr__(self, "camel_case", camel_case)
        object.__setattr__(self, "human_readable", human_readable)

    @classmethod
    def from_string(cls, s: str):
        if " " in s:
            return cls(human_readable=s)

        if "_" in s:
            return cls(snake_case=s)

        if s[0].isupper():
            return cls(pascal_case=s)

        return cls(camel_case=s)

    @classmethod
    def from_value(cls, param: dict | str):
        if isinstance(param, dict):
            return cls(**param)

        if isinstance(param, str):
            return cls.from_string(param)

        raise ValueError(
            f"Failed to constuct name from value of type {type(param)}: \"{param}\". "
            f"It is possible to constuct name from dictionary or string."
        )

    def __bool__(self):
        return bool(self.human_readable) and bool(self.snake_case) and bool(self.pascal_case) and bool(self.camel_case)

    def __str__(self):
        return str(self.human_readable)


def _validate_snake_case_name(s: str):
    if not s:
        raise ValueError("Empty string is not a valid snake case name")

    def is_valid_char(char: str):
        return (char.isalpha() and char.islower()) or char.isdigit() or char == "_"

    for index, char in enumerate(s):
        if not is_valid_char(char):
            raise ValueError(f"Invalid snake case name \"{s}\": unexpected character \"{char}\" at position {index}")


def _validate_pascal_case_name(s: str):
    if not s:
        raise ValueError("Empty string is not a valid pascal case name")

    if not (s[0].isalpha() and s[0].isupper()):
        raise ValueError(f"Invalid pascal case name \"{s}\": unexpected character \"{s[0]}\" at position 0")

    for index, char in enumerate(s[1:]):
        if not char.isalnum():
            raise ValueError(f"Invalid pascal case name \"{s}\": unexpected character \"{char}\" at position {index}")


def _validate_camel_case_name(s: str):
    if not s:
        raise ValueError("Empty string is not a valid camel case name")

    if not (s[0].isalpha() and s[0].islower()):
        raise ValueError(f"Invalid camel case name \"{s}\": unexpected character \"{s[0]}\" at position 0")

    for index, char in enumerate(s[1:]):
        if not char.isalnum():
            raise ValueError(f"Invalid camel case name \"{s}\": unexpected character \"{char}\" at position {index}")


def _tokenize_snake_case_name(s: str) -> list[str]:
    return s.split("_")


def _tokenize_pascal_case_name(s: str) -> list[str]:
    # https://stackoverflow.com/a/37697078
    return re.sub('([A-Z][a-z]+)', r' \1', re.sub('([A-Z]+)', r' \1', s)).split()


def _tokenize_camel_case_name(s: str) -> list[str]:
    return _tokenize_pascal_case_name(s)


def _tokenize_human_readable_name(s: str) -> list[str]:
    return s.split()


def _make_snake_case_name(tokens: list[str]) -> str:
    return "_".join([token.lower() for token in tokens])


def _make_pascal_case_name(tokens: list[str]) -> str:
    return "".join([_capitalize(token) for token in tokens])


def _make_camel_case_name(tokens: list[str]) -> str:
    return tokens[0].lower() + _make_pascal_case_name(tokens[1:])


def _make_human_readable_name(tokens: list[str]) -> str:
    return " ".join([token.lower() for token in tokens])


def _capitalize(s: str) -> str:
    return s[:1].upper() + s[1:]
