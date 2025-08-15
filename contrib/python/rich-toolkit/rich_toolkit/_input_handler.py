import string


class TextInputHandler:
    DOWN_KEY = "\x1b[B"
    UP_KEY = "\x1b[A"
    LEFT_KEY = "\x1b[D"
    RIGHT_KEY = "\x1b[C"
    BACKSPACE_KEY = "\x7f"
    DELETE_KEY = "\x1b[3~"
    TAB_KEY = "\t"
    SHIFT_TAB_KEY = "\x1b[Z"
    ENTER_KEY = "\r"

    def __init__(self):
        self.text = ""
        self.cursor_left = 0

    def _move_cursor_left(self) -> None:
        self.cursor_left = max(0, self.cursor_left - 1)

    def _move_cursor_right(self) -> None:
        self.cursor_left = min(len(self.text), self.cursor_left + 1)

    def _insert_char(self, char: str) -> None:
        self.text = self.text[: self.cursor_left] + char + self.text[self.cursor_left :]
        self._move_cursor_right()

    def _delete_char(self) -> None:
        if self.cursor_left == 0:
            return

        self.text = self.text[: self.cursor_left - 1] + self.text[self.cursor_left :]
        self._move_cursor_left()

    def _delete_forward(self) -> None:
        if self.cursor_left == len(self.text):
            return

        self.text = self.text[: self.cursor_left] + self.text[self.cursor_left + 1 :]

    def handle_key(self, key: str) -> None:
        if key == self.BACKSPACE_KEY:
            self._delete_char()
        elif key == self.DELETE_KEY:
            self._delete_forward()
        elif key == self.LEFT_KEY:
            self._move_cursor_left()
        elif key == self.RIGHT_KEY:
            self._move_cursor_right()
        elif key in (
            self.UP_KEY,
            self.DOWN_KEY,
            self.ENTER_KEY,
            self.SHIFT_TAB_KEY,
            self.TAB_KEY,
        ):
            pass
        else:
            # even if we call this handle key, in some cases we might receive multiple keys
            # at once
            for char in key:
                if char in string.printable:
                    self._insert_char(char)
