#!/usr/bin/env python3

# Just a little easter egg ¯\_(ツ)_/¯

import argparse
import collections
import curses
import enum
import os
import pathlib
import random
import sys
import time

REQUIRED_COLUMNS = 80
REQUIRED_LINES = 24
POO_CHAR = "X"
HEAD_CHAR = "@"
BODY_CHAR = "0"
CRICKET_CHAR = "C"


class FishError(RuntimeError):
    def __init__(self, msg, win=False):
        super().__init__(msg)
        self.win = win


Coordinate = collections.namedtuple("Coordinate", ["line", "column"])


def sum_coordinates(lhs, rhs):
    return Coordinate(lhs.line + rhs.line, lhs.column + rhs.column)


class Direction(enum.Enum):
    UP = Coordinate(-1, 0)
    DOWN = Coordinate(1, 0)
    LEFT = Coordinate(0, -1)
    RIGHT = Coordinate(0, 1)


class GameState(enum.Enum):
    RUNNING = 0
    LOST = 1
    WIN = 2


class Game(object):
    def __init__(self, fish_window, speed):
        self.fish_window = fish_window
        self.fish = collections.deque()
        self.fish_direction = Direction.RIGHT
        self.poo_queue = collections.deque()
        self.obstacles = {}
        self.cricket = self._generate_cricket()
        self.speed = speed
        self.turn = 0
        self.state = GameState.RUNNING
        self.state_message = None

        max_line, max_column = self.fish_window.getmaxyx()
        self.fish.append(Coordinate(max_line // 2, max_column // 2))
        for _ in range(4):
            self.fish.append(sum_coordinates(self.fish[-1], Direction.LEFT.value))

        for c in self.fish:
            self.obstacles[c] = BODY_CHAR

        self._redraw()

    def run(self):
        while True:
            self._process_user_input()
            self._make_game_turn()
            self._redraw()
            time.sleep(1 / self.speed)

    def _generate_cricket(self, banned=()):
        new_cricket = None
        max_line, max_column = self.fish_window.getmaxyx()
        while new_cricket is None or new_cricket in self.obstacles or new_cricket in banned:
            line = random.randint(1, max_line - 2)
            column = random.randint(1, max_column - 2)
            new_cricket = Coordinate(line, column)
        return new_cricket

    def _process_user_input(self):
        next_direction = self.fish_direction
        while True:
            c = self.fish_window.getch()
            if c == -1:
                break

            if self.state != GameState.RUNNING:
                if c in [ord('\r'), ord('\n'), curses.KEY_ENTER]:
                    if self.state == GameState.WIN:
                        raise FishError("Win!", win=True)
                    else:
                        raise FishError("Defeat!")

            if c in [ord("k"), curses.KEY_UP]:
                if self.fish_direction != Direction.DOWN:
                    next_direction = Direction.UP
            elif c in [ord("l"), curses.KEY_RIGHT]:
                if self.fish_direction != Direction.LEFT:
                    next_direction = Direction.RIGHT
            elif c in [ord("j"), curses.KEY_DOWN]:
                if self.fish_direction != Direction.UP:
                    next_direction = Direction.DOWN
            elif c in [ord("h"), curses.KEY_LEFT]:
                if self.fish_direction != Direction.RIGHT:
                    next_direction = Direction.LEFT
            elif c == 27:
                raise KeyboardInterrupt()

        self.fish_direction = next_direction

    def _make_game_turn(self):
        if self.state != GameState.RUNNING:
            return

        self.turn += 1

        new_head = sum_coordinates(self.fish[0], self.fish_direction.value)
        if new_head == self.cricket:
            self.cricket = self._generate_cricket(banned=(self.cricket,))
            self.poo_queue.append(self.turn + len(self.fish))
        else:
            tail = self.fish.pop()
            del self.obstacles[tail]

            if self.poo_queue and self.poo_queue[0] <= self.turn:
                self.obstacles[tail] = POO_CHAR
                self.poo_queue.popleft()

        max_line, max_column = self.fish_window.getmaxyx()
        # NB. we have window borders so we check against [1, max-2] interval
        if not (
            1 <= new_head.line <= max_line - 2
            and 1 <= new_head.column <= max_column - 2
        ) or new_head in self.obstacles:
            self.state = GameState.LOST
            self.state_message = "Game is lost!"
            return

        self.fish.appendleft(new_head)
        self.obstacles[new_head] = BODY_CHAR

    def _redraw(self):
        def color(color_index):
            if self.state == GameState.WIN:
                color_index = 2
            elif self.state == GameState.LOST:
                color_index = 1
            return curses.color_pair(color_index)

        max_line, max_column = self.fish_window.getmaxyx()
        self.fish_window.erase()
        self.fish_window.border()

        for coord, char in self.obstacles.items():
            self.fish_window.addch(coord.line, coord.column, char, color(0))

        self.fish_window.addch(self.fish[0].line, self.fish[0].column, HEAD_CHAR, color(0))
        self.fish_window.addch(self.cricket.line, self.cricket.column, CRICKET_CHAR, color(2) | curses.A_BOLD)

        if self.state != GameState.RUNNING:
            message = self.state_message + ' Press ENTER.'
            text_line = max_line // 2
            text_column = (max_column - len(message)) // 2
            self.fish_window.addstr(text_line, text_column, message.encode('utf8'))
        self.fish_window.refresh()


def resolve_ya():
    prev = None
    path = pathlib.Path(".").resolve()
    while path != prev:
        ya_path = path / "ya"
        if ya_path.exists() and (path / ".arcadia.root").exists():
            return ya_path
        prev, path = path, path.parent
    raise FishError("Current direcotory is not inside arcadia")


def main_game(stdscr, args):
    # Init colours
    bg = curses.pair_content(0)[1]
    curses.init_pair(1, curses.COLOR_RED, bg)
    curses.init_pair(2, curses.COLOR_GREEN, bg)

    # Hide cursor
    curses.curs_set(0)

    total_columns = curses.COLS
    total_lines = curses.LINES

    if total_columns < REQUIRED_COLUMNS or total_lines < REQUIRED_LINES:
        raise FishError("Your terminal is too small. Min required size {}x{}, yours: {}x{}".format(
            REQUIRED_COLUMNS, REQUIRED_LINES,
            total_columns, total_lines))

    fish_window = curses.newwin(total_lines, total_columns, 0, 0)
    fish_window.nodelay(True)
    fish_window.keypad(True)

    try:
        g = Game(fish_window, args.speed)
        g.run()
    except curses.error:
        if stdscr.getmaxyx() != (total_lines, total_columns):
            raise FishError("You MUST NOT change terminal size")
        else:
            raise


def main():
    try:
        parser = argparse.ArgumentParser()
        parser.set_defaults(speed=10)
        parser.add_argument("--faster", action="store_true", help="faster")

        args = parser.parse_args()
        if not os.isatty(0) or not os.isatty(1):
            raise FishError("{} must be run in terminal".format(sys.argv[0]))

        os.environ["ESCDELAY"] = "0"
        try:
            curses.wrapper(lambda s: main_game(s, args))
        except KeyboardInterrupt:
            raise FishError("Interrupted")
    except FishError as e:
        if not e.win:
            print(e, file=sys.stderr)
            exit(1)


if __name__ == '__main__':
    main()
