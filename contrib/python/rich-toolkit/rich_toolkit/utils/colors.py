import atexit
import io
import signal

from rich.color import Color
from rich.color_triplet import ColorTriplet
from rich.style import Style
from rich.text import Text
from typing_extensions import Literal


def lighten(color: Color, amount: float) -> Color:
    triplet = color.triplet

    if not triplet:
        triplet = color.get_truecolor()

    r, g, b = triplet

    r = int(r + (255 - r) * amount)
    g = int(g + (255 - g) * amount)
    b = int(b + (255 - b) * amount)

    return Color.from_triplet(ColorTriplet(r, g, b))


def darken(color: Color, amount: float) -> Color:
    triplet = color.triplet

    if not triplet:
        triplet = color.get_truecolor()

    r, g, b = triplet

    r = int(r * (1 - amount))
    g = int(g * (1 - amount))
    b = int(b * (1 - amount))

    return Color.from_triplet(ColorTriplet(r, g, b))


def fade_color(
    color: Color, background_color: Color, brightness_multiplier: float
) -> Color:
    """
    Fade a color towards the background color based on a brightness multiplier.

    Args:
        color: The original color (Rich Color object)
        background_color: The background color to fade towards
        brightness_multiplier: Float between 0.0 and 1.0 where:
            - 1.0 = original color (no fading)
            - 0.0 = completely faded to background color

    Returns:
        A new Color object with the faded color
    """
    # Extract RGB components from the original color

    color_triplet = color.triplet

    if color_triplet is None:
        color_triplet = color.get_truecolor()

    r, g, b = color_triplet

    assert background_color.triplet is not None
    # Extract RGB components from the background color
    bg_r, bg_g, bg_b = background_color.triplet

    # Blend the original color with the background color based on the brightness multiplier
    new_r = int(r * brightness_multiplier + bg_r * (1 - brightness_multiplier))
    new_g = int(g * brightness_multiplier + bg_g * (1 - brightness_multiplier))
    new_b = int(b * brightness_multiplier + bg_b * (1 - brightness_multiplier))

    # Ensure values are within valid RGB range (0-255)
    new_r = max(0, min(255, new_r))
    new_g = max(0, min(255, new_g))
    new_b = max(0, min(255, new_b))

    # Return a new Color object with the calculated RGB values
    return Color.from_rgb(new_r, new_g, new_b)


def fade_text(
    text: Text,
    text_color: Color,
    background_color: str,
    brightness_multiplier: float,
) -> Text:
    bg_color = Color.parse(background_color)

    new_spans = []
    for span in text._spans:
        style: Style | str = span.style

        if isinstance(style, str):
            style = Style.parse(style)

        if style.color:
            color = style.color

            if color == Color.default():
                color = text_color

            style = style.copy()
            style._color = fade_color(color, bg_color, brightness_multiplier)

        new_spans.append(span._replace(style=style))
    text = text.copy()
    text._spans = new_spans
    text.style = Style(color=fade_color(text_color, bg_color, brightness_multiplier))
    return text


def _get_terminal_color(
    color_type: Literal["text", "background"], default_color: str
) -> str:
    import os
    import re
    import select
    import sys

    # Set appropriate OSC code and default color based on color_type
    if color_type.lower() == "text":
        osc_code = "10"
    elif color_type.lower() == "background":
        osc_code = "11"
    else:
        raise ValueError("color_type must be either 'text' or 'background'")

    try:
        import termios
        import tty
    except ImportError:
        # Not on Unix-like systems (probably Windows), so we return the default color
        return default_color

    try:
        if not os.isatty(sys.stdin.fileno()):
            return default_color
    except (AttributeError, IOError, io.UnsupportedOperation):
        # Handle cases where stdin is redirected or not a real TTY (like in tests)
        return default_color

    # Save terminal settings so we can restore them
    old_settings = termios.tcgetattr(sys.stdin)
    old_blocking = os.get_blocking(sys.stdin.fileno())

    def restore_terminal():
        try:
            termios.tcsetattr(sys.stdin, termios.TCSADRAIN, old_settings)
            os.set_blocking(sys.stdin.fileno(), old_blocking)
        except Exception:
            pass

    atexit.register(restore_terminal)
    signal.signal(
        signal.SIGTERM, lambda signum, frame: (restore_terminal(), sys.exit(0))
    )

    try:
        # Set terminal to raw mode
        tty.setraw(sys.stdin)

        # Send OSC escape sequence to query color
        sys.stdout.write(f"\033]{osc_code};?\033\\")
        sys.stdout.flush()

        # Wait for response with timeout
        if select.select([sys.stdin], [], [], 1.0)[0]:
            os.set_blocking(sys.stdin.fileno(), False)

            # Read response
            response = ""
            while True:
                try:
                    char = sys.stdin.read(1)
                except io.BlockingIOError:
                    char = ""
                except TypeError:
                    char = ""
                if char is None or char == "":  # No more response data available
                    if select.select([sys.stdin], [], [], 1.0)[0]:
                        continue
                    else:
                        break

                response += char

                if char == "\\":  # End of OSC response
                    break
                if len(response) > 50:  # Safety limit
                    break

            # Parse the response (format: \033]10;rgb:RRRR/GGGG/BBBB\033\\)
            match = re.search(
                r"rgb:([0-9a-f]+)/([0-9a-f]+)/([0-9a-f]+)", response, re.IGNORECASE
            )
            if match:
                r, g, b = match.groups()
                # Convert to standard hex format
                r = int(r[0:2], 16)
                g = int(g[0:2], 16)
                b = int(b[0:2], 16)
                return f"#{r:02x}{g:02x}{b:02x}"

            return default_color
        else:
            return default_color
    finally:
        # Restore terminal settings
        restore_terminal()


def get_terminal_text_color(default_color: str = "#FFFFFF") -> str:
    """Get the terminal text (foreground) color."""
    return _get_terminal_color("text", default_color)


def get_terminal_background_color(default_color: str = "#000000") -> str:
    """Get the terminal background color."""
    return _get_terminal_color("background", default_color)


if __name__ == "__main__":
    print(get_terminal_background_color())
    print(get_terminal_text_color())
