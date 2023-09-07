# coding: utf-8

from test.const import Highlight, Colors


DEFAULT_PALETTE = {
    Highlight.RESET: None,
    Highlight.PATH: Colors.YELLOW,
    Highlight.IMPORTANT: Colors.LIGHT_DEFAULT,
    Highlight.UNIMPORTANT: Colors.DARK_DEFAULT,
    Highlight.BAD: Colors.RED,
    Highlight.WARNING: Colors.YELLOW,
    Highlight.GOOD: Colors.GREEN,
    Highlight.ALTERNATIVE1: Colors.CYAN,
    Highlight.ALTERNATIVE2: Colors.MAGENTA,
    Highlight.ALTERNATIVE3: Colors.LIGHT_BLUE,
}

EMPTY_SCHEME = {name: None for name in Colors._table.values()}

DEFAULT_TERM_SCHEME = {name: name for name in Colors._table.values()}

ESPRESSO_HTML_SCHEME = {
    Colors.BLUE: "color:#0066ff;",
    Colors.CYAN: "color:#36c8ca;",
    Colors.DEFAULT: "color:#d3d7cf;",
    Colors.GREEN: "color:#3ab23c;",
    Colors.GREY: "color:#454545;",
    Colors.MAGENTA: "color:#c5656b;",
    Colors.RED: "color:#ff3333;",
    Colors.WHITE: "color:#d3d7cf;",
    Colors.YELLOW: "color:#f0e53a;",

    Colors.LIGHT_BLUE: "color:#43a8ed;font-weight:bold;",
    Colors.LIGHT_CYAN: "color:#34e2e2;font-weight:bold;",
    Colors.LIGHT_DEFAULT: "color:#eeeeec;font-weight:bold;",
    Colors.LIGHT_GREEN: "color:#9aff87;font-weight:bold;",
    Colors.LIGHT_GREY: "color:#999999;font-weight:bold;",
    Colors.LIGHT_MAGENTA: "color:#ff818a;font-weight:bold;",
    Colors.LIGHT_RED: "color:#ef2929;font-weight:bold;",
    Colors.LIGHT_WHITE: "color:#eeeeec;font-weight:bold;",
    Colors.LIGHT_YELLOW: "color:#fffb5c;font-weight:bold;",

    Colors.DARK_BLUE: "color:#43a8ed;opacity:0.6;",
    Colors.DARK_CYAN: "color:#34e2e2;opacity:0.6;",
    Colors.DARK_DEFAULT: "color:#eeeeec;opacity:0.6;",
    Colors.DARK_GREEN: "color:#9aff87;opacity:0.6;",
    Colors.DARK_GREY: "color:#999999;opacity:0.6;",
    Colors.DARK_MAGENTA: "color:#ff818a;opacity:0.6;",
    Colors.DARK_RED: "color:#ef2929;opacity:0.6;",
    Colors.DARK_WHITE: "color:#eeeeec;opacity:0.6;",
    Colors.DARK_YELLOW: "color:#fffb5c;opacity:0.6;",
}

MATERIAL_HTML_SCHEME = {
    Colors.BLUE: "color:#134eb3;",
    Colors.CYAN: "color:#0e717c;",
    Colors.DEFAULT: "color:#212121;",
    Colors.GREEN: "color:#457b23;",
    Colors.GREY: "color:#454545;",
    Colors.MAGENTA: "color:#550088;",
    Colors.RED: "color:#b7141e;",
    Colors.WHITE: "color:#efefef;",
    Colors.YELLOW: "color:#fd7b08;",

    Colors.LIGHT_BLUE: "color:#54a4f4;font-weight:bold;",
    Colors.LIGHT_CYAN: "color:#26bbd1;font-weight:bold;",
    Colors.LIGHT_DEFAULT: "color:#424242;font-weight:bold;",
    Colors.LIGHT_GREEN: "color:#7aba3a;font-weight:bold;",
    Colors.LIGHT_GREY: "color:#999999;font-weight:bold;",
    Colors.LIGHT_MAGENTA: "color:#aa4dbc;font-weight:bold;",
    Colors.LIGHT_RED: "color:#e83a3f;font-weight:bold;",
    Colors.LIGHT_WHITE: "color:#d9d9d9;font-weight:bold;",
    Colors.LIGHT_YELLOW: "color:#fd8e08;font-weight:bold;",

    Colors.DARK_BLUE: "color:#54a4f4;opacity:0.6;",
    Colors.DARK_CYAN: "color:#26bbd1;opacity:0.6;",
    Colors.DARK_DEFAULT: "color:#424242;opacity:0.6;",
    Colors.DARK_GREEN: "color:#7aba3a;opacity:0.6;",
    Colors.DARK_GREY: "color:#999999;opacity:0.6;",
    Colors.DARK_MAGENTA: "color:#aa4dbc;opacity:0.6;",
    Colors.DARK_RED: "color:#e83a3f;opacity:0.6;",
    Colors.DARK_WHITE: "color:#d9d9d9;opacity:0.6;",
    Colors.DARK_YELLOW: "color:#fd8e08;opacity:0.6;",
}

LIGHT_HTML_SCHEME = MATERIAL_HTML_SCHEME
DARK_HTML_SCHEME = ESPRESSO_HTML_SCHEME
DEFAULT_HTML_SCHEME = DARK_HTML_SCHEME
