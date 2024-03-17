# Generated by devtools/yamaker (pypi).

PY2_LIBRARY()

VERSION(0.11.2)

LICENSE(MIT)

NO_LINT()

NO_CHECK_IMPORTS(
    lark.__pyinstaller.*
)

PY_SRCS(
    TOP_LEVEL
    lark-stubs/__init__.pyi
    lark-stubs/exceptions.pyi
    lark-stubs/grammar.pyi
    lark-stubs/indenter.pyi
    lark-stubs/lark.pyi
    lark-stubs/lexer.pyi
    lark-stubs/load_grammar.pyi
    lark-stubs/reconstruct.pyi
    lark-stubs/tree.pyi
    lark-stubs/visitors.pyi
    lark/__init__.py
    lark/__pyinstaller/__init__.py
    lark/__pyinstaller/hook-lark.py
    lark/common.py
    lark/exceptions.py
    lark/grammar.py
    lark/indenter.py
    lark/lark.py
    lark/lexer.py
    lark/load_grammar.py
    lark/parse_tree_builder.py
    lark/parser_frontends.py
    lark/parsers/__init__.py
    lark/parsers/cyk.py
    lark/parsers/earley.py
    lark/parsers/earley_common.py
    lark/parsers/earley_forest.py
    lark/parsers/grammar_analysis.py
    lark/parsers/lalr_analysis.py
    lark/parsers/lalr_parser.py
    lark/parsers/lalr_puppet.py
    lark/parsers/xearley.py
    lark/reconstruct.py
    lark/tools/__init__.py
    lark/tools/nearley.py
    lark/tools/serialize.py
    lark/tools/standalone.py
    lark/tree.py
    lark/tree_matcher.py
    lark/utils.py
    lark/visitors.py
)

RESOURCE_FILES(
    PREFIX contrib/python/lark-parser/py2/
    .dist-info/METADATA
    .dist-info/entry_points.txt
    .dist-info/top_level.txt
    lark/grammars/common.lark
    lark/grammars/lark.lark
    lark/grammars/python.lark
    lark/grammars/unicode.lark
)

END()