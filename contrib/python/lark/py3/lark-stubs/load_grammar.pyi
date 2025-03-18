from typing import List, Tuple, Union, Callable, Dict, Optional

from lark import Tree
from lark.grammar import RuleOptions


class Grammar:
    rule_defs: List[Tuple[str, Tuple[str, ...], Tree, RuleOptions]]
    term_defs: List[Tuple[str, Tuple[Tree, int]]]
    ignore: List[str]


class GrammarBuilder:
    global_keep_all_tokens: bool
    import_paths: List[Union[str, Callable]]

    def __init__(self, global_keep_all_tokens: bool = False, import_paths: List[Union[str, Callable]] = None) -> None: ...

    def load_grammar(self, grammar_text: str, grammar_name: str = ..., mangle: Callable[[str], str] = None) -> None: ...

    def do_import(self, dotted_path: Tuple[str, ...], base_path: Optional[str], aliases: Dict[str, str],
                  base_mangle: Callable[[str], str] = None) -> None:  ...

    def validate(self) -> None: ...

    def build(self) -> Grammar: ...
