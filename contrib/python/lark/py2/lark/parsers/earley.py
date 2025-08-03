"""This module implements an Earley parser.

The core Earley algorithm used here is based on Elizabeth Scott's implementation, here:
    https://www.sciencedirect.com/science/article/pii/S1571066108001497

That is probably the best reference for understanding the algorithm here.

The Earley parser outputs an SPPF-tree as per that document. The SPPF tree format
is explained here: https://lark-parser.readthedocs.io/en/latest/_static/sppf/sppf.html
"""

from collections import deque

from ..tree import Tree
from ..visitors import Transformer_InPlace, v_args
from ..exceptions import UnexpectedEOF, UnexpectedToken
from ..utils import logger
from .grammar_analysis import GrammarAnalyzer
from ..grammar import NonTerminal
from .earley_common import Item, TransitiveItem
from .earley_forest import ForestSumVisitor, SymbolNode, ForestToParseTree

class Parser:
    def __init__(self, parser_conf, term_matcher, resolve_ambiguity=True, debug=False, tree_class=Tree):
        analysis = GrammarAnalyzer(parser_conf)
        self.parser_conf = parser_conf
        self.resolve_ambiguity = resolve_ambiguity
        self.debug = debug
        self.tree_class = tree_class

        self.FIRST = analysis.FIRST
        self.NULLABLE = analysis.NULLABLE
        self.callbacks = parser_conf.callbacks
        self.predictions = {}

        ## These could be moved to the grammar analyzer. Pre-computing these is *much* faster than
        #  the slow 'isupper' in is_terminal.
        self.TERMINALS = { sym for r in parser_conf.rules for sym in r.expansion if sym.is_term }
        self.NON_TERMINALS = { sym for r in parser_conf.rules for sym in r.expansion if not sym.is_term }

        self.forest_sum_visitor = None
        for rule in parser_conf.rules:
            if rule.origin not in self.predictions:
                self.predictions[rule.origin] = [x.rule for x in analysis.expand_rule(rule.origin)]

            ## Detect if any rules have priorities set. If the user specified priority = "none" then
            #  the priorities will be stripped from all rules before they reach us, allowing us to
            #  skip the extra tree walk. We'll also skip this if the user just didn't specify priorities
            #  on any rules.
            if self.forest_sum_visitor is None and rule.options.priority is not None:
                self.forest_sum_visitor = ForestSumVisitor

        self.term_matcher = term_matcher


    def predict_and_complete(self, i, to_scan, columns, transitives):
        """The core Earley Predictor and Completer.

        At each stage of the input, we handling any completed items (things
        that matched on the last cycle) and use those to predict what should
        come next in the input stream. The completions and any predicted
        non-terminals are recursively processed until we reach a set of,
        which can be added to the scan list for the next scanner cycle."""
        # Held Completions (H in E.Scotts paper).
        node_cache = {}
        held_completions = {}

        column = columns[i]
        # R (items) = Ei (column.items)
        items = deque(column)
        while items:
            item = items.pop()    # remove an element, A say, from R

            ### The Earley completer
            if item.is_complete:   ### (item.s == string)
                if item.node is None:
                    label = (item.s, item.start, i)
                    item.node = node_cache[label] if label in node_cache else node_cache.setdefault(label, SymbolNode(*label))
                    item.node.add_family(item.s, item.rule, item.start, None, None)

                # create_leo_transitives(item.rule.origin, item.start)

                ###R Joop Leo right recursion Completer
                if item.rule.origin in transitives[item.start]:
                    transitive = transitives[item.start][item.s]
                    if transitive.previous in transitives[transitive.column]:
                        root_transitive = transitives[transitive.column][transitive.previous]
                    else:
                        root_transitive = transitive

                    new_item = Item(transitive.rule, transitive.ptr, transitive.start)
                    label = (root_transitive.s, root_transitive.start, i)
                    new_item.node = node_cache[label] if label in node_cache else node_cache.setdefault(label, SymbolNode(*label))
                    new_item.node.add_path(root_transitive, item.node)
                    if new_item.expect in self.TERMINALS:
                        # Add (B :: aC.B, h, y) to Q
                        to_scan.add(new_item)
                    elif new_item not in column:
                        # Add (B :: aC.B, h, y) to Ei and R
                        column.add(new_item)
                        items.append(new_item)
                ###R Regular Earley completer
                else:
                    # Empty has 0 length. If we complete an empty symbol in a particular
                    # parse step, we need to be able to use that same empty symbol to complete
                    # any predictions that result, that themselves require empty. Avoids
                    # infinite recursion on empty symbols.
                    # held_completions is 'H' in E.Scott's paper.
                    is_empty_item = item.start == i
                    if is_empty_item:
                        held_completions[item.rule.origin] = item.node

                    originators = [originator for originator in columns[item.start] if originator.expect is not None and originator.expect == item.s]
                    for originator in originators:
                        new_item = originator.advance()
                        label = (new_item.s, originator.start, i)
                        new_item.node = node_cache[label] if label in node_cache else node_cache.setdefault(label, SymbolNode(*label))
                        new_item.node.add_family(new_item.s, new_item.rule, i, originator.node, item.node)
                        if new_item.expect in self.TERMINALS:
                            # Add (B :: aC.B, h, y) to Q
                            to_scan.add(new_item)
                        elif new_item not in column:
                            # Add (B :: aC.B, h, y) to Ei and R
                            column.add(new_item)
                            items.append(new_item)

            ### The Earley predictor
            elif item.expect in self.NON_TERMINALS: ### (item.s == lr0)
                new_items = []
                for rule in self.predictions[item.expect]:
                    new_item = Item(rule, 0, i)
                    new_items.append(new_item)

                # Process any held completions (H).
                if item.expect in held_completions:
                    new_item = item.advance()
                    label = (new_item.s, item.start, i)
                    new_item.node = node_cache[label] if label in node_cache else node_cache.setdefault(label, SymbolNode(*label))
                    new_item.node.add_family(new_item.s, new_item.rule, new_item.start, item.node, held_completions[item.expect])
                    new_items.append(new_item)

                for new_item in new_items:
                    if new_item.expect in self.TERMINALS:
                        to_scan.add(new_item)
                    elif new_item not in column:
                        column.add(new_item)
                        items.append(new_item)

    def _parse(self, lexer, columns, to_scan, start_symbol=None):
        def is_quasi_complete(item):
            if item.is_complete:
                return True

            quasi = item.advance()
            while not quasi.is_complete:
                if quasi.expect not in self.NULLABLE:
                    return False
                if quasi.rule.origin == start_symbol and quasi.expect == start_symbol:
                    return False
                quasi = quasi.advance()
            return True

        def create_leo_transitives(origin, start):
            visited = set()
            to_create = []
            trule = None
            previous = None

            ### Recursively walk backwards through the Earley sets until we find the
            #   first transitive candidate. If this is done continuously, we shouldn't
            #   have to walk more than 1 hop.
            while True:
                if origin in transitives[start]:
                    previous = trule = transitives[start][origin]
                    break

                is_empty_rule = not self.FIRST[origin]
                if is_empty_rule:
                    break

                candidates = [ candidate for candidate in columns[start] if candidate.expect is not None and origin == candidate.expect ]
                if len(candidates) != 1:
                    break
                originator = next(iter(candidates))

                if originator is None or originator in visited:
                    break

                visited.add(originator)
                if not is_quasi_complete(originator):
                    break

                trule = originator.advance()
                if originator.start != start:
                    visited.clear()

                to_create.append((origin, start, originator))
                origin = originator.rule.origin
                start = originator.start

            # If a suitable Transitive candidate is not found, bail.
            if trule is None:
                return

            #### Now walk forwards and create Transitive Items in each set we walked through; and link
            #    each transitive item to the next set forwards.
            while to_create:
                origin, start, originator = to_create.pop()
                titem = None
                if previous is not None:
                        titem = previous.next_titem = TransitiveItem(origin, trule, originator, previous.column)
                else:
                        titem = TransitiveItem(origin, trule, originator, start)
                previous = transitives[start][origin] = titem



        def scan(i, token, to_scan):
            """The core Earley Scanner.

            This is a custom implementation of the scanner that uses the
            Lark lexer to match tokens. The scan list is built by the
            Earley predictor, based on the previously completed tokens.
            This ensures that at each phase of the parse we have a custom
            lexer context, allowing for more complex ambiguities."""
            next_to_scan = set()
            next_set = set()
            columns.append(next_set)
            transitives.append({})
            node_cache = {}

            for item in set(to_scan):
                if match(item.expect, token):
                    new_item = item.advance()
                    label = (new_item.s, new_item.start, i)
                    new_item.node = node_cache[label] if label in node_cache else node_cache.setdefault(label, SymbolNode(*label))
                    new_item.node.add_family(new_item.s, item.rule, new_item.start, item.node, token)

                    if new_item.expect in self.TERMINALS:
                        # add (B ::= Aai+1.B, h, y) to Q'
                        next_to_scan.add(new_item)
                    else:
                        # add (B ::= Aa+1.B, h, y) to Ei+1
                        next_set.add(new_item)

            if not next_set and not next_to_scan:
                expect = {i.expect.name for i in to_scan}
                raise UnexpectedToken(token, expect, considered_rules=set(to_scan), state=frozenset(i.s for i in to_scan))

            return next_to_scan


        # Define parser functions
        match = self.term_matcher

        # Cache for nodes & tokens created in a particular parse step.
        transitives = [{}]

        ## The main Earley loop.
        # Run the Prediction/Completion cycle for any Items in the current Earley set.
        # Completions will be added to the SPPF tree, and predictions will be recursively
        # processed down to terminals/empty nodes to be added to the scanner for the next
        # step.
        expects = {i.expect for i in to_scan}
        i = 0
        for token in lexer.lex(expects):
            self.predict_and_complete(i, to_scan, columns, transitives)

            to_scan = scan(i, token, to_scan)
            i += 1

            expects.clear()
            expects |= {i.expect for i in to_scan}

        self.predict_and_complete(i, to_scan, columns, transitives)

        ## Column is now the final column in the parse.
        assert i == len(columns)-1
        return to_scan

    def parse(self, lexer, start):
        assert start, start
        start_symbol = NonTerminal(start)

        columns = [set()]
        to_scan = set()     # The scan buffer. 'Q' in E.Scott's paper.

        ## Predict for the start_symbol.
        # Add predicted items to the first Earley set (for the predictor) if they
        # result in a non-terminal, or the scanner if they result in a terminal.
        for rule in self.predictions[start_symbol]:
            item = Item(rule, 0, 0)
            if item.expect in self.TERMINALS:
                to_scan.add(item)
            else:
                columns[0].add(item)

        to_scan = self._parse(lexer, columns, to_scan, start_symbol)

        # If the parse was successful, the start
        # symbol should have been completed in the last step of the Earley cycle, and will be in
        # this column. Find the item for the start_symbol, which is the root of the SPPF tree.
        solutions = [n.node for n in columns[-1] if n.is_complete and n.node is not None and n.s == start_symbol and n.start == 0]
        if not solutions:
            expected_terminals = [t.expect.name for t in to_scan]
            raise UnexpectedEOF(expected_terminals, state=frozenset(i.s for i in to_scan))

        if self.debug:
            from .earley_forest import ForestToPyDotVisitor
            try:
                debug_walker = ForestToPyDotVisitor()
            except ImportError:
                logger.warning("Cannot find dependency 'pydot', will not generate sppf debug image")
            else:
                debug_walker.visit(solutions[0], "sppf.png")


        if len(solutions) > 1:
            assert False, 'Earley should not generate multiple start symbol items!'

        if self.tree_class is not None:
            # Perform our SPPF -> AST conversion
            transformer = ForestToParseTree(self.tree_class, self.callbacks, self.forest_sum_visitor and self.forest_sum_visitor(), self.resolve_ambiguity)
            return transformer.transform(solutions[0])

        # return the root of the SPPF
        return solutions[0]

class ApplyCallbacks(Transformer_InPlace):
    def __init__(self, postprocess):
        self.postprocess = postprocess

    @v_args(meta=True)
    def drv(self, children, meta):
        return self.postprocess[meta.rule](children)
