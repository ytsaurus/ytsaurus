import os

import yaml

import yatest.common

DOCS_ROOT = "yt/docs"
INTERNAL_OVERLAY_DIR = "yandex-specific"
INTERNAL_GATE = 'audience == "internal"'

PAGES_PENDING_MOVE_TO_INTERNAL_TREE = set()


def _is_internal_gate(when):
    return when is not None and " ".join(str(when).split()) == INTERNAL_GATE


def _collect_internal_hrefs(node, toc_dir, gated, hrefs):
    if isinstance(node, list):
        for child in node:
            _collect_internal_hrefs(child, toc_dir, gated, hrefs)
        return
    if not isinstance(node, dict):
        return
    gated = gated or _is_internal_gate(node.get("when"))
    href = node.get("href")
    if gated and isinstance(href, str):
        hrefs.add(os.path.normpath(os.path.join(toc_dir, href.split("#")[0])))
    _collect_internal_hrefs(node.get("items"), toc_dir, gated, hrefs)


def _internal_overlay_of(page):
    lang, separator, rest = page.partition(os.sep)
    if not separator:
        return None
    return os.path.join(lang, INTERNAL_OVERLAY_DIR, rest)


def _find_pages_gated_internal_but_living_outside_the_internal_tree():
    docs_root = yatest.common.source_path(DOCS_ROOT)
    lang_roots = sorted(
        name for name in os.listdir(docs_root) if os.path.isdir(os.path.join(docs_root, name, INTERNAL_OVERLAY_DIR))
    )
    assert lang_roots, "no language tree with a {} overlay found under {}".format(INTERNAL_OVERLAY_DIR, DOCS_ROOT)

    pages = set()
    for lang in lang_roots:
        for directory, _, file_names in os.walk(os.path.join(docs_root, lang)):
            for file_name in file_names:
                if not file_name.startswith("toc") or not file_name.endswith(".yaml"):
                    continue
                with open(os.path.join(directory, file_name), encoding="utf-8") as toc_file:
                    toc = yaml.safe_load(toc_file)
                hrefs = set()
                _collect_internal_hrefs(
                    toc.get("items") if isinstance(toc, dict) else toc,
                    os.path.relpath(directory, docs_root),
                    False,
                    hrefs,
                )
                for page in hrefs:
                    if INTERNAL_OVERLAY_DIR in page.split(os.sep):
                        continue
                    overlay = _internal_overlay_of(page)
                    if overlay and os.path.isfile(os.path.join(docs_root, overlay)):
                        continue
                    if os.path.isfile(os.path.join(docs_root, page)):
                        pages.add(os.path.join(DOCS_ROOT, page))
    return pages


def test_internal_only_pages_live_in_the_internal_tree():
    unexpected = sorted(
        _find_pages_gated_internal_but_living_outside_the_internal_tree() - PAGES_PENDING_MOVE_TO_INTERNAL_TREE
    )
    assert not unexpected, (
        "pages are gated by 'when: {}' in a toc but live in the external tree, "
        "so they are shipped to the public repository while being unreachable in the public book; "
        "move them under a {}/ path: {}".format(INTERNAL_GATE, INTERNAL_OVERLAY_DIR, unexpected)
    )


def test_no_stale_entries_in_pages_pending_move():
    stale = sorted(
        PAGES_PENDING_MOVE_TO_INTERNAL_TREE - _find_pages_gated_internal_but_living_outside_the_internal_tree()
    )
    assert not stale, (
        "these pages no longer violate the layering rule; "
        "drop them from PAGES_PENDING_MOVE_TO_INTERNAL_TREE: {}".format(stale)
    )
