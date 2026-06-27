# Holder analysis and retention-chain / cycle tracing.
#
# Find every live pointer to an object, classify each holder (strong / weak /
# raw / ref? / dead) and its container, then follow strong holders to report a
# retention CYCLE or a ROOT.

import re
from collections import deque

import gdb

from memory import find_pointers_to
from type_info import find_enclosing_object, wrapper_inner_type, split_template_args
from ref_counted import resolve_refcount

_BINDSTATE_RE = re.compile(r"NYT::NDetail::TBindState<(.+)>$")


def classify_reference(container_type, target_inner, slot_offset=None):
    """Classify a reference to a target held by a container of container_type.
    Returns (kind, note) with kind in 'strong' | 'weak' | 'raw' | 'noise' |
    'ref?' | 'unknown'.

    Preferred path: introspect the container's *actual* field at slot_offset via
    debug info -- this names the holding member exactly (TIntrusivePtr -> strong,
    TWeakPtr -> weak, ...) and, crucially, recognizes a byte-match that lands on a
    non-pointer field as coincidental ('noise') rather than a candidate edge.

    Fallbacks (no field info): for TBindState<...> closures we parse the
    captured-arg template list; otherwise we report a conservative 'ref?'.
    """
    if container_type is None:
        return "unknown", ""

    # Precise: the container's real field at the matched offset.
    if slot_offset is not None:
        kind, inner, name = field_holder_kind(container_type, slot_offset)
        if kind == "noise":
            return "noise", "byte-match at a non-pointer offset (coincidental)"
        if kind is not None:
            note = "%s member%s" % (kind, (" " + name) if name else "")
            if target_inner and inner and not _type_matches(inner, target_inner):
                note += " (member <%s> != target)" % inner
            return kind, note

    m = _BINDSTATE_RE.match(container_type)
    if m:
        # Prefer the captured arg whose T matches the target; else first concrete.
        best = ("unknown", "no matching capture in TBindState")
        for arg in split_template_args(m.group(1)):
            kind, t = _ptr_wrapper_kind(arg)
            if kind is None:
                continue
            note = "TBindState capture %s" % arg
            if target_inner and t and _type_matches(t, target_inner):
                return kind, note
            if best[0] == "unknown":
                best = (kind, note + " (type-match unverified)")
        return best

    # Generic container: we can't always know the member's smartness without
    # field-level type info. A live ref-counted container pointing at the target
    # is almost always holding it via an intrusive (strong) or weak member. Report
    # "ref?" and let the caller treat it as a candidate strong edge, reconciling
    # against the target's StrongCount.
    return "ref?", "live container; member smartness undetermined (treat as candidate ref)"


def _ptr_wrapper_kind(arg):
    """Map a single type/arg string to (kind, inner_type)."""
    arg = arg.strip()
    for prefix, kind in (
        ("NYT::TIntrusivePtr<", "strong"),
        ("NYT::TAtomicIntrusivePtr<", "strong"),
        ("NYT::TWeakPtr<", "weak"),
        ("NYT::TUnretainedWrapper<", "raw"),
    ):
        if arg.startswith(prefix) and arg.endswith(">"):
            return kind, arg[len(prefix):-1].strip()
    return None, None


def field_holder_kind(container_type, offset):
    """Resolve the container's debug type and classify the field at byte #offset.
    Returns (kind, inner_type, field_name):
      - ('strong'|'weak'|'raw', inner, name) for a recognized holder member;
      - ('noise', None, None) if the type is known but #offset is not a pointer
        field (the byte-match is coincidental);
      - (None, None, None) if the type has no usable debug info (caller falls back).
    """
    gtype = _lookup_type(container_type)
    if gtype is None:
        return None, None, None
    kind, inner, name = _smart_ptr_at(gtype, offset)
    if kind is not None:
        return kind, inner, name
    return "noise", None, None


def _lookup_type(name):
    try:
        return gdb.lookup_type(name).strip_typedefs()
    except (gdb.error, RuntimeError):
        return None


def _smart_ptr_at(gtype, offset, depth=0):
    """Find the holder member (smart/raw pointer) covering byte #offset within
    gtype, descending through base classes and nested aggregates. Returns
    (kind, inner_type, field_name) or (None, None, None)."""
    if depth > 16:
        return None, None, None
    try:
        fields = gtype.fields()
    except (gdb.error, TypeError, AttributeError):
        return None, None, None
    for f in fields:
        if getattr(f, "bitpos", None) is None:
            continue  # static member / enum constant
        fstart = f.bitpos // 8
        ftype = f.type.strip_typedefs()
        try:
            fsize = ftype.sizeof or 1
        except gdb.error:
            fsize = 1
        if not (fstart <= offset < fstart + fsize):
            continue
        # A smart-pointer member must match at its own start (its first word is
        # the stored pointer): prefer it over recursing into its raw inner field.
        kind, inner = _ptr_wrapper_kind(_type_name(ftype))
        if kind is not None and fstart == offset:
            return kind, inner, f.name
        if ftype.code in (gdb.TYPE_CODE_STRUCT, gdb.TYPE_CODE_UNION):
            sub = _smart_ptr_at(ftype, offset - fstart, depth + 1)
            if sub[0] is not None:
                return sub
        if fstart == offset and ftype.code == gdb.TYPE_CODE_PTR:
            return "raw", None, f.name
    return None, None, None


def _type_name(ftype):
    return str(ftype).replace("const ", "").replace("volatile ", "").strip()


def _type_matches(a, b):
    """Loose type equality ignoring whitespace and leading/trailing qualifiers."""
    na = re.sub(r"\s+", "", a)
    nb = re.sub(r"\s+", "", b)
    return na == nb or na.endswith(nb) or nb.endswith(na)


class Holder:
    __slots__ = ("slot", "container", "container_type", "kind", "note", "container_refcount")


def analyze_holders(target):
    """Find every live pointer to target and resolve each holder's container,
    kind, and liveness. Returns (target_refcount, [Holder])."""
    target_rc = resolve_refcount(target)
    target_inner = target_rc.inner or (
        wrapper_inner_type(target_rc.typename) if target_rc.typename else None
    )
    holders = []
    for slot in find_pointers_to(target):
        primary, ctype, _vslot = find_enclosing_object(slot)
        h = Holder()
        h.slot = slot
        h.container = primary
        h.container_type = ctype
        h.container_refcount = None
        if primary is None:
            h.kind, h.note = "unknown", "no enclosing vtable found (raw stack/global?)"
            holders.append(h)
            continue
        crc = resolve_refcount(primary)
        h.container_refcount = crc
        if not crc.alive:
            h.kind = "dead"
            h.note = "container appears dead/freed (StrongCount=%s)" % (
                crc.strong if crc.strong is not None else "?")
            holders.append(h)
            continue
        offset = (slot - primary) & ((1 << 64) - 1)
        h.kind, h.note = classify_reference(ctype, target_inner, offset)
        holders.append(h)
    return target_rc, holders


def trace_retention(start, max_depth=40, max_nodes=200, stop_on_cycle=True):
    """Follow strong holders backward, breadth-first. Each node is an object
    address; an edge (container -> held_object) means "container holds object".
    Report a CYCLE when a holder is already in the graph, or a ROOT when a node
    has no live strong holder.

    BFS (not DFS) so the *shortest* cycle is found first with minimal expansion
    -- crucial because the retention graph of a leaked object fans out into
    globally-shared infrastructure (delayed executor, invoker queues). With
    stop_on_cycle (default) the walk returns as soon as the first cycle closes,
    so it doesn't drown in that fan-out.
    """
    results = []
    visited = {start}
    came_from = {}  # node -> (held_obj, holder): the edge by which node entered
    queue = deque([(start, 0)])
    nodes = 0

    def path_to(obj):
        chain = []
        cur = obj
        while cur in came_from:
            held, h = came_from[cur]
            chain.append((cur, held, h))  # cur holds held
            cur = held
        chain.reverse()
        return chain

    while queue:
        obj, depth = queue.popleft()
        if nodes >= max_nodes:
            break
        if depth > max_depth:
            continue
        nodes += 1
        rc = resolve_refcount(obj)
        if not rc.ok:
            results.append({"kind": "root", "reason": rc.error or "not ref-counted",
                            "obj": obj, "path": path_to(obj)})
            continue
        _trc, holders = analyze_holders(obj)
        strong_holders = []
        seen = set()
        for h in holders:
            if h.container is None or h.kind not in ("strong", "ref?"):
                continue
            if h.container == obj or h.container in seen:
                continue
            seen.add(h.container)
            strong_holders.append(h)
        if not strong_holders:
            other = [h for h in holders if h.kind != "dead" and h.container is not None]
            results.append({"kind": "root", "reason": "no live strong holder found",
                            "obj": obj, "candidates": other, "path": path_to(obj)})
            continue
        for h in strong_holders:
            container = h.container
            edge = (container, obj, h)
            if container in visited:
                results.append({"kind": "cycle", "edge": edge, "path": path_to(obj) + [edge]})
                if stop_on_cycle:
                    return results
                continue
            visited.add(container)
            came_from[container] = (obj, h)
            queue.append((container, depth + 1))
    return results
