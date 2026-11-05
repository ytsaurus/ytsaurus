# Holder analysis and retention-chain / cycle tracing.
#
# Find every live pointer to an object, classify each holder (strong / weak /
# raw / ref? / dead) and its container, then follow strong holders to report a
# retention CYCLE or a ROOT.

import re

import gdb

import tcmalloc
from memory import find_pointers_into
from type_info import find_enclosing_object, wrapper_inner_type, split_template_args
from ref_counted import resolve_refcount

# Cap how far past the object base we scan for sub-object holders.
_MAX_SUBOBJECT_SPAN = 0x10000

_BINDSTATE_RE = re.compile(r"NYT::NDetail::TBindState<(.+)>$")


def classify_reference(container_type, target_inner, slot_offset=None, sub_offset=0):
    """Classify a reference to a target held by a container of container_type.
    Returns (kind, note) with kind in 'strong' | 'weak' | 'raw' | 'noise' |
    'ref?' | 'unknown'.

    #sub_offset is the byte offset *within the target* that the slot points at: 0
    is the object base, non-zero is a secondary base subobject (multiple
    inheritance), where the holder's member type is legitimately a base of the
    target -- so a member/target type mismatch there is expected, not suspicious.

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
            # At a secondary base subobject the member type is a base of the
            # target, so only flag a mismatch for a base-pointer (sub_offset 0).
            if sub_offset == 0 and target_inner and inner and not _type_matches(inner, target_inner):
                note += " (member <%s> != target)" % inner
            return kind, note

    m = _BINDSTATE_RE.match(container_type)
    if m:
        # Prefer the captured arg whose T matches the target; else first concrete.
        # A live closure that points at the target but whose capture isn't visible
        # in the type (e.g. a C++ lambda capture) is still a candidate ref, so we
        # default to "ref?" rather than "unknown".
        best = ("ref?", "live TBindState closure; capture not visible in type (lambda?)")
        for arg in split_template_args(m.group(1)):
            kind, t = _ptr_wrapper_kind(arg)
            if kind is None:
                continue
            note = "TBindState capture %s" % arg
            if target_inner and t and _type_matches(t, target_inner):
                return kind, note
            if best[0] == "ref?":
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
    __slots__ = ("slot", "sub_offset", "container", "container_type", "kind",
                 "note", "container_refcount")


def _object_size(target_rc):
    """sizeof the target's dynamic type (so the holder scan covers every base
    subobject), or 8 (base only) when the type can't be sized."""
    for name in (target_rc.typename, target_rc.inner):
        if not name:
            continue
        try:
            return max(8, int(gdb.lookup_type(name).sizeof))
        except (gdb.error, RuntimeError, ValueError):
            continue
    return 8


def object_extent(target, rc=None):
    """Byte span to scan for holders of any sub-object of the allocation at
    #target: the exact tcmalloc block size, else the dynamic type's sizeof,
    capped at _MAX_SUBOBJECT_SPAN. Shared by the heap scan (analyze_holders) and
    the off-heap stack scans so both see interface/secondary-base holders."""
    blk = tcmalloc.allocation_of(target)
    if blk is not None:
        size = blk[1]
    else:
        size = _object_size(rc if rc is not None else resolve_refcount(target))
    return min(size, _MAX_SUBOBJECT_SPAN)


def analyze_holders(target):
    """Find every live pointer to target and resolve each holder's container,
    kind, and liveness. Returns (target_refcount, [Holder]).

    Scans the whole object extent, not just its base address, so a holder of a
    secondary base subobject (multiple inheritance) is found -- each holder
    records the sub-object offset it points at."""
    target_rc = resolve_refcount(target)
    target_inner = target_rc.inner or (
        wrapper_inner_type(target_rc.typename) if target_rc.typename else None
    )
    # Span the whole allocation so a holder of any sub-object (a secondary base
    # at a non-zero offset-to-top) is found, not just the most-derived base word.
    extent = object_extent(target, target_rc)
    holders = []
    for slot, sub in find_pointers_into(target, extent):
        if target <= slot < target + extent:
            continue  # the object's own internal pointer into itself, not a holder
        primary, ctype, _vslot, ckind = find_enclosing_object(slot)
        h = Holder()
        h.slot = slot
        h.sub_offset = sub
        h.container = primary
        h.container_type = ctype
        h.container_refcount = None
        if ckind == "closure":
            # A vtable-less closure: named from its Run thunk, but its object base
            # (hence refcount/liveness) isn't recoverable, so don't resolve it as
            # one -- just classify the captured reference and drop the bogus base.
            h.container = None
            h.kind, h.note = classify_reference(ctype, target_inner, None, sub)
            holders.append(h)
            continue
        if primary is None:
            h.kind = "unknown"
            h.note = "no enclosing vtable found (raw stack/global?)"
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
        h.kind, h.note = classify_reference(ctype, target_inner, offset, sub)
        holders.append(h)
    return target_rc, holders


def _strong_holders_of(obj):
    """The deduped strong/ref? holders of obj (each a Holder), and the full holder
    list (for ROOT candidate reporting)."""
    _trc, holders = analyze_holders(obj)
    strong, seen = [], set()
    for h in holders:
        if h.container is None or h.kind not in ("strong", "ref?"):
            continue
        if h.container == obj or h.container in seen:
            continue
        seen.add(h.container)
        strong.append(h)
    return strong, holders


def trace_retention(start, max_depth=40, max_nodes=200, stop_on_cycle=True):
    """Follow strong holders backward with a colored DFS to find a true retention
    CYCLE. An edge (container -> obj) means "container holds obj".

    Each node is WHITE (unseen), GRAY (on the current DFS path), or BLACK (fully
    explored). A holder edge to a GRAY node is a back-edge -- a real cycle. An
    edge to a BLACK node is a DAG cross/forward edge (e.g. a diamond, where two
    holders of a node share a common ancestor) and is NOT a cycle -- the previous
    global-visited check mis-reported those. A node with no live strong holder is
    a ROOT (its real retainer is off-heap, or it's a leak anchor).
    """
    WHITE, GRAY, BLACK = 0, 1, 2
    color = {}
    results = []
    path = []          # edges (container, held, holder): start -> ... -> current node
    ctl = {"nodes": 0, "stop": False}

    def dfs(obj, depth):
        if ctl["stop"]:
            return
        color[obj] = GRAY
        try:
            rc = resolve_refcount(obj)
            if not rc.ok:
                results.append({"kind": "root", "reason": rc.error or "not ref-counted",
                                "obj": obj, "candidates": [], "path": list(path)})
                return
            ctl["nodes"] += 1
            strong, holders = _strong_holders_of(obj)
            if not strong:
                other = [h for h in holders if h.kind != "dead" and h.container is not None]
                results.append({"kind": "root", "reason": "no live strong holder found",
                                "obj": obj, "candidates": other, "path": list(path)})
                return
            if depth >= max_depth or ctl["nodes"] >= max_nodes:
                results.append({"kind": "root", "reason": "search bound reached",
                                "obj": obj, "candidates": [], "path": list(path)})
                return
            for h in strong:
                container = h.container
                edge = (container, obj, h)
                c = color.get(container, WHITE)
                if c == GRAY:
                    results.append({"kind": "cycle", "edge": edge, "path": list(path) + [edge]})
                    if stop_on_cycle:
                        ctl["stop"] = True
                        return
                elif c == WHITE:
                    path.append(edge)
                    dfs(container, depth + 1)
                    path.pop()
                    if ctl["stop"]:
                        return
                # c == BLACK: DAG cross/forward edge -- already fully explored, not a cycle.
        finally:
            color[obj] = BLACK

    dfs(start, 0)
    return results
