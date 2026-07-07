# tcmalloc allocation introspection.
#
# Given any pointer, recover the start and size of the tcmalloc allocation that
# contains it, by reading tcmalloc's radix page map (Static::pagemap_) and
# size-class table straight from the core -- read-only, no inferior calls.
#
# The page map yields, per page, a Span* and a size class. A span covers a run of
# pages; for a small size class the span is carved into equal objects of
# class_to_size, so the containing object's start is
#   span_start + floor((addr - span_start) / object_size) * object_size.
# For size class 0 (a large / single allocation) the whole span is one object.
#
# Geometry (page shift, radix fan-out) is read from the page-map type itself, so
# this adapts to the build's tcmalloc configuration rather than hard-coding it.

import gdb

import _announce
from memory import gdb_type

_PAGEMAP = "tcmalloc::tcmalloc_internal::Static::pagemap_.map_"
_SIZEMAP = "tcmalloc::tcmalloc_internal::Static::sizemap_"

_cfg = None  # None = not probed, False = unavailable, dict = ready


def _ilog2(n):
    return n.bit_length() - 1


def _config():
    """Lazily read the page-map geometry and handles. Returns a dict, or None when
    the binary isn't tcmalloc / the symbols aren't present."""
    global _cfg
    if _cfg is not None:
        return _cfg or None
    try:
        root = gdb.parse_and_eval(_PAGEMAP)["root_"]
        sizemap = gdb.parse_and_eval(_SIZEMAP)
    except gdb.error:
        _cfg = False
        return None
    try:
        root_len = int(root.type.range()[1]) + 1          # kRootLength
        leaf_type = root.type.target().target()           # Leaf* -> Leaf
        leaf_len = None
        for f in leaf_type.fields():
            if f.name == "span":
                leaf_len = int(f.type.range()[1]) + 1      # kLeafLength
        if not leaf_len:
            raise gdb.error("no span[] in Leaf")
        try:
            addr_bits = int(gdb.parse_and_eval("tcmalloc::tcmalloc_internal::kAddressBits"))
        except gdb.error:
            addr_bits = 48
        leaf_bits = _ilog2(leaf_len)
        # PageMap2<BITS> with BITS = addr_bits - page_shift = kRootBits + kLeafBits.
        page_shift = addr_bits - (_ilog2(root_len) + leaf_bits)
    except (gdb.error, TypeError):
        _cfg = False
        return None
    _cfg = {
        "root": root,
        "sizemap": sizemap,
        "root_len": root_len,
        "leaf_bits": leaf_bits,
        "leaf_mask": leaf_len - 1,
        "page_shift": page_shift,
    }
    return _cfg


def available():
    return _config() is not None


def _span_of(addr):
    """(span_value, size_class) for the page containing #addr, or (None, None)."""
    cfg = _config()
    if cfg is None:
        return None, None
    try:
        page = addr >> cfg["page_shift"]
        i1 = page >> cfg["leaf_bits"]
        if i1 >= cfg["root_len"]:
            return None, None
        leaf = cfg["root"][i1]
        if int(leaf) == 0:
            return None, None
        i2 = page & cfg["leaf_mask"]
        leaf = leaf.dereference()
        span = leaf["span"][i2]
        if int(span) == 0:
            return None, None
        return span.dereference(), int(leaf["sizeclass"][i2])
    except gdb.error:
        return None, None


def allocation_of(addr):
    """(start, size) of the tcmalloc allocation containing #addr, or None when the
    address isn't in a tcmalloc small/large span (metadata, stack, unmapped)."""
    cfg = _config()
    if cfg is None:
        return None
    span, sc = _span_of(addr)
    if span is None:
        return None
    try:
        shift = cfg["page_shift"]
        span_start = int(span["first_page_"]) << shift
        if sc != 0:
            # A size-class span carved into equal objects (a sampled small object
            # also lands here, as a single object at the span start).
            obj_size = int(cfg["sizemap"]["class_to_size_"][sc])
            if obj_size <= 0 or addr < span_start:
                return None
            idx = (addr - span_start) // obj_size
            return span_start + idx * obj_size, obj_size
        # size class 0 -> a large or sampled single-object span. num_pages lives in
        # large_or_sampled_state_ whenever is_large_span_ OR sampled_ is set (this
        # is exactly Span::is_large_or_sampled(), which Span::num_pages() keys on).
        large_or_sampled = int(span["is_large_span_"]) or int(span["sampled_"])
        state = span["large_or_sampled_state_"] if large_or_sampled else span["small_span_state_"]
        return span_start, int(state["num_pages"]) << shift
    except gdb.error:
        return None


class YtTcmallocBlock(gdb.Command):
    """yt-tcmalloc-block <addr>: start, size and size class of the tcmalloc
    allocation that contains addr (read straight from the page map; core-safe)."""

    def __init__(self):
        super().__init__("yt-tcmalloc-block", gdb.COMMAND_USER)

    def invoke(self, arg, from_tty):
        arg = arg.strip()
        if not arg:
            raise gdb.GdbError("Expected an address argument")
        addr = int(gdb.parse_and_eval(arg).cast(gdb_type("unsigned long")))
        if not available():
            print("No tcmalloc page map found (binary not built with tcmalloc?)")
            return
        blk = allocation_of(addr)
        if blk is None:
            print("0x%x is not in a tcmalloc span (metadata / stack / unmapped?)" % addr)
            return
        start, size = blk
        _span, sc = _span_of(addr)
        print("addr     0x%x" % addr)
        print("start    0x%x" % start)
        print("size     %d bytes" % size)
        print("offset   +0x%x" % (addr - start))
        print("end      0x%x" % (start + size))
        print("class    %s" % ("%d (small)" % sc if sc else "0 (large/single span)"))


def register():
    YtTcmallocBlock()
    _announce.command("tcmalloc", "yt-tcmalloc-block")


register()
