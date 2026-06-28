# yt-rc-dump: aggregate per-type live-object table from the RefCountedTracker.
#
# The complement to yt-rc-find: it answers "which types are leaking, and by how
# much" across the whole process, before you trace a single instance. Read-only
# and core-friendly -- the tracker singleton and its per-thread slots are read
# straight from memory (no inferior calls).
#
# The tracker (TRefCountedTracker, a LeakySingleton) keeps, per registered type
# (cookie): the global accumulated slot (threads that have exited) plus a slot in
# every live thread's local vector. Alive = allocated - freed, summed over all.

import re
import struct

import gdb

import _announce
from memory import info_symbol, read_block

# size_t fields shared by TLocalSlot (plain) and TGlobalSlot (atomic); read raw so
# both layouts work.
_SLOT_FIELDS = (
    "ObjectsAllocated", "ObjectsFreed",
    "TagObjectsAllocated", "TagObjectsFreed",
    "SpaceSizeAllocated", "SpaceSizeFreed",
)


def _u64(value):
    return int(value.cast(gdb.lookup_type("unsigned long")))


def _read_u64(addr):
    b = read_block(addr, 8)
    return struct.unpack_from("<Q", b)[0] if b else 0


def _tracker_addr():
    """Address of the in-place TRefCountedTracker (a LeakySingleton whose Storage
    holds the object). Read from the symbol table -- a non-debug symbol."""
    try:
        out = gdb.execute("info variables LeakySingleton<NYT::TRefCountedTracker>",
                          to_string=True)
    except gdb.error:
        return None
    for line in out.splitlines():
        if "Storage" in line and "guard" not in line:
            m = re.search(r"(0x[0-9a-f]+)", line)
            if m:
                return int(m.group(1), 16)
    return None


def _slot_totals(slot):
    """(objects_allocated, objects_freed, bytes_allocated, bytes_freed) for a slot,
    reading each field as a raw u64 (so atomic and plain layouts read the same)."""
    vals = {}
    for name in _SLOT_FIELDS:
        try:
            vals[name] = _read_u64(int(slot[name].address))
        except gdb.error:
            vals[name] = 0
    objs_alloc = vals["ObjectsAllocated"] + vals["TagObjectsAllocated"]
    objs_freed = vals["ObjectsFreed"] + vals["TagObjectsFreed"]
    return objs_alloc, objs_freed, vals["SpaceSizeAllocated"], vals["SpaceSizeFreed"]


def _local_slot_vectors(tracker):
    """The per-thread std::vector<TLocalSlot>* held in the AllLocalSlots_ THashSet.
    Walks the Arcadia hash table directly (bucket array + intrusive node chains)."""
    try:
        buckets = tracker["AllLocalSlots_"]["rep"]["buckets"]
        data = buckets["Data"]
    except gdb.error:
        return []
    data_addr = _u64(data)
    if data_addr == 0:
        return []
    # The bucket array is allocated with two extra slots; Data[-1] holds size+2.
    nbuckets = _read_u64(data_addr - 8) - 2
    out = []
    for b in range(max(0, nbuckets)):
        node = data[b]
        # Chains terminate at a tagged pointer (low bit set); empty buckets too.
        while (_u64(node) & 1) == 0 and _u64(node) != 0:
            nd = node.dereference()
            out.append(nd["val"])
            node = nd["next"]
    return out


def _aggregate(tracker):
    """Per-cookie totals {cookie: [obj_alloc, obj_freed, bytes_alloc, bytes_freed]},
    summing the global slots and every live thread's local slots."""
    totals = {}

    def add(cookie, t):
        r = totals.setdefault(cookie, [0, 0, 0, 0])
        for i in range(4):
            r[i] += t[i]

    gs = tracker["GlobalSlots_"]
    try:
        gbeg = gs["__begin_"]
        gn = _u64(gs["__end_"] - gbeg)
        for c in range(gn):
            add(c, _slot_totals(gbeg[c]))
    except gdb.error:
        pass

    for vec_ptr in _local_slot_vectors(tracker):
        try:
            vec = vec_ptr.dereference()
            beg = vec["__begin_"]
            n = _u64(vec["__end_"] - beg)
        except gdb.error:
            continue
        for c in range(n):
            add(c, _slot_totals(beg[c]))
    return totals


def _cookie_typename(tracker, cookie):
    """Type name for a cookie via its TRefCountedTypeKey (a std::type_info*),
    resolved by `info symbol` -> 'typeinfo for <Name>'."""
    try:
        key = tracker["CookieToKey_"]["__begin_"][cookie]
        type_info = _u64(key["TypeKey"])
    except gdb.error:
        return None
    sym = info_symbol(type_info)
    m = re.match(r"typeinfo for (.+?)(?: \+ \d+)?(?: in section .*)?$", sym)
    return m.group(1).strip() if m else None


class YtRcDump(gdb.Command):
    """yt-rc-dump [<type-substr>]: per-type live ref-counted object table from the
    RefCountedTracker, sorted by bytes alive. Optional name-substring filter."""

    def __init__(self):
        super().__init__("yt-rc-dump", gdb.COMMAND_USER)

    def invoke(self, arg, from_tty):
        substr = arg.strip()
        tracker_addr = _tracker_addr()
        if tracker_addr is None:
            print("RefCountedTracker not found (binary built without ref-counted tracking?)")
            return
        try:
            tracker = gdb.parse_and_eval("*(NYT::TRefCountedTracker*)0x%x" % tracker_addr)
        except gdb.error as e:
            # The TRefCountedTracker type needs debug info; a stripped binary has
            # the symbol (so tracker_addr resolves) but not the type.
            print("RefCountedTracker type info unavailable (%s)" % e)
            return

        rows = []
        for cookie, (oa, of, ba, bf) in _aggregate(tracker).items():
            alive = oa - of
            if alive <= 0:
                continue
            name = _cookie_typename(tracker, cookie)
            if name is None:
                continue
            if substr and substr not in name:
                continue
            rows.append((ba - bf, alive, name))
        if not rows:
            print("No live ref-counted objects%s" % (" matching %r" % substr if substr else ""))
            return
        rows.sort(reverse=True)

        limit = len(rows) if substr else min(len(rows), 50)
        total_objs = sum(r[1] for r in rows)
        total_bytes = sum(r[0] for r in rows)
        print("%-14s %12s   %s" % ("bytes alive", "objects", "type"))
        for bytes_alive, alive, name in rows[:limit]:
            print("%-14d %12d   %s" % (bytes_alive, alive, name))
        if limit < len(rows):
            print("... (%d more; pass a substring to filter)" % (len(rows) - limit))
        print("Total: %d objects, %.1f MB across %d type(s)" % (
            total_objs, total_bytes / 1e6, len(rows)))


def register():
    YtRcDump()
    _announce.command("ref-counted", "yt-rc-dump")


register()
