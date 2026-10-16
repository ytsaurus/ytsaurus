# Section and heap-zone discovery.
#
# Everything the walker needs to know about the core's memory layout is derived
# from `maintenance info sections`, so the tool stays portable across builds and
# allocator-agnostic (tcmalloc / jemalloc / glibc malloc / YTAlloc alike).

import re

import gdb

_SECTION_RE = re.compile(
    r"\s*\[\s*\d+\]\s+(0x[0-9a-f]+)->(0x[0-9a-f]+)\b.*?:\s*(\S+)\s+(.*)$"
)


class TSections:
    """Cached parse of `maintenance info sections`."""

    def __init__(self):
        self.all = []  # list of (lo, hi, name, flags)
        self._parse()

    def _parse(self):
        try:
            out = gdb.execute("maintenance info sections", to_string=True)
        except gdb.error:
            out = ""
        for line in out.splitlines():
            m = _SECTION_RE.match(line)
            if not m:
                continue
            self.all.append((int(m.group(1), 16), int(m.group(2), 16), m.group(3), m.group(4)))

    def named(self, *names):
        return [(lo, hi) for lo, hi, name, _flags in self.all if name in names]

    def code_relro_ranges(self):
        """Ranges where vtable symbols live (a vptr field value points here)."""
        return self.named(".text", ".data.rel.ro", ".rodata", ".data.rel.ro.local")

    def heap_zones(self):
        """Writable in-core regions where heap objects can live.

        Allocator-agnostic: every LOAD segment that has contents in the core
        (HAS_CONTENTS) and is writable (not READONLY). Writable .data/.bss,
        thread stacks and shared-library data get included too; that only makes
        the pointer search slower, never wrong -- a stale/stack hit is filtered
        later by container liveness. Read-only .text/.rodata/relro are excluded:
        a heap pointer is never stored there.
        """
        zones = []
        for lo, hi, _name, flags in self.all:
            if "LOAD" not in flags or "HAS_CONTENTS" not in flags:
                continue
            if "READONLY" in flags or hi <= lo:
                continue
            zones.append((lo, hi))
        zones.sort()
        merged = []
        for lo, hi in zones:
            if merged and lo <= merged[-1][1]:
                merged[-1] = (merged[-1][0], max(merged[-1][1], hi))
            else:
                merged.append((lo, hi))
        return merged


_sections = None


def sections():
    global _sections
    if _sections is None:
        _sections = TSections()
    return _sections


def in_ranges(val, ranges):
    for lo, hi in ranges:
        if lo <= val < hi:
            return True
    return False
