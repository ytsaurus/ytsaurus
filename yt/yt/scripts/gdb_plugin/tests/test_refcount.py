from yt_commands import authors

from gdb_test_lib import get_core, analyze, assert_contains, assert_absent, assert_search

import re


@authors("babenko")
def test_obj():
    ctx, path = get_core()
    out = analyze(ctx, path, "yt-rc-obj GdbCycleHeadAddress")
    # Type resolved from the vptr, counter located via the address-salted signature.
    assert_contains(out, "TGdbCycleHead")
    assert_search(r"alive\s+yes", out)


@authors("babenko")
def test_cycle():
    ctx, path = get_core()
    out = analyze(ctx, path, "yt-rc-alive GdbCycleHeadAddress")
    # head -> tail -> head must close as a cycle (a back-edge to a node already on
    # the DFS path).
    assert_contains(out, "CYCLE", "already on the path")


@authors("babenko")
def test_diamond_is_not_a_cycle():
    ctx, path = get_core()
    out = analyze(ctx, path, "yt-rc-alive GdbDagSinkAddress")
    # The sink is held by two mids that share one top -- a DAG convergence, not a
    # cycle. The colored DFS must treat the second path to `top` as a cross-edge
    # and report a ROOT, never a (bogus) CYCLE.
    assert_absent(out, "CYCLE")
    assert_contains(out, "ROOT")


@authors("babenko")
def test_backref():
    ctx, path = get_core()
    out = analyze(ctx, path, "yt-rc-backref GdbCycleHeadAddress")
    # The tail object holds the head via its intrusive-ptr member.
    assert_contains(out, "TGdbCycleTail")


@authors("babenko")
def test_find():
    ctx, path = get_core()
    out = analyze(ctx, path, "yt-rc-find TGdbLiveSolo")
    assert_contains(out, "TGdbLiveSolo")


@authors("babenko")
def test_fiber_unwind():
    ctx, path = get_core()
    # The object is pinned only by a parked fiber's stack -> off-heap attribution
    # must fire and the fiber must unwind to a clean, recognizable chain. This
    # exercises the rbp-walk path in debug builds (frame pointers) and the stack
    # scan in release builds (frame pointers omitted).
    out = analyze(ctx, path, "yt-rc-backref GdbFiberHeldAddress")
    assert_contains(out, "parked fiber", "WaitUntilSet", "FiberTrampoline")


@authors("babenko")
def test_thread_unwind():
    ctx, path = get_core()
    # The object is pinned only by a running thread's stack (a main() local) ->
    # attributed to that thread with its native backtrace.
    out = analyze(ctx, path, "yt-rc-backref GdbThreadHeldAddress")
    assert_contains(out, "running thread", "StopHere")


@authors("babenko")
def test_final_type():
    ctx, path = get_core()
    # New<T> for a final, non-TRefCounted type lays the counter before the object
    # (no virtual-base cast). The walker must still identify it. (Counts/liveness
    # come from the signature, exercised in debug / signature-enabled builds; the
    # signature mechanism itself is unit-tested in intrusive_ptr_ut.cpp.)
    out = analyze(ctx, path, "yt-rc-obj GdbFinalAddress")
    assert_contains(out, "TGdbFinalThing")


@authors("babenko")
def test_atomic_intrusive_ptr():
    ctx, path = get_core()
    # The object is held via TAtomicIntrusivePtr, which packs a local refcount
    # into the pointer's top bits -- the holder must still be found (low-48-bit
    # match) and attributed to its container.
    out = analyze(ctx, path, "yt-rc-backref GdbAtomicHeldAddress")
    assert_contains(out, "TGdbAtomicHolder")


@authors("babenko")
def test_cycle_root():
    ctx, path = get_core()
    # Tracing an object with no live heap holder bottoms out at a ROOT and is
    # attributed off-heap (here: a running thread's stack).
    out = analyze(ctx, path, "yt-rc-alive GdbThreadHeldAddress")
    assert_contains(out, "ROOT", "running thread")


@authors("babenko")
def test_find_none():
    ctx, path = get_core()
    out = analyze(ctx, path, "yt-rc-find ThisTypeDoesNotExistAnywhere")
    assert_contains(out, "None found")


@authors("babenko")
def test_weak_holder_classified():
    ctx, path = get_core()
    # The child holds the parent via a TWeakPtr member. The holder must be
    # classified 'weak' (from the container's real field type), not a candidate
    # strong ref -- otherwise a weak back-edge would fabricate a retention cycle.
    out = analyze(ctx, path, "yt-rc-backref GdbWeakParentAddress")
    assert_search(r"weak\b.*TGdbWeakChild", out)


@authors("babenko")
def test_weak_edge_not_a_cycle():
    ctx, path = get_core()
    # Parent --strong--> Child --weak--> Parent. The weak edge must NOT close a
    # cycle: tracing the child bottoms out at a ROOT, never a CYCLE.
    out = analyze(ctx, path, "yt-rc-alive GdbWeakChildAddress")
    assert_contains(out, "ROOT")
    assert_absent(out, "CYCLE")


@authors("babenko")
def test_backref_secondary_base_subobject():
    ctx, path = get_core()
    # The object is reachable only through its secondary IGdbBeta base, at a
    # non-zero sub-object offset -- a base-address-only scan misses the holder.
    # The full-extent scan must find it, classify it strong (from the holder's
    # real TIntrusivePtr<IGdbBeta> field), and tag the sub-object offset.
    out = analyze(ctx, path, "yt-rc-backref GdbMultiAddress")
    assert_contains(out, "TGdbBetaHolder", "via subobject")
    assert_search(r"strong\b.*TGdbBetaHolder", out)


@authors("babenko")
def test_backref_bind_closure_capture():
    ctx, path = get_core()
    # A BIND closure bound-captures a strong ref to its target. The holder is a
    # TBindState; the walker must name it and classify the captured strong ref
    # (rather than dropping it as noise/unknown).
    out = analyze(ctx, path, "yt-rc-backref GdbClosureHeldAddress")
    assert_contains(out, "TBindState")


@authors("babenko")
def test_rc_dump():
    ctx, path = get_core()
    # Aggregate per-type live table from the RefCountedTracker. Fiber/thread
    # execution stacks are always live, so they anchor the assertion.
    out = analyze(ctx, path, "yt-rc-dump")
    assert_contains(out, "bytes alive", "TExecutionStack")


@authors("babenko")
def test_rc_dump_filter():
    ctx, path = get_core()
    # The fixture builds owning rows from a row buffer tagged TOwningRowTag.
    out = analyze(ctx, path, "yt-rc-dump OwningRowTag")
    assert_contains(out, "TOwningRowTag")


@authors("babenko")
def test_virtual_inheritance():
    ctx, path = get_core()
    # TRefCounted is a shared *virtual* base of the diamond, so the counter sits
    # at a runtime vbase offset. Resolution must find it both from the
    # most-derived pointer and from an interior pointer to the virtual base.
    out_derived = analyze(ctx, path, "yt-rc-obj GdbDiamondAddress")
    out_base = analyze(ctx, path, "yt-rc-obj GdbDiamondBaseAddress")
    for out in (out_derived, out_base):
        assert_contains(out, "TGdbDiamond")
        assert_search(r"alive\s+yes", out)
    # Both pointers must resolve to the very same counter.
    m1 = re.search(r"counter\s+(0x[0-9a-f]+)", out_derived)
    m2 = re.search(r"counter\s+(0x[0-9a-f]+)", out_base)
    assert m1 and m2 and m1.group(1) == m2.group(1), \
        "counters differ:\n--- derived ---\n%s\n--- base ---\n%s" % (out_derived, out_base)
