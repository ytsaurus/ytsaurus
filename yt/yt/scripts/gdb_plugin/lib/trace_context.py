import struct

import gdb

import fiber_attribution
from memory import info_symbol, read_block, read_ptr


def _compact_vector_elements(vector):
    inline_size = int(vector["InlineMeta_"]["SizePlusOne"])
    if inline_size:
        return [vector["InlineElements_"][i] for i in range(inline_size - 1)]
    storage = vector["OnHeapMeta_"]["Storage"].dereference()
    elements = storage["Elements"]
    size = int(storage["End"] - elements)
    return [elements[i] for i in range(size)]


def _first_compressed_pair(value):
    fields = value.type.strip_typedefs().fields()
    base = value.cast(fields[0].type)
    try:
        return base["__value_"]
    except gdb.error:
        return base


def _string_rep(value):
    try:
        return value["__rep_"]
    except gdb.error:
        return _first_compressed_pair(value["__r_"])


def _read_std_string(value):
    representation = _string_rep(value)
    short = representation["__s"]
    if short["__is_long_"]:
        data = representation["__l"]["__data_"]
        size = int(representation["__l"]["__size_"])
    else:
        data = short["__data_"]
        size = int(short["__size_"])
    try:
        address = int(data)
    except gdb.error:
        address = int(data.address)
    return read_block(address, size) or b""


def _decode_logging_tags(logging_tags):
    # Keep this in sync with TTaggedPayloadReader:
    # library/cpp/yt/logging/tagged_payload.cpp
    payload = _read_std_string(logging_tags["Payload_"]["Underlying_"])
    offset = 0
    result = []
    while offset < len(payload):
        if offset + 4 > len(payload):
            raise ValueError("truncated logging-tag key size")
        key_size = struct.unpack_from("<I", payload, offset)[0] & 0x7fffffff
        offset += 4
        if offset + key_size + 4 > len(payload):
            raise ValueError("truncated logging-tag key")
        key = payload[offset:offset + key_size].decode("utf-8", "replace")
        offset += key_size
        value_size = struct.unpack_from("<I", payload, offset)[0]
        offset += 4
        if offset + value_size > len(payload):
            raise ValueError("truncated logging-tag value")
        value = payload[offset:offset + value_size].decode("utf-8", "replace")
        offset += value_size
        result.append((key, value))
    return result


def _trace_context_from_storage(storage_addr):
    storage_type = gdb.lookup_type("NYT::NConcurrency::TPropagatingStorage")
    storage = gdb.Value(storage_addr).cast(storage_type.pointer()).dereference()
    impl_ptr = storage["Impl_"]["T_"]
    if int(impl_ptr) == 0:
        return None
    data = impl_ptr.dereference()["Data_"]["Storage_"]
    trace_ptr_type = gdb.lookup_type("NYT::NTracing::TTraceContextPtr")
    for item in _compact_vector_elements(data):
        any_value = item["second"]
        handler = int(any_value["__h_"])
        if "TTraceContext" not in info_symbol(handler):
            continue
        trace_ptr = any_value["__s_"].address.cast(trace_ptr_type.pointer()).dereference()
        raw = trace_ptr["T_"]
        return None if int(raw) == 0 else raw.dereference()
    return None


def _trace_context_from_bind_state(bind_state):
    address = int(bind_state)
    if not address:
        return None, True
    vptr = read_ptr(address)
    symbol = info_symbol(vptr) if vptr is not None else ""
    if "TBindState<true" not in symbol:
        return None, True
    base_size = gdb.lookup_type("NYT::NDetail::TBindStateBase").sizeof
    return _trace_context_from_storage(address + base_size), True


def find_fiber_trace_context(fib):
    """Return the propagated TTraceContext of a parked fiber, read-only."""
    seed = fiber_attribution._seed_regs_for(fib)
    leaf, saved_thread, saved_frame = fiber_attribution._arm_seed(seed)
    try:
        frame = leaf
        while frame is not None:
            try:
                trace_context, found = _trace_context_from_bind_state(
                    frame.read_var("fiberBindState"))
            except (gdb.error, ValueError, TypeError):
                found = False
                trace_context = None
            if found:
                return trace_context
            frame = frame.older()
        return None
    finally:
        fiber_attribution._restore_seed(saved_thread, saved_frame)


def trace_context_tags(trace_context):
    """Return (logging_tags, tracing_tags) as lists of string pairs."""
    logging_tags = _decode_logging_tags(trace_context["LoggingTags_"])
    tracing_tags = []
    for item in _compact_vector_elements(trace_context["Tags_"]):
        key = _read_std_string(item["first"]).decode("utf-8", "replace")
        value = _read_std_string(item["second"]).decode("utf-8", "replace")
        tracing_tags.append((key, value))
    return logging_tags, tracing_tags
