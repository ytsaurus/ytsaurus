def split_lenval(value):
    """Splits a `Lenval`-framed batch: each frame is a protobuf varint64 length followed by the payload."""
    payloads = []
    position = 0
    while position < len(value):
        length = 0
        shift = 0
        while True:
            assert position < len(value), "truncated lenval varint"
            byte = value[position]
            position += 1
            length |= (byte & 0x7F) << shift
            if not byte & 0x80:
                break
            shift += 7
        assert position + length <= len(value), "truncated lenval frame"
        payloads.append(value[position : position + length])
        position += length
    return payloads


def pack_lenval(payloads):
    """Packs payloads into one `Lenval`-framed record. The inverse of split_lenval."""
    record = bytearray()
    for payload in payloads:
        length = len(payload)
        while True:
            byte = length & 0x7F
            length >>= 7
            record.append(byte | 0x80 if length else byte)
            if not length:
                break
        record += payload
    return bytes(record)
