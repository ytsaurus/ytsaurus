#pragma once

#include <util/system/defaults.h>

class TOutputStream;
class TInputStream;

namespace NYT {
namespace NYson {

////////////////////////////////////////////////////////////////////////////////

// Various functions that read/write varints from/to a stream.

// Returns the number of bytes written.
int WriteVarUInt64(TOutputStream* output, ui64 value);
int WriteVarInt32(TOutputStream* output, i32 value);
int WriteVarInt64(TOutputStream* output, i64 value);

// Returns the number of bytes read.
int ReadVarUInt64(TInputStream* input, ui64* value);
int ReadVarInt32(TInputStream* input, i32* value);
int ReadVarInt64(TInputStream* input, i64* value);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYson
} // namespace NYT
