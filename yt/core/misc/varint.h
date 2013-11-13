#pragma once

#include <util/system/defaults.h>

class TOutputStream;
class TInputStream;

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

const size_t MaxVarintSize = (8 * sizeof(ui64) - 1) / 7 + 1;

// Various functions to read/write varints.

// Returns the number of bytes written.
int WriteVarUInt64(TOutputStream* output, ui64 value);
int WriteVarUInt32(TOutputStream* output, ui32 value);
int WriteVarInt32(TOutputStream* output, i32 value);
int WriteVarInt64(TOutputStream* output, i64 value);

int WriteVarUInt64(char* output, ui64 value);
int WriteVarUInt32(char* output, ui32 value);
int WriteVarInt32(char* output, i32 value);
int WriteVarInt64(char* output, i64 value);

// Returns the number of bytes read.
int ReadVarUInt64(TInputStream* input, ui64* value);
int ReadVarUInt32(TInputStream* input, ui32* value);
int ReadVarInt32(TInputStream* input, i32* value);
int ReadVarInt64(TInputStream* input, i64* value);

int ReadVarUInt64(const char* input, ui64* value);
int ReadVarUInt32(const char* input, ui32* value);
int ReadVarInt32(const char* input, i32* value);
int ReadVarInt64(const char* input, i64* value);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
