#include "stdafx.h"
#include "varint.h"
#include "zigzag.h"

#include <core/misc/error.h>

#include <util/stream/output.h>
#include <util/stream/input.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TWriteCallback>
int WriteVarUInt64Impl(TWriteCallback doWrite, ui64 value)
{
    bool stop = false;
    int bytesWritten = 0;
    while (!stop) {
        ++bytesWritten;
        ui8 byte = static_cast<ui8> (value | 0x80);
        value >>= 7;
        if (value == 0) {
            stop = true;
            byte &= 0x7F;
        }
        doWrite(byte);
    }
    return bytesWritten;
}

// These are optimized versions of these Read/Write functions in protobuf/io/coded_stream.cc.
int WriteVarUInt64(TOutputStream* output, ui64 value)
{
    return WriteVarUInt64Impl([&] (ui8 byte) {
        output->Write(byte);
    }, value);
}

int WriteVarUInt64(char* output, ui64 value)
{
    return WriteVarUInt64Impl([&] (ui8 byte) {
        *output = byte;
        ++output;
    }, value);
}

////////////////////////////////////////////////////////////////////////////////

template <class TOutput>
int WriteVarUInt32Impl(TOutput output, ui32 value)
{
    return WriteVarUInt64(output, static_cast<ui64>(value));
}

int WriteVarUInt32(TOutputStream* output, ui32 value)
{
    return WriteVarUInt32Impl(output, value);
}

int WriteVarUInt32(char* output, ui32 value)
{
    return WriteVarUInt32Impl(output, value);
}

////////////////////////////////////////////////////////////////////////////////

template <class TOutput>
int WriteVarInt32Impl(TOutput output, i32 value)
{
    return WriteVarUInt64(output, static_cast<ui64>(ZigZagEncode32(value)));
}

int WriteVarInt32(TOutputStream* output, i32 value)
{
    return WriteVarInt32Impl(output, value);
}

int WriteVarInt32(char* output, i32 value)
{
    return WriteVarInt32Impl(output, value);
}

////////////////////////////////////////////////////////////////////////////////

template <class TOutput>
int WriteVarInt64Impl(TOutput output, i64 value)
{
    return WriteVarUInt64(output, static_cast<ui64>(ZigZagEncode64(value)));
}

int WriteVarInt64(TOutputStream* output, i64 value)
{
    return WriteVarInt64Impl(output, value);
}

int WriteVarInt64(char* output, i64 value)
{
    return WriteVarInt64Impl(output, value);
}

////////////////////////////////////////////////////////////////////////////////

template <class TReadCallback>
int ReadVarUInt64Impl(TReadCallback doRead, ui64* value)
{
    size_t count = 0;
    ui64 result = 0;

    ui8 byte;
    do {
        if (7 * count > 8 * sizeof(ui64) ) {
            THROW_ERROR_EXCEPTION("Value is too big for ui64");
        }
        byte = doRead();
        result |= (static_cast<ui64> (byte & 0x7F)) << (7 * count);
        ++count;
    } while (byte & 0x80);

    *value = result;
    return count;
}

int ReadVarUInt64(TInputStream* input, ui64* value)
{
    return ReadVarUInt64Impl([&] () {
        char byte;
        if (input->Read(&byte, 1) != 1) {
            THROW_ERROR_EXCEPTION("Premature end of stream while reading ui64");
        }
        return byte;
    }, value);
}

int ReadVarUInt64(const char* input, ui64* value)
{
    return ReadVarUInt64Impl([&] () {
        char byte = *input;
        ++input;
        return byte;
    }, value);
}

////////////////////////////////////////////////////////////////////////////////

template <class TInput>
int ReadVarUInt32Impl(TInput input, ui32* value)
{
    ui64 varInt;
    int bytesRead = ReadVarUInt64(input, &varInt);
    if (varInt > std::numeric_limits<ui32>::max()) {
        THROW_ERROR_EXCEPTION("Value is too big for ui32");
    }
    *value = static_cast<ui32>(varInt);
    return bytesRead;
}

int ReadVarUInt32(TInputStream* input, ui32* value)
{
    return ReadVarUInt32Impl(input, value);
}

int ReadVarUInt32(const char* input, ui32* value)
{
    return ReadVarUInt32Impl(input, value);
}

////////////////////////////////////////////////////////////////////////////////

template <class TInput>
int ReadVarInt32Impl(TInput input, i32* value)
{
    ui64 varInt;
    int bytesRead = ReadVarUInt64(input, &varInt);
    if (varInt > std::numeric_limits<ui32>::max()) {
        THROW_ERROR_EXCEPTION("Value is too big for i32");
    }
    *value = ZigZagDecode32(static_cast<ui32>(varInt));
    return bytesRead;
}

int ReadVarInt32(TInputStream* input, i32* value)
{
    return ReadVarInt32Impl(input, value);
}

int ReadVarInt32(const char* input, i32* value)
{
    return ReadVarInt32Impl(input, value);
}

////////////////////////////////////////////////////////////////////////////////

template <class TInput>
int ReadVarInt64Impl(TInput input, i64* value)
{
    ui64 varInt;
    int bytesRead = ReadVarUInt64(input, &varInt);
    *value = ZigZagDecode64(varInt);
    return bytesRead;
}

int ReadVarInt64(TInputStream* input, i64* value)
{
    return ReadVarInt64Impl(input, value);
}

int ReadVarInt64(const char* input, i64* value)
{
    return ReadVarInt64Impl(input, value);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
