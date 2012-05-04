#include "stdafx.h"
#include "serialize.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Auxiliary constants and functions.
namespace {

const ui8 Padding[YTAlignment] = { 0 };

} // namespace

int GetPaddingSize(i64 size)
{
    int result = static_cast<int>(size % YTAlignment);
    return result == 0 ? 0 : YTAlignment - result;
}

i64 AlignUp(i64 size)
{
    return size + GetPaddingSize(size);
}

i32 AlignUp(i32 size)
{
    return size + GetPaddingSize(size);
}

void WritePadding(TOutputStream& output, i64 recordSize)
{
    output.Write(&Padding, GetPaddingSize(recordSize));
}

void WritePadding(TFile& output, i64 recordSize)
{
    output.Write(&Padding, GetPaddingSize(recordSize));
}

////////////////////////////////////////////////////////////////////////////////

// There are optimized versions of these Read/Write functions in protobuf/io/coded_stream.cc.
int WriteVarUInt64(TOutputStream* output, ui64 value)
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
        output->Write(byte);
    }
    return bytesWritten;
}

int WriteVarInt32(TOutputStream* output, i32 value)
{
    return WriteVarUInt64(output, static_cast<ui64>(ZigZagEncode32(value)));
}

int WriteVarInt64(TOutputStream* output, i64 value)
{
    return WriteVarUInt64(output, static_cast<ui64>(ZigZagEncode64(value)));
}

int ReadVarUInt64(TInputStream* input, ui64* value)
{
    size_t count = 0;
    ui64 result = 0;

    ui8 byte = 0;
    do {
        if (7 * count > 8 * sizeof(ui64) ) {
            ythrow yexception() << Sprintf("The data is too long to read ui64");
        }
        if (input->Read(&byte, 1) != 1) {
            ythrow yexception() << Sprintf("The data is short to read ui64");
        }
        result |= (static_cast<ui64> (byte & 0x7F)) << (7 * count);
        ++count;
    } while (byte & 0x80);

    *value = result;
    return count;
}

int ReadVarInt32(TInputStream* input, i32* value)
{
    ui64 varInt;
    int bytesRead = ReadVarUInt64(input, &varInt);
    if (varInt > Max<ui32>()) {
        ythrow yexception() << Sprintf("Value %" PRIx64 " is to large to parse as ui32", varInt);
    }
    *value = ZigZagDecode32(static_cast<ui32> (varInt));
    return bytesRead;
}

int ReadVarInt64(TInputStream* input, i64* value)
{
    ui64 varInt;
    int bytesRead = ReadVarUInt64(input, &varInt);
    *value = ZigZagDecode64(varInt);
    return bytesRead;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

