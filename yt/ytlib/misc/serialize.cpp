#include "serialize.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Auxiliary constants and functions.
namespace {

const ui8 Padding[YTAlignment] = { 0 };

} // namespace <anonymous>

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

void WriteVarInt(ui64 value, TOutputStream* output)
{
    bool stop = false;
    while (!stop) {
        ui8 byte = static_cast<ui8> (value | 0x80);
        value >>= 7;
        if (value == 0) {
            stop = true;
            byte &= 0x7F;
        }
        output->Write(byte);
    }
}

void WriteVarInt32(i32 value, TOutputStream* output)
{
    WriteVarInt(static_cast<ui64>(ZigZagEncode32(value)), output);
}

void WriteVarInt64(i64 value, TOutputStream* output)
{
    WriteVarInt(static_cast<ui64>(ZigZagEncode64(value)), output);
}

ui64 ReadVarInt(TInputStream* input)
{
    size_t count = 0;
    ui64 result = 0;

    ui8 byte = 0;
    do {
        if (7 * count > 8 * sizeof(ui64) ) {
            // TODO: exception message
            throw yexception();
        }
        input->Read(&byte, 1);
        result |= (static_cast<ui64> (byte & 0x7F)) << (7 * count);
        ++count;
    } while (byte & 0x80);

    return result;
}

i32 ReadVarInt32(TInputStream* input)
{
    ui64 value = ReadVarInt(input);
    if (value > Max<ui32>()) {
        // TODO: exception message
        throw yexception();
    }

    return ZigZagDecode32(static_cast<ui32> (value));
}

i64 ReadVarInt64(TInputStream* input)
{
    ui64 value = ReadVarInt(input);
    return ZigZagDecode64(value);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

