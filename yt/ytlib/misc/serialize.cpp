#include "stdafx.h"
#include "serialize.h"
#include <ytlib/logging/log.h>

namespace NYT {

static NLog::TLogger Logger("Serialize");

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
            LOG_FATAL("The data is too long to read ui64");
        }
        if (input->Read(&byte, 1) != 1) {
            LOG_FATAL("The data is too short to read ui64");
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
        LOG_FATAL("Value %" PRIx64 " is too large to read ui32", varInt);
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

TSharedRef PackRefs(const std::vector<TSharedRef>& refs)
{
    i64 size = 0;

    // Number of bytes to hold vector size.
    size += sizeof(i32);
    // Number of bytes to hold ref sizes.
    size += sizeof(i64) * refs.size();
    // Number of bytes to hold refs.
    FOREACH (const auto& ref, refs) {
        size += ref.Size();
    }

    TBlob blob(size);
    TMemoryOutput output(&*blob.begin(), blob.size());

    WritePod(output, static_cast<i32>(refs.size()));
    FOREACH (const auto& ref, refs) {
        WritePod(output, static_cast<i64>(ref.Size()));
        Write(output, ref);
    }

    return TSharedRef(MoveRV(blob));
}

void UnpackRefs(const TSharedRef& packedRef, std::vector<TSharedRef>* refs)
{
    TMemoryInput input(packedRef.Begin(), packedRef.Size());

    i32 refCount;
    ReadPod(input, refCount);
    YCHECK(refCount >= 0);

    *refs = std::vector<TSharedRef>(refCount);
    for (i32 i = 0; i < refCount; ++i) {
        i64 refSize;
        ReadPod(input, refSize);
        TRef ref(const_cast<char*>(input.Buf()), static_cast<size_t>(refSize));
        (*refs)[i] = TSharedRef(packedRef, ref);
        input.Skip(refSize);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

