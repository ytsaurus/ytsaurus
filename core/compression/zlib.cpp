#include "zlib.h"

#include <yt/core/misc/assert.h>
#include <yt/core/misc/error.h>
#include <yt/core/misc/serialize.h>
#include <yt/core/misc/finally.h>

#include <contrib/libs/zlib/zlib.h>

#include <array>
#include <iostream>

namespace NYT::NCompression {

////////////////////////////////////////////////////////////////////////////////

static constexpr size_t MaxZlibUInt = std::numeric_limits<uInt>::max();

////////////////////////////////////////////////////////////////////////////////

void ZlibCompress(int level, StreamSource* source, TBlob* output)
{
    z_stream stream;
    stream.zalloc = Z_NULL;
    stream.zfree = Z_NULL;
    stream.opaque = Z_NULL;

    int returnCode = deflateInit(&stream, level);
    YCHECK(returnCode == Z_OK);
    auto cleanupGuard = Finally([&] () { deflateEnd(&stream); });

    size_t totalUncompressedSize = source->Available();
    size_t totalCompressedSize = deflateBound(&stream, totalUncompressedSize);

    // Write out header.
    output->Reserve(sizeof(ui64) + totalCompressedSize);
    output->Resize(sizeof(ui64), false);
    {
        TMemoryOutput memoryOutput(output->Begin(), sizeof(ui64));
        WritePod(memoryOutput, static_cast<ui64>(totalUncompressedSize)); // Force serialization type.
    }

    // Write out body.
    do {
        size_t inputAvailable;
        const char* inputNext = source->Peek(&inputAvailable);
        inputAvailable = std::min(inputAvailable, source->Available());
        inputAvailable = std::min(inputAvailable, MaxZlibUInt);

        stream.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(inputNext));
        stream.avail_in = static_cast<uInt>(inputAvailable);

        int flush = (stream.avail_in == source->Available()) ? Z_FINISH : Z_NO_FLUSH;

        do {
            size_t outputAvailable = output->Capacity() - output->Size();
            if (outputAvailable == 0) {
                // XXX(sandello): Somehow we have missed our buffer estimate.
                // Reallocate it to provide enough capacity.
                size_t sourceAvailable = source->Available() - inputAvailable + stream.avail_in;
                outputAvailable = deflateBound(&stream, sourceAvailable);
                output->Reserve(output->Size() + outputAvailable);
            }
            outputAvailable = std::min(outputAvailable, MaxZlibUInt);

            stream.next_out = reinterpret_cast<Bytef*>(output->End());
            stream.avail_out = static_cast<uInt>(outputAvailable);

            returnCode = deflate(&stream, flush);
            // We should not throw exception here since caller does not expect such behavior.
            YCHECK(returnCode == Z_OK || returnCode == Z_STREAM_END);

            output->Resize(output->Size() + outputAvailable - stream.avail_out, false);
        } while (stream.avail_out == 0);

        // Entire input was consumed.
        YCHECK(stream.avail_in == 0);
        source->Skip(inputAvailable - stream.avail_in);
    } while (source->Available() > 0);

    YCHECK(returnCode == Z_STREAM_END);
}

void ZlibDecompress(StreamSource* source, TBlob* output)
{
    if (source->Available() == 0) {
        return;
    }

    // Read header.
    ui64 totalUncompressedSize = 0;
    ReadPod(source, totalUncompressedSize);

    // We add one additional byte to process correctly last inflate.
    output->Reserve(totalUncompressedSize + 1);

    z_stream stream;
    stream.zalloc = Z_NULL;
    stream.zfree = Z_NULL;
    stream.opaque = Z_NULL;

    int returnCode = inflateInit(&stream);
    YCHECK(returnCode == Z_OK);
    auto cleanupGuard = Finally([&] () { inflateEnd(&stream); });

    // Read body.
    do {
        size_t inputAvailable;
        const char* inputNext = source->Peek(&inputAvailable);
        inputAvailable = std::min(inputAvailable, source->Available());
        inputAvailable = std::min(inputAvailable, MaxZlibUInt);

        stream.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(inputNext));
        stream.avail_in = static_cast<uInt>(inputAvailable);

        int flush = (stream.avail_in == source->Available()) ? Z_FINISH : Z_NO_FLUSH;

        size_t outputAvailable = output->Capacity() - output->Size();
        outputAvailable = std::min(outputAvailable, MaxZlibUInt);

        stream.next_out = reinterpret_cast<Bytef*>(output->End());
        stream.avail_out = static_cast<uInt>(outputAvailable);

        returnCode = inflate(&stream, flush);
        // We should not throw exception here since caller does not expect such behavior.
        YCHECK(returnCode == Z_OK || returnCode == Z_STREAM_END);

        source->Skip(inputAvailable - stream.avail_in);

        output->Resize(output->Size() + outputAvailable - stream.avail_out, false);
    } while (returnCode != Z_STREAM_END);

    YCHECK(source->Available() == 0);
    YCHECK(output->Size() == totalUncompressedSize);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCompression
