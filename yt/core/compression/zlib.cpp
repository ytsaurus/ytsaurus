#include "zlib.h"

#include <core/misc/assert.h>
#include <core/misc/error.h>
#include <core/misc/serialize.h>

#include <contrib/libs/zlib/zlib.h>

#include <iostream>
#include <array>

namespace NYT {
namespace NCompression {

////////////////////////////////////////////////////////////////////////////////

static const size_t BufferSize = 1 << 16;

void ZlibCompress(StreamSource* source, StreamSink* sink, int level)
{
    std::array<char, BufferSize> buffer;

    z_stream stream;
    stream.zalloc = Z_NULL;
    stream.zfree = Z_NULL;
    stream.opaque = Z_NULL;

    int flush, returnCode;
    returnCode = deflateInit(&stream, level);
    YCHECK(returnCode == Z_OK);

    do {
        size_t available;
        stream.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(source->Peek(&available)));
        stream.avail_in = available;
        flush = (stream.avail_in == source->Available()) ? Z_FINISH : Z_NO_FLUSH;
        do {
            size_t previousAvailable = stream.avail_in;
            stream.next_out = reinterpret_cast<Bytef*>(buffer.data());
            stream.avail_out = BufferSize;
            returnCode = deflate(&stream, flush);
            YCHECK(returnCode != Z_STREAM_ERROR);

            source->Skip(previousAvailable - stream.avail_in);
            sink->Append(buffer.data(), BufferSize - stream.avail_out);
        } while (stream.avail_out == 0);
        YCHECK(stream.avail_in == 0);

    } while (flush != Z_FINISH);
    YCHECK(returnCode == Z_STREAM_END);

    deflateEnd(&stream);
}

void ZlibCompress(int level, StreamSource* source, TBlob* output)
{
    output->Resize(sizeof(size_t));
    {
        size_t available = source->Available();
        TMemoryOutput memoryOutput(output->Begin(), sizeof(available));
        WritePod(memoryOutput, available);
    }

    TDynamicByteArraySink sink(output);
    ZlibCompress(source, &sink, level);
}

void ZlibDecompress(StreamSource* source, TBlob* output)
{
    size_t size;
    ReadPod(source, size);
    // We add one additional byte for process correctly last inflate
    output->Resize(size + 1, false);

    z_stream stream;
    stream.zalloc = Z_NULL;
    stream.zfree = Z_NULL;
    stream.opaque = Z_NULL;

    int returnCode;
    returnCode = inflateInit(&stream);
    YCHECK(returnCode == Z_OK);

    size_t currentPos = 0;
    do {
        size_t available;
        stream.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(source->Peek(&available)));
        stream.avail_in = available;
        if (available == 0) {
            break;
        }

        stream.next_out = reinterpret_cast<Bytef*>(output->Begin() + currentPos);
        stream.avail_out = output->Size() - currentPos;
        returnCode = inflate(&stream, Z_NO_FLUSH);
        if (!(returnCode == Z_OK || returnCode == Z_STREAM_END)) {
            THROW_ERROR_EXCEPTION("Zlib decompression failed: %v", stream.msg);
        }

        source->Skip(available - stream.avail_in);
        currentPos = output->Size() - stream.avail_out;
    } while (returnCode != Z_STREAM_END);

    YCHECK(currentPos + 1 == output->Size());
    output->Resize(currentPos);

    inflateEnd(&stream);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCompression
} // namespace NYT
