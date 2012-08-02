#include "zlib.h"

#include <ytlib/misc/assert.h>
#include <ytlib/misc/serialize.h>

#include <contrib/libs/zlib/zlib.h>

#include <iostream>

namespace NYT {
namespace NCodec {

////////////////////////////////////////////////////////////////////////////////

static const size_t BufferSize = 1 << 20;

void ZlibCompress(StreamSource* source, StreamSink* sink, int level)
{
    char buffer[BufferSize];

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
            stream.next_out = reinterpret_cast<Bytef*>(buffer);
            stream.avail_out = BufferSize;
            returnCode = deflate(&stream, flush);
            YCHECK(returnCode != Z_STREAM_ERROR);

            source->Skip(previousAvailable - stream.avail_in);
            sink->Append(buffer, BufferSize - stream.avail_out);
        } while(stream.avail_out == 0);
        YCHECK(stream.avail_in == 0);

    } while(flush != Z_FINISH);
    YCHECK(returnCode == Z_STREAM_END);

    deflateEnd(&stream);
}

void ZlibCompress(int level, StreamSource* source, std::vector<char>* output)
{
    output->resize(sizeof(size_t));
    {
        size_t available = source->Available();
        TMemoryOutput memoryOutput(output->data(), sizeof(available));
        WritePod(memoryOutput, available);
    }

    DynamicByteArraySink sink(output);
    ZlibCompress(source, &sink, level);
}

void ZlibDecompress(StreamSource* source, std::vector<char>* output)
{
    size_t size;
    ReadPod(source, size);
    // We add one additional byte for process correctly last inflate
    output->resize(size + 1);

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

        stream.next_out = reinterpret_cast<Bytef*>(output->data() + currentPos);
        stream.avail_out = output->size() - currentPos;
        returnCode = inflate(&stream, Z_NO_FLUSH);
        YCHECK(returnCode == Z_OK || returnCode == Z_STREAM_END);

        source->Skip(available - stream.avail_in);
        currentPos = output->size() - stream.avail_out;
    } while(returnCode != Z_STREAM_END);

    YCHECK(currentPos + 1 == output->size());
    output->resize(currentPos);

    inflateEnd(&stream);
}

////////////////////////////////////////////////////////////////////////////////

}} // namespace NYT::Ncodec
