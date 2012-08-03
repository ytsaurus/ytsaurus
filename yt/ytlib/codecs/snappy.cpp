#include "snappy.h"

#include <contrib/libs/snappy/snappy.h>
#include <contrib/libs/snappy/snappy-stubs-internal.h>

namespace NYT {
namespace NCodec {

////////////////////////////////////////////////////////////////////////////////

void SnappyCompress(StreamSource* source, std::vector<char>* output)
{
    output->resize(snappy::MaxCompressedLength(source->Available()));
    snappy::UncheckedByteArraySink writer(output->data());
    size_t compressedSize = snappy::Compress(source, &writer);
    output->resize(compressedSize);
}

void SnappyDecompress(StreamSource* source, std::vector<char>* output)
{
    ui32 size = 0;
    {
        // Piece of code from snappy implementation.
        // Snappy libraries has no tools to determine uncompressed size from const Source
        size_t len;
        const char* start = source->Peek(&len);
        const char* limit = start + len;
        YCHECK(snappy::Varint::Parse32WithLimit(start, limit, &size));
    }
    output->resize(size);
    YCHECK(snappy::RawUncompress(source, output->data()));
}

////////////////////////////////////////////////////////////////////////////////

}} // namespace NYT::NCodec

