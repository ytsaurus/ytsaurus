#include "snappy.h"

#include <contrib/libs/snappy/snappy.h>
#include <contrib/libs/snappy/snappy-stubs-internal.h>

namespace NYT {
namespace NCompression {

////////////////////////////////////////////////////////////////////////////////

void SnappyCompress(StreamSource* source, TBlob* output)
{
    output->Resize(snappy::MaxCompressedLength(source->Available()), false);
    snappy::UncheckedByteArraySink writer(output->Begin());
    size_t compressedSize = snappy::Compress(source, &writer);
    output->Resize(compressedSize);
}

void SnappyDecompress(StreamSource* source, TBlob* output)
{
    ui32 size = 0;
    {
        // Extracted from snappy implementation as it
        // provides no means to determine uncompressed size from stream source.
        size_t len;
        const char* start = source->Peek(&len);
        const char* limit = start + len;
        YCHECK(snappy::Varint::Parse32WithLimit(start, limit, &size));
    }
    output->Resize(size, false);
    YCHECK(snappy::RawUncompress(source, output->Begin()));
}

////////////////////////////////////////////////////////////////////////////////

}} // namespace NYT::NCompression

