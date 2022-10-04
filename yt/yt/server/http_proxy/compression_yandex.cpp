#include "compression.h"

#include <library/cpp/streams/lzop/lzop.h>
#include <library/cpp/streams/lz/lz.h>

namespace NYT::NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

std::vector<TContentEncoding> SupportedCompressions = {
    "gzip",
    IdentityContentEncoding,
    "br",
    "x-lzop",
    "y-lzo",
    "y-lzf",
    "y-snappy",
    "deflate",
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IOutputStream> TryDetectOptionalCompressors(
    TContentEncoding contentEncoding,
    IOutputStream* inner)
{
    if (contentEncoding == "x-lzop") {
        return std::make_unique<TLzopCompress>(inner, DefaultStreamBufferSize);
    }

    if (contentEncoding == "y-lzo") {
        return std::make_unique<TLzoCompress>(inner, DefaultStreamBufferSize);
    }

    if (contentEncoding == "y-lzf") {
        return std::make_unique<TLzfCompress>(inner, DefaultStreamBufferSize);
    }

    if (contentEncoding == "y-snappy") {
        return std::make_unique<TSnappyCompress>(inner, DefaultStreamBufferSize);
    }

    return nullptr;
}

std::unique_ptr<IInputStream> TryDetectOptionalDecompressors(
    TContentEncoding contentEncoding,
    IInputStream* inner)
{
    if (contentEncoding == "x-lzop") {
        return std::make_unique<TLzopDecompress>(inner);
    }

    if (contentEncoding == "y-lzo") {
        return std::make_unique<TLzoDecompress>(inner);
    }

    if (contentEncoding == "y-lzf") {
        return std::make_unique<TLzfDecompress>(inner);
    }

    if (contentEncoding == "y-snappy") {
        return std::make_unique<TSnappyDecompress>(inner);
    }

    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
