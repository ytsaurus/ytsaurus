#include "compression.h"

namespace NYT::NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IOutputStream> TryDetectOptionalCompressors(
    TContentEncoding contentEncoding,
    IOutputStream* inner)
{
    Y_UNUSED(contentEncoding, inner);
    return nullptr;
}

std::unique_ptr<IInputStream> TryDetectOptionalDecompressors(
    TContentEncoding contentEncoding,
    IInputStream* inner)
{
    Y_UNUSED(contentEncoding, inner);
    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
