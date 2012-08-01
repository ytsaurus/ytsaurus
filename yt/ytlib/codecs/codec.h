#pragma once

#include "public.h"

#include <ytlib/misc/ref.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(ECodecId,
    ((None)(0))
    ((Snappy)(1))
    ((GzipNormal)(2))
    ((GzipBestCompression)(3))
    ((Lz4)(4))
    ((Lz4HighCompression)(5))
);


////////////////////////////////////////////////////////////////////////////////

//! A generic interface for compression/decompression.
struct ICodec
    : public TRefCounted
{
    //! Compress a given block.
    virtual TSharedRef Compress(const TSharedRef& block) = 0;

    //! Compress a vector of blocks.
    virtual TSharedRef Compress(const std::vector<TSharedRef>& blocks) = 0;

    //! Decompress a given block.
    virtual TSharedRef Decompress(const TSharedRef& block) = 0;
};


//! Returns a codec for the registered id.
/*!
 *  Codec instances are singletons.
 */
TCodecPtr GetCodec(ECodecId id);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

