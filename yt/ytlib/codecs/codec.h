#pragma once

#include <ytlib/misc/ref.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(ECodecId,
    ((None)(0))
    ((Snappy)(1))
    ((Gzip)(2))
    ((Lz4)(3))
);


////////////////////////////////////////////////////////////////////////////////

//! A generic interface for compression/decompression.
struct ICodec
{
    //! Compress a given block.
    virtual TSharedRef Compress(const TSharedRef& block) = 0;

    //! Compress a vector of blocks.
    virtual TSharedRef Compress(const std::vector<TSharedRef>& blocks) = 0;

    //! Decompress a given block.
    virtual TSharedRef Decompress(const TSharedRef& block) = 0;

    virtual ~ICodec() { }

};

//! Returns a codec for the registered id.
/*!
 *  Codec instances are singletons.
 */
ICodec* GetCodec(ECodecId id);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

