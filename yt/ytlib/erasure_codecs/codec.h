#pragma once

#include <ytlib/misc/ref.h>

namespace NYT {

namespace NErasure {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(ECodec,
    ((ReedSolomon3)(0))
);

////////////////////////////////////////////////////////////////////////////////

struct ICodec
{
    virtual std::vector<TSharedRef> Encode(const std::vector<TSharedRef>& blocks) = 0;

    virtual std::vector<TSharedRef> Decode(const std::vector<TSharedRef>& blocks, const std::vector<int>& erasedIndices) = 0;
};


//! Returns a codec for the registered id.
ICodec* GetCodec(ECodec id);

////////////////////////////////////////////////////////////////////////////////

} // namespace NErasure

} // namespace NYT


