#pragma once

#include "enum.h"
#include "ref.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(ECodecId,
    ((None)(0))
    ((Snappy)(1))
);

////////////////////////////////////////////////////////////////////////////////

struct ICodec
{
    virtual TSharedRef Encode(const TSharedRef& block) const = 0;
    virtual TSharedRef Decode(const TSharedRef& block) const = 0;

    virtual ~ICodec() { }

    static const ICodec& GetCodec(ECodecId id);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

