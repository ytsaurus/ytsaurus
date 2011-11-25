#pragma once

#include "ref.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

typedef int TCodecId;

////////////////////////////////////////////////////////////////////////////////

struct ICodec
{
    virtual TSharedRef Encode(const TSharedRef& block) const = 0;
    virtual TSharedRef Decode(const TSharedRef& block) const = 0;

    //! Globally identifies codec type within YT.
    virtual TCodecId GetId() const = 0;
    virtual ~ICodec() { }

    static const ICodec& GetCodec(TCodecId id);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

