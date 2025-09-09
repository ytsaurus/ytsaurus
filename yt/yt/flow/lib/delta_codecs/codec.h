#pragma once

#include "public.h"

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NFlow::NDeltaCodecs {

////////////////////////////////////////////////////////////////////////////////

struct ICodec
{
    virtual ~ICodec() = default;

    virtual TSharedRef ApplyPatch(const TSharedRef& base, const TSharedRef& patch) const = 0;

    virtual std::optional<TSharedRef> TryComputePatch(const TSharedRef& base, const TSharedRef& value) const = 0;
};

ICodec* GetCodec(ECodec id);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDeltaCodecs
