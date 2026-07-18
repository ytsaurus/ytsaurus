#pragma once

#include "public.h"

#include "codec.h"

namespace NYT::NFlow::NDeltaCodecs {

////////////////////////////////////////////////////////////////////////////////

class TXDeltaCodec
    : public ICodec
{
public:
    using TRecordSize = ui32;

    TSharedRef ApplyPatch(const TSharedRef& base, const TSharedRef& patch) const override;
    std::optional<TSharedRef> TryComputePatch(const TSharedRef& /*base*/, const TSharedRef& /*value*/) const override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDeltaCodecs
