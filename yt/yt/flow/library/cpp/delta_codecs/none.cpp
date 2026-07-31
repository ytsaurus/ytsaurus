#include "none.h"

#include <library/cpp/yt/error/error.h>

namespace NYT::NFlow::NDeltaCodecs {

////////////////////////////////////////////////////////////////////////////////

TSharedRef TNoneCodec::ApplyPatch(const TSharedRef& base, const TSharedRef& patch) const
{
    if (!patch.ToStringBuf().empty()) {
        THROW_ERROR_EXCEPTION("None codec expects empty patch")
            << TErrorAttribute("patch_size", patch.ToStringBuf().size());
    }
    return base;
}

std::optional<TSharedRef> TNoneCodec::TryComputePatch(const TSharedRef& /*base*/, const TSharedRef& /*value*/) const
{
    return std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDeltaCodecs
