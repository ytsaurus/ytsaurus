#pragma once

#include "public.h"

#include "codec.h"

namespace NYT::NFlow::NDeltaCodecs {

////////////////////////////////////////////////////////////////////////////////

//! Byte-compatible with NBSYeti::TVCDiffCodec: the patch is a raw open-vcdiff
//! stream in the interleaved format, without any additional framing.
class TVCDiffCodec
    : public ICodec
{
public:
    TSharedRef ApplyPatch(const TSharedRef& base, const TSharedRef& patch) const override;
    std::optional<TSharedRef> TryComputePatch(const TSharedRef& base, const TSharedRef& value) const override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDeltaCodecs
