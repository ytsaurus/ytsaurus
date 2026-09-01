#include "vcdiff.h"

#include <library/cpp/yt/error/error.h>

#include <contrib/tools/open-vcdiff/src/google/format_extension_flags.h>
#include <contrib/tools/open-vcdiff/src/google/vcdecoder.h>
#include <contrib/tools/open-vcdiff/src/google/vcencoder.h>

namespace NYT::NFlow::NDeltaCodecs {

////////////////////////////////////////////////////////////////////////////////

// open-vcdiff's decoder refuses to reconstruct a target larger than this
// (its own default, matching the BigRT wrapper). The encoder has no such
// bound of its own, so it is pinned here explicitly on BOTH sides: a patch
// this decoder cannot apply must never be produced, or the value would
// become permanently unreadable once stored.
constexpr size_t MaxTargetSize = 67108864; // 64 MB

////////////////////////////////////////////////////////////////////////////////

TSharedRef TVCDiffCodec::ApplyPatch(const TSharedRef& base, const TSharedRef& patch) const
{
    if (patch.ToStringBuf().empty()) {
        return base;
    }

    // The decoder enforces open-vcdiff's default 64 MB target size limit,
    // same as the BigRT wrapper; a hostile patch fails instead of allocating.
    open_vcdiff::VCDiffDecoder decoder;
    std::string output;
    if (!decoder.Decode(base.data(), base.size(), patch.ToStringBuf(), &output)) {
        THROW_ERROR_EXCEPTION("Failed to decode by VCDiff");
    }
    return TSharedRef::FromString(std::move(output));
}

std::optional<TSharedRef> TVCDiffCodec::TryComputePatch(const TSharedRef& base, const TSharedRef& value) const
{
    if (base.ToStringBuf() == value.ToStringBuf()) {
        return TSharedRef::MakeEmpty();
    }

    // Refuse rather than mint an inapplicable patch; the caller rewrites the
    // base instead (the documented "no patch available" path).
    if (value.Size() > MaxTargetSize) {
        return std::nullopt;
    }

    open_vcdiff::VCDiffEncoder encoder(base.data(), base.size());
    encoder.SetFormatFlags(open_vcdiff::VCD_FORMAT_INTERLEAVED);
    std::string patch;
    if (!encoder.Encode(value.data(), value.size(), &patch)) {
        THROW_ERROR_EXCEPTION("Failed to encode by VCDiff");
    }
    return TSharedRef::FromString(std::move(patch));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDeltaCodecs
