#include "block.h"

#include <yt/yt/core/misc/checksum.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

TBlock::TBlock(
    TSharedRef block)
    : Data(std::move(block))
{ }

TBlock::TBlock(TSharedRef block, TChecksum checksum)
    : Data(std::move(block))
    , Checksum(checksum)
{ }

std::vector<TBlock> TBlock::Wrap(const std::vector<TSharedRef>& blocks)
{
    std::vector<TBlock> wrapped;
    for (auto& block : blocks) {
        wrapped.emplace_back(block);
    }
    return wrapped;
}

std::vector<TBlock> TBlock::Wrap(std::vector<TSharedRef>&& blocks)
{
    std::vector<TBlock> wrapped;
    for (auto& block : blocks) {
        wrapped.emplace_back(std::move(block));
    }
    return wrapped;
}


std::vector<TSharedRef> TBlock::Unwrap(const std::vector<TBlock>& blocks)
{
    std::vector<TSharedRef> raw;
    for (auto& block : blocks) {
        raw.emplace_back(block.Data);
    }
    return raw;
}

TError TBlock::ValidateChecksum() const
{
    if (!Data || Checksum == NullChecksum) {
        return TError();
    }

    auto actualChecksum = GetChecksum(Data);
    if (actualChecksum == Checksum) {
        return TError();
    } else {
        return TError(
            EErrorCode::InvalidBlockChecksum,
            "Invalid checksum detected in block")
            << TErrorAttribute("expected_checksum", Checksum)
            << TErrorAttribute("actual_checksum", actualChecksum)
            << TErrorAttribute("recalculated_checksum", GetChecksum(Data));
    }
}

TChecksum TBlock::GetOrComputeChecksum() const
{
    if (Checksum == NullChecksum) {
        return GetChecksum(Data);
    } else {
        return Checksum;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
