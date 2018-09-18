#include "block.h"

#include <yt/core/misc/checksum.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

TBlock::TBlock(
    TSharedRef block)
    : Data(std::move(block))
{ }

TBlock::TBlock(
    TSharedRef block,
    TChecksum checksum)
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

bool TBlock::IsChecksumValid() const
{
    if (!Data || (Checksum == NullChecksum)) {
        return true;
    }
    return GetChecksum(Data) == Checksum;
}

void TBlock::ValidateChecksum() const
{
    if (Checksum == NullChecksum) {
        return;
    }

    auto actual = GetChecksum(Data);
    YCHECK(actual == Checksum);
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

} // namespace NChunkClient
} // namespace NYT
