#include "block_reorderer.h"

#include "block.h"

#include <random>

namespace NYT::NChunkClient
{

////////////////////////////////////////////////////////////////////////////////

TBlockReorderer::TBlockReorderer(TBlockReordererConfigPtr config)
    : Config_(std::move(config))
{ }

void TBlockReorderer::ReorderBlocks(std::vector<TBlock>& blocks)
{
    auto oldSize = BlockIndexMapping_.size();
    auto newSize = BlockIndexMapping_.size() + blocks.size();
    BlockIndexMapping_.resize(newSize, -1);

    if (!Config_->EnableBlockReordering) {
        for (size_t index = oldSize; index != newSize; ++index) {
            BlockIndexMapping_[index] = index;
        }
        return;
    }

    struct TEntry
    {
        size_t Index = 0;
        std::optional<int> GroupIndex;
    };

    std::vector<TEntry> entries(blocks.size());

    for (size_t index = 0; index < blocks.size(); ++index) {
        entries[index] = {index, blocks[index].GroupIndex};
    }

    if (Config_->ShuffleBlocks) {
        //! This mode is only for testing.
        std::random_device randomDevice;
        std::mt19937 generator(randomDevice());

        std::shuffle(entries.begin(), entries.end(), generator);
    } else {
        //! Stable sort allows keeping original block order inside group (in particular,
        //! when writer is actually horizontal).
        std::stable_sort(entries.begin(), entries.end(), [] (const TEntry& lhs, const TEntry& rhs) {
            return lhs.GroupIndex < rhs.GroupIndex;
        });
    }

    std::vector<TBlock> newBlocks(blocks.size());

    for (size_t index = 0; index < blocks.size(); ++index) {
        size_t fromIndex = entries[index].Index;
        size_t toIndex = index;
        newBlocks[toIndex] = std::move(blocks[fromIndex]);
        YT_VERIFY(BlockIndexMapping_[oldSize + fromIndex] == -1);
        BlockIndexMapping_[oldSize + fromIndex] = oldSize + toIndex;
    }

    blocks.swap(newBlocks);
}

bool TBlockReorderer::IsEnabled() const
{
    return Config_->EnableBlockReordering;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
