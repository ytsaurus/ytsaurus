#include "block_map.h"

#include <library/cpp/yt/assert/assert.h>

#include <library/cpp/yt/memory/new.h>

#include <atomic>
#include <vector>

namespace NYT::NNbd::NJournal {

////////////////////////////////////////////////////////////////////////////////

//! Maps each block index to its state, kept in a vector of atomic 64-bit slots:
//! the two high bits hold the state and the remaining 62 bits hold the id (if any).
/*!
 *  Slots are accessed with acquire/release ordering so that a reader that observes a
 *  dirty/clean id also observes everything the writer published before installing it.
 */
class TBlockMap
    : public IBlockMap
{
public:
    explicit TBlockMap(int blockCount)
        : Slots_(blockCount)
    {
        YT_VERIFY(blockCount >= 0);
    }

    std::variant<TEmptyBlock, TStoredBlockId, TDirtyBlockId> Find(int blockIndex) override
    {
        auto slot = GetSlot(blockIndex).load(std::memory_order::acquire);
        switch (slot >> IdBitCount) {
            case EmptyState:
                return TEmptyBlock{};
            case DirtyState:
                return TDirtyBlockId(slot & IdMask);
            case CleanState:
                return TStoredBlockId(slot & IdMask);
        }
        YT_ABORT();
    }

    void PutDirty(int blockIndex, TDirtyBlockId blockId) override
    {
        GetSlot(blockIndex).store(MakeSlot(DirtyState, blockId.Underlying()), std::memory_order::release);
    }

    bool TryMakeClean(int blockIndex, TDirtyBlockId expectedBlockId, TStoredBlockId storedBlockId) override
    {
        auto expectedSlot = MakeSlot(DirtyState, expectedBlockId.Underlying());
        auto desiredSlot = MakeSlot(CleanState, storedBlockId.Underlying());
        return GetSlot(blockIndex).compare_exchange_strong(expectedSlot, desiredSlot);
    }

private:
    std::vector<std::atomic<ui64>> Slots_;

    static constexpr int IdBitCount = 62;
    static constexpr ui64 IdMask = (1ULL << IdBitCount) - 1;

    static constexpr ui64 EmptyState = 0;
    static constexpr ui64 DirtyState = 1;
    static constexpr ui64 CleanState = 2;

    std::atomic<ui64>& GetSlot(int blockIndex)
    {
        YT_VERIFY(0 <= blockIndex && blockIndex < std::ssize(Slots_));
        return Slots_[blockIndex];
    }

    static ui64 MakeSlot(ui64 state, ui64 id)
    {
        // The id must leave the two high bits free for the state.
        YT_VERIFY((id & ~IdMask) == 0);
        return (state << IdBitCount) | id;
    }
};

////////////////////////////////////////////////////////////////////////////////

IBlockMapPtr CreateBlockMap(int blockCount)
{
    return New<TBlockMap>(blockCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NJournal
