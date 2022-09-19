#include "block_tracker.h"

namespace NYT {

/////////////////////////////////////////////////////////////////////////////

namespace NDetail {

class TCategoryUsageGuard
    : public ISharedRangeHolder
    , public TNonCopyable
{
public:
    TCategoryUsageGuard(
        IBlockTrackerPtr tracker,
        TSharedRef holder,
        EMemoryCategory category)
        : Tracker_(std::move(tracker))
        , Holder_(std::move(holder))
        , Category_(category)
    {
        YT_VERIFY(Tracker_);
        YT_VERIFY(Holder_);

        Tracker_->AcquireCategory(Holder_, Category_);
    }

    ~TCategoryUsageGuard() override
    {
        Tracker_->ReleaseCategory(Holder_, Category_);
    }

private:
    const IBlockTrackerPtr Tracker_;
    const TSharedRef Holder_;
    const EMemoryCategory Category_;
};

} // namespace NDetail

/////////////////////////////////////////////////////////////////////////////

TSharedRef ResetCategory(
    TSharedRef block,
    IBlockTrackerPtr tracker,
    std::optional<EMemoryCategory> category)
{
    if (!tracker || !block) {
        return block;
    }

    block = tracker->RegisterBlock(std::move(block));

    if (category) {
        TRef ref = block;
        block = TSharedRef(
            ref,
            New<NDetail::TCategoryUsageGuard>(
                std::move(tracker),
                std::move(block),
                *category));
    }

    return block;
}

/////////////////////////////////////////////////////////////////////////////

TSharedRef AttachCategory(
    TSharedRef block,
    IBlockTrackerPtr tracker,
    std::optional<EMemoryCategory> category)
{
    auto blockWithCategory = ResetCategory(block, std::move(tracker), category);

    TRef ref = block;
    return TSharedRef(
        ref,
        MakeSharedRangeHolder(
            std::move(block),
            std::move(blockWithCategory)));
}

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT
