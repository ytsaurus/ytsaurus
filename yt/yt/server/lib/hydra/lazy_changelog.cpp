#include "lazy_changelog.h"
#include "changelog.h"

#include <yt/yt/core/concurrency/scheduler.h>

namespace NYT::NHydra {

using namespace NConcurrency;
using namespace NHydra::NProto;

////////////////////////////////////////////////////////////////////////////////

class TLazyChangelog
    : public IChangelog
{
public:
    TLazyChangelog(
        int changelogId,
        TFuture<IChangelogPtr> futureChangelog)
        : ChangelogId_(changelogId)
        , FutureChangelog_(futureChangelog)
        , BacklogAppendPromise_(NewPromise<void>())
    {
        FutureChangelog_.Subscribe(
            BIND(&TLazyChangelog::OnUnderlyingChangelogReady, MakeWeak(this)));
    }

    int GetId() const override
    {
        return ChangelogId_;
    }

    int GetRecordCount() const override
    {
        auto guard = Guard(SpinLock_);
        return UnderlyingChangelog_
            ? UnderlyingChangelog_->GetRecordCount()
            : BacklogRecords_.size();
    }

    i64 GetDataSize() const override
    {
        auto guard = Guard(SpinLock_);
        return UnderlyingChangelog_
            ? UnderlyingChangelog_->GetDataSize()
            : BacklogDataSize_;
    }

    i64 EstimateChangelogSize(i64 payloadSize) const override
    {
        auto guard = Guard(SpinLock_);
        return UnderlyingChangelog_
            ? UnderlyingChangelog_->EstimateChangelogSize(payloadSize)
            : payloadSize;
    }

    const TChangelogMeta& GetMeta() const override
    {
        return GetUnderlyingChangelog()->GetMeta();
    }

    TFuture<void> Append(TRange<TSharedRef> records) override
    {
        auto guard = Guard(SpinLock_);

        if (!UnderlyingError_.IsOK()) {
            return MakeFuture(UnderlyingError_);
        }

        if (UnderlyingChangelog_) {
            guard.Release();
            return UnderlyingChangelog_->Append(records);
        } else {
            for (const auto& record : records) {
                BacklogRecords_.push_back(record);
            }
            return BacklogAppendPromise_;
        }
    }

    TFuture<void> Flush() override
    {
        return FutureChangelog_.Apply(BIND([=] (IChangelogPtr changelog) -> TFuture<void> {
            return changelog->Flush();
        }));
    }

    TFuture<std::vector<TSharedRef>> Read(
        int firstRecordId,
        int maxRecords,
        i64 maxBytes) const override
    {
        auto guard = Guard(SpinLock_);

        if (!UnderlyingError_.IsOK()) {
            return MakeFuture<std::vector<TSharedRef>>(UnderlyingError_);
        }

        if (UnderlyingChangelog_) {
            guard.Release();
            return UnderlyingChangelog_->Read(
                firstRecordId,
                maxRecords,
                maxBytes);
        } else {
            int recordCount = std::min(static_cast<int>(BacklogRecords_.size()) - firstRecordId, maxRecords);
            std::vector<TSharedRef> result(recordCount);
            for (int index = 0; index < recordCount; ++index) {
                result[index] = BacklogRecords_[index + firstRecordId];
            }
            return MakeFuture(result);
        }
    }

    TFuture<void> Truncate(int recordCount) override
    {
        return FutureChangelog_.Apply(BIND([=] (const TErrorOr<IChangelogPtr>& changelogOrError) -> TFuture<void> {
            if (!changelogOrError.IsOK()) {
                return MakeFuture<void>(TError(changelogOrError));
            }
            return changelogOrError.Value()->Truncate(recordCount);
        }));
    }

    TFuture<void> Close() override
    {
        return FutureChangelog_.Apply(BIND([=] (IChangelogPtr changelog) -> TFuture<void> {
            return changelog->Close();
        }));
    }

private:
    const int ChangelogId_;
    const TFuture<IChangelogPtr> FutureChangelog_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);

    //! If non-OK then the underlying changelog could not open.
    TError UnderlyingError_;

    //! If non-null then contains the underlying changelog.
    IChangelogPtr UnderlyingChangelog_;

    //! Collects records while changelog is still opening.
    std::vector<TSharedRef> BacklogRecords_;

    //! Sum of sizes of #BacklogRecords_.
    i64 BacklogDataSize_ = 0;

    //! Fulfilled when the records collected while the underlying changelog
    //! was opening are flushed.
    TPromise<void> BacklogAppendPromise_;


    void OnUnderlyingChangelogReady(TErrorOr<IChangelogPtr> changelogOrError)
    {
        auto guard = Guard(SpinLock_);

        if (!changelogOrError.IsOK()) {
            UnderlyingError_ = changelogOrError;
            auto promise = BacklogAppendPromise_;
            guard.Release();
            promise.Set(UnderlyingError_);
            return;
        }

        YT_VERIFY(!UnderlyingChangelog_);
        UnderlyingChangelog_ = changelogOrError.Value();

        YT_VERIFY(UnderlyingChangelog_->GetId() == ChangelogId_);

        auto future = UnderlyingChangelog_->Append(BacklogRecords_);
        BacklogRecords_.clear();

        auto promise = BacklogAppendPromise_;

        guard.Release();

        promise.SetFrom(std::move(future));
    }

    IChangelogPtr GetUnderlyingChangelog() const
    {
        return WaitFor(FutureChangelog_)
            .ValueOrThrow();
    }
};

////////////////////////////////////////////////////////////////////////////////

IChangelogPtr CreateLazyChangelog(
    int changelogId,
    TFuture<IChangelogPtr> futureChangelog)
{
    return New<TLazyChangelog>(
        changelogId,
        std::move(futureChangelog));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
