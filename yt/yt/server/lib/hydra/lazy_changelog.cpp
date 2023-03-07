#include "lazy_changelog.h"
#include "changelog.h"

#include <yt/core/concurrency/scheduler.h>

namespace NYT::NHydra {

using namespace NConcurrency;
using namespace NHydra::NProto;

////////////////////////////////////////////////////////////////////////////////

class TLazyChangelog
    : public IChangelog
{
public:
    explicit TLazyChangelog(TFuture<IChangelogPtr> futureChangelog)
        : FutureChangelog_(futureChangelog)
        , BacklogAppendPromise_(NewPromise<void>())
    {
        FutureChangelog_.Subscribe(
            BIND(&TLazyChangelog::OnUnderlyingChangelogReady, MakeWeak(this)));
    }

    virtual int GetRecordCount() const override
    {
        TGuard<TSpinLock> guard(SpinLock_);
        return UnderlyingChangelog_
            ? UnderlyingChangelog_->GetRecordCount()
            : BacklogRecords_.size();
    }

    virtual i64 GetDataSize() const override
    {
        TGuard<TSpinLock> guard(SpinLock_);
        return UnderlyingChangelog_
            ? UnderlyingChangelog_->GetDataSize()
            : BacklogDataSize_;
    }

    virtual TFuture<void> Append(TRange<TSharedRef> records) override
    {
        TGuard<TSpinLock> guard(SpinLock_);

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

    virtual TFuture<void> Flush() override
    {
        return FutureChangelog_.Apply(BIND([=] (IChangelogPtr changelog) -> TFuture<void> {
            return changelog->Flush();
        }));
    }

    virtual TFuture<std::vector<TSharedRef>> Read(
        int firstRecordId,
        int maxRecords,
        i64 maxBytes) const override
    {
        TGuard<TSpinLock> guard(SpinLock_);

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

    virtual TFuture<void> Truncate(int recordCount) override
    {
        return FutureChangelog_.Apply(BIND([=] (const TErrorOr<IChangelogPtr>& changelogOrError) -> TFuture<void> {
            if (!changelogOrError.IsOK()) {
                return MakeFuture<void>(TError(changelogOrError));
            }
            return changelogOrError.Value()->Truncate(recordCount);
        }));
    }

    virtual TFuture<void> Close() override
    {
        return FutureChangelog_.Apply(BIND([=] (IChangelogPtr changelog) -> TFuture<void> {
            return changelog->Close();
        }));
    }

    virtual TFuture<void> Preallocate(size_t size) override
    {
        return FutureChangelog_.Apply(BIND([=] (IChangelogPtr changelog) -> TFuture<void> {
            return changelog->Preallocate(size);
        }));
    }

private:
    TFuture<IChangelogPtr> FutureChangelog_;

    TSpinLock SpinLock_;

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
        TGuard<TSpinLock> guard(SpinLock_);

        if (!changelogOrError.IsOK()) {
            UnderlyingError_ = changelogOrError;
            auto promise = BacklogAppendPromise_;
            guard.Release();
            promise.Set(UnderlyingError_);
            return;
        }

        YT_VERIFY(!UnderlyingChangelog_);
        UnderlyingChangelog_ = changelogOrError.Value();

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

IChangelogPtr CreateLazyChangelog(TFuture<IChangelogPtr> futureChangelog)
{
    return New<TLazyChangelog>(futureChangelog);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
