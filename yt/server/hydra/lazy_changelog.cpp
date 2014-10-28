#include "stdafx.h"
#include "lazy_changelog.h"
#include "changelog.h"

#include <core/concurrency/scheduler.h>

namespace NYT {
namespace NHydra {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TLazyChangelog
    : public IChangelog
{
public:
    explicit TLazyChangelog(TFuture<TErrorOr<IChangelogPtr>> futureChangelogOrError)
        : FutureChangelogOrError_(futureChangelogOrError)
        , BacklogAppendPromise_(NewPromise<TError>())
    {
        FutureChangelogOrError_.Subscribe(
            BIND(&TLazyChangelog::OnUnderlyingChangelogReady, MakeWeak(this)));
    }

    virtual TSharedRef GetMeta() const override
    {
        return GetUnderlyingChangelog()->GetMeta();
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

    virtual bool IsSealed() const override
    {
        TGuard<TSpinLock> guard(SpinLock_);
        return UnderlyingChangelog_
            ? UnderlyingChangelog_->IsSealed()
            : false;
    }

    virtual TAsyncError Append(const TSharedRef& data) override
    {
        TGuard<TSpinLock> guard(SpinLock_);
        if (!UnderlyingError_.IsOK()) {
            return MakeFuture(UnderlyingError_);
        }
        if (UnderlyingChangelog_) {
            guard.Release();
            return UnderlyingChangelog_->Append(data);
        } else {
            BacklogRecords_.push_back(data);
            return BacklogAppendPromise_;
        }
    }

    virtual TAsyncError Flush() override
    {
        return FutureChangelogOrError_.Apply(BIND([=] (const TErrorOr<IChangelogPtr>& changelogOrError ) -> TAsyncError {
            if (!changelogOrError.IsOK()) {
                return MakeFuture<TError>(TError(changelogOrError));
            }
            return changelogOrError.Value()->Flush();
        }));
    }

    virtual std::vector<TSharedRef> Read(
        int firstRecordId,
        int maxRecords,
        i64 maxBytes) const override
    {
        TGuard<TSpinLock> guard(SpinLock_);

        if (!UnderlyingError_.IsOK()) {
            THROW_ERROR_EXCEPTION(UnderlyingError_);
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
            return result;
        }
    }

    virtual TAsyncError Seal(int recordCount) override
    {
        return FutureChangelogOrError_.Apply(BIND([=] (const TErrorOr<IChangelogPtr>& changelogOrError) -> TAsyncError {
            if (!changelogOrError.IsOK()) {
                return MakeFuture<TError>(TError(changelogOrError));
            }
            return changelogOrError.Value()->Seal(recordCount);
        }));
    }

    virtual TAsyncError Unseal() override
    {
        return FutureChangelogOrError_.Apply(BIND([=] (const TErrorOr<IChangelogPtr> changelogOrError) -> TAsyncError {
            if (!changelogOrError.IsOK()) {
                return MakeFuture<TError>(TError(changelogOrError));
            }
            return changelogOrError.Value()->Unseal();
        }));
    }

private:
    TFuture<TErrorOr<IChangelogPtr>> FutureChangelogOrError_;

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
    TAsyncErrorPromise BacklogAppendPromise_;


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

        YCHECK(!UnderlyingChangelog_);
        UnderlyingChangelog_ = changelogOrError.Value();
        
        TAsyncError lastBacklogAppendResult;
        for (const auto& record : BacklogRecords_) {
            lastBacklogAppendResult = UnderlyingChangelog_->Append(record);
        }
        BacklogRecords_.clear();

        auto promise = BacklogAppendPromise_;

        guard.Release();

        if (lastBacklogAppendResult) {
            promise.SetFrom(std::move(lastBacklogAppendResult));
        } else {
            promise.Set(TError());
        }
    }

    IChangelogPtr GetUnderlyingChangelog() const
    {
        auto changelogOrError = WaitFor(FutureChangelogOrError_);
        THROW_ERROR_EXCEPTION_IF_FAILED(changelogOrError);
        return changelogOrError.Value();
    }

};

IChangelogPtr CreateLazyChangelog(TFuture<TErrorOr<IChangelogPtr>> futureChangelogOrError)
{
    return New<TLazyChangelog>(futureChangelogOrError);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
