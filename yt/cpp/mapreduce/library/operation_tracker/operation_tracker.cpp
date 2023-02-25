#include "operation_tracker.h"

#include <yt/cpp/mapreduce/interface/operation.h>
#include <library/cpp/threading/future/future.h>

#include <util/generic/hash_set.h>

namespace NYT {

using namespace NThreading;

class TOperationTracker::TImpl
    : public TThrRefBase
{
public:
    void AddOperation(IOperationPtr operation)
    {
        auto operationCompleteFuture = operation->Watch();

        {
            auto g = Guard(Lock_);
            CompletedOperationPromiseList_.emplace_back(NewPromise<IOperationPtr>());
        }

        ::TIntrusivePtr<TImpl> pThis = this;
        operationCompleteFuture.Subscribe(
            [pThis=pThis, operation=operation] (const TFuture<void>&) {
                auto g = Guard(pThis->Lock_);
                pThis->CompletedOperationPromiseList_[pThis->NextCompleted_].SetValue(operation);
                pThis->NextCompleted_++;
            });
    }

    NThreading::TFuture<IOperationPtr> NextCompletedOperation()
    {
        auto g = Guard(Lock_);
        if (NextReturned_ == CompletedOperationPromiseList_.size()) {
            return MakeFuture<IOperationPtr>(nullptr);
        } else {
            return CompletedOperationPromiseList_[NextReturned_++];
        }
    }

private:
    TMutex Lock_;

    TVector<TPromise<IOperationPtr>> CompletedOperationPromiseList_;
    size_t NextReturned_ = 0;
    size_t NextCompleted_ = 0;
};


TOperationTracker::TOperationTracker()
    : Impl_(MakeIntrusive<TImpl>())
{ }

TOperationTracker::~TOperationTracker() = default;

void TOperationTracker::AddOperation(IOperationPtr operation)
{
    Impl_->AddOperation(operation);
}

TVector<IOperationPtr> TOperationTracker::WaitAllCompleted()
{
    TVector<IOperationPtr> result;
    while (auto op = WaitOneCompleted()) {
        result.push_back(op);
    }
    return result;
}

TVector<IOperationPtr> TOperationTracker::WaitAllCompletedOrError()
{
    TVector<IOperationPtr> result;
    while (auto op = WaitOneCompletedOrError()) {
        result.push_back(op);
    }
    return result;
}

IOperationPtr TOperationTracker::WaitOneCompleted()
{
    auto nextCompletedOperation = Impl_->NextCompletedOperation();
    nextCompletedOperation.Wait();
    auto op = nextCompletedOperation.GetValue();
    if (op) {
        op->Watch().GetValue(); // Check if we have exception
    }
    return op;
}

IOperationPtr TOperationTracker::WaitOneCompletedOrError()
{
    auto nextCompletedOperation = Impl_->NextCompletedOperation();
    nextCompletedOperation.Wait();
    auto op = nextCompletedOperation.GetValue();
    return op;
}

} // namespace NYT
