#pragma once

#include <mapreduce/yt/common/abortable_registry.h>
#include <mapreduce/yt/http/requests.h>
#include <mapreduce/yt/http/retry_request.h>

#include <util/datetime/base.h>
#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/system/thread.h>

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TPingableTransaction
{
public:
    TPingableTransaction(
        const TAuth& auth,
        const TTransactionId& parentId,
        const TMaybe<TDuration>& timeout = Nothing(),
        const TMaybe<TInstant>& deadline = Nothing(),
        bool pingAncestors = false,
        bool autoPingable = true,
        const TMaybe<TString>& title = Nothing(),
        const TMaybe<TNode>& attributes = Nothing());

    ~TPingableTransaction();

    const TTransactionId GetId() const;

    void Commit();
    void Abort();

private:
    TAuth Auth_;
    TTransactionId TransactionId_;
    TDuration MinPingInterval_;
    TDuration MaxPingInterval_;

    // We have to own an IntrusivePtr to registry to prevent use-after-free
    ::TIntrusivePtr<NDetail::TAbortableRegistry> AbortableRegistry_;

    std::atomic<bool> Running_{false};
    THolder<TThread> Thread_;

    void Stop(bool commit);

    void Pinger();
    static void* Pinger(void* opaque);
};

////////////////////////////////////////////////////////////////////////////////

class TPingRetryPolicy
    :   public IRequestRetryPolicy
{
public:
    TPingRetryPolicy(ui32 attemptCount = 0);

    void NotifyNewAttempt() override;
    TMaybe<TDuration> OnGenericError(const yexception& e) override;
    TMaybe<TDuration> OnRetriableError(const TErrorResponse& e) override;
    void OnIgnoredError(const TErrorResponse& e) override;
    TString GetAttemptDescription() const override;

private:
    ui32 AttemptCount_;
    ui32 Attempt_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

TYPath Snapshot(
    const IClientRetryPolicyPtr& clientRetryPolicy,
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
