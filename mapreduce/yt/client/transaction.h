#pragma once

#include <mapreduce/yt/common/abortable_registry.h>
#include <mapreduce/yt/http/requests.h>

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
        bool pingAncestors = false,
        bool autoPingable = true,
        const TMaybe<TString>& title = Nothing(),
        const TMaybe<TNode>& attributes = Nothing());

    ~TPingableTransaction();

    const TTransactionId GetId() const;

    void Commit();
    void Abort();
    void Ping();

private:
    TAuth Auth_;
    TTransactionId TransactionId_;
    // We have to own an IntrusivePtr to registry to prevent use-after-free
    ::TIntrusivePtr<NDetail::TAbortableRegistry> AbortableRegistry_;

    std::atomic<bool> Running_{false};
    THolder<TThread> Thread_;

    void Stop(bool commit);

    void Pinger();
    static void* Pinger(void* opaque);
};

////////////////////////////////////////////////////////////////////////////////

TYPath Snapshot(const TAuth& auth, const TTransactionId& transactionId, const TYPath& path);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
