#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/object_server/object.h>

namespace NYT::NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

//! Handles transactions for intermediate objects.
class TTransactionRotator
{
public:
    explicit TTransactionRotator(
        NCellMaster::TBootstrap* bootstrap,
        TString transactionTitle);

    //! Commits previous transaction if needed. Starts new transaction.
    void Rotate();

    //! Returns |true| if current or previous transaction is finished.
    //! In this case caller may want to update transactions or log changes.
    bool OnTransactionFinished(TTransaction* transaction);

    //! Clear all persistent fields. See AutomatonPart::Clear().
    void Clear();

    void Persist(const NCellMaster::TPersistenceContext& context);

    //! Returns previous transaction id or |NullTransactionId| if none.
    TTransactionId GetPreviousTransactionId() const;
    //! Returns current transaction id or |NullTransactionId| if none.
    TTransactionId GetTransactionId() const;
    //! Returns current transaction.
    TTransaction* GetTransaction() const;

private:
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    NCellMaster::TBootstrap* const Bootstrap_;

    const TString TransactionTitle_;

    using TTransactionWeakPtr =
        NObjectServer::TWeakObjectPtr<NTransactionServer::TTransaction>;

    TTransactionWeakPtr Transaction_;
    TTransactionWeakPtr PreviousTransaction_;

    static TTransactionId TransactionIdFromPtr(const TTransactionWeakPtr& ptr);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
