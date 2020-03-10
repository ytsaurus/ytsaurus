#pragma once

#include "public.h"

#include <yt/ytlib/transaction_client/public.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager
    : public TRefCounted
{
public:
    TTransactionManager(
        NServer::NMaster::TBootstrap* bootstrap,
        TTransactionManagerConfigPtr config);
    ~TTransactionManager();

    void Initialize();

    TFuture<TTimestamp> GenerateTimestamp();

    TFuture<TTransactionPtr> StartReadWriteTransaction();
    TFuture<TTransactionPtr> StartReadOnlyTransaction(TTimestamp startTimestamp = NullTimestamp);

    TTransactionPtr FindTransaction(const TTransactionId& id);
    TTransactionPtr GetTransactionOrThrow(const TTransactionId& id);

private:
    class TImpl;
    const NYT::TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TTransactionManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
