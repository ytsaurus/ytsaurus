#pragma once

#include "public.h"
#include "transaction.h"

#include <yt/yt/orm/client/objects/transaction_context.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

struct TStartReadOnlyTransactionOptions
{
    TTimestamp StartTimestamp = NullTimestamp;

    //! Transaction requests will be tagged either
    //! by this field (if provided) or by rpc user of the current fiber.
    TString UserTag;

    TReadingTransactionOptions ReadingTransactionOptions;
};

struct TStartReadWriteTransactionOptions
    : public TStartReadOnlyTransactionOptions
{
    TDuration LeaseDuration;

    TTransactionId UnderlyingTransactionId;
    TString UnderlyingTransactionAddress;
    TMutatingTransactionOptions MutatingTransactionOptions;
};

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager
    : public TRefCounted
{
public:
    TTransactionManager(
        NServer::NMaster::IBootstrap* bootstrap,
        TTransactionManagerConfigPtr config);
    ~TTransactionManager();

    void Initialize();

    TTransactionManagerConfigPtr GetConfig() const;

    TFuture<TTimestamp> GenerateTimestamps(int count = 1);
    TTimestamp GenerateTimestampBuffered();

    TFuture<TTransactionPtr> StartReadWriteTransaction(
        TStartReadWriteTransactionOptions options = TStartReadWriteTransactionOptions());

    TFuture<TTransactionPtr> StartReadOnlyTransaction(
        TStartReadOnlyTransactionOptions options = TStartReadOnlyTransactionOptions());

    TTransactionPtr FindTransactionIfConnectedOrThrow(TTransactionId id);
    TTransactionPtr GetTransactionIfConnectedOrThrow(TTransactionId id);

    void ValidateDataCompleteness(TTimestamp timestamp) const;

    void ProfilePerformanceStatistics(const TPerformanceStatistics& statistics);

protected:
    virtual TTransactionPtr NewTransaction(
        TTransactionConfigsSnapshot configsSnapshot,
        TTransactionId id,
        TTimestamp startTimestamp,
        TYTTransactionOrClientDescriptor ytTransactionOrClient,
        std::string identityUserTag,
        TTransactionOptions options);

private:
    class TImpl;
    const NYT::TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TTransactionManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
