#pragma once

#include "discovery_base.h"

#include <yt/yt/ytlib/discovery_client/public.h>

#include <yt/yt/client/api/transaction.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TDiscovery
    : public TDiscoveryBase
{
public:
    TDiscovery(
        TDiscoveryV1ConfigPtr config,
        NApi::IClientPtr client,
        IInvokerPtr invoker,
        std::vector<TString> extraAttributes,
        NLogging::TLogger logger = {});

    TFuture<void> Enter(TString name, NYTree::IAttributeDictionaryPtr attributes) override;
    TFuture<void> Leave() override;

    int Version() const override;

private:
    TDiscoveryV1ConfigPtr Config_;
    NApi::IClientPtr Client_;
    NApi::TListNodeOptions ListOptions_;
    NApi::ITransactionPtr Transaction_;
    NApi::ITransaction::TAbortedHandler TransactionAbortedHandler_;
    int Epoch_;

    void DoEnter(TString name, NYTree::IAttributeDictionaryPtr attributes);
    void DoLeave();

    void DoUpdateList() override;

    void DoCreateNode(int epoch);
    void DoLockNode(int epoch);

    void DoRestoreTransaction(int epoch, const TError& error);
};

DEFINE_REFCOUNTED_TYPE(TDiscovery)

////////////////////////////////////////////////////////////////////////////////

IDiscoveryPtr CreateDiscoveryV1(
    TDiscoveryV1ConfigPtr config,
    NApi::IClientPtr client,
    IInvokerPtr invoker,
    std::vector<TString> extraAttributes,
    NLogging::TLogger logger = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
