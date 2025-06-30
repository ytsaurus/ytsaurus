#pragma once

#include "public.h"

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

struct ISequoiaResponseKeeper
    : public TRefCounted
{
    virtual std::optional<TSharedRefArray> FindResponse(
        const NRpc::IServiceContextPtr& context,
        const NSequoiaClient::ISequoiaTransactionPtr& transaction) const = 0;

    virtual void KeepResponse(
        const NSequoiaClient::ISequoiaTransactionPtr& transaction,
        NRpc::TMutationId mutationId,
        TSharedRefArray responseMessage) const = 0;

    virtual const TSequoiaResponseKeeperDynamicConfigPtr& GetDynamicConfig() const = 0;

    virtual void Reconfigure(const TSequoiaResponseKeeperDynamicConfigPtr& newConfig) = 0;
};

DEFINE_REFCOUNTED_TYPE(ISequoiaResponseKeeper)

////////////////////////////////////////////////////////////////////////////////

ISequoiaResponseKeeperPtr CreateSequoiaResponseKeeper(
    TSequoiaResponseKeeperDynamicConfigPtr config,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
