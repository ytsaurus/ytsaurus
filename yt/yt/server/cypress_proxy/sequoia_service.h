#pragma once

#include "public.h"

#include <yt/yt/ytlib/sequoia_client/ypath_detail.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

struct TCypressResolveResult
{ };

struct TSequoiaResolveResult
{
    NSequoiaClient::TAbsoluteYPath ResolvedPrefix;
    NCypressClient::TNodeId ResolvedPrefixNodeId;

    NSequoiaClient::TYPath UnresolvedSuffix;
};

using TResolveResult = std::variant<
    TCypressResolveResult,
    TSequoiaResolveResult
>;

struct ISequoiaServiceContext
    : public virtual NRpc::IServiceContext
{
    virtual void SetRequestHeader(std::unique_ptr<NRpc::NProto::TRequestHeader> header) = 0;

    virtual const NSequoiaClient::ISequoiaTransactionPtr& GetSequoiaTransaction() const = 0;

    virtual const TResolveResult& GetResolveResultOrThrow() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ISequoiaServiceContext);

////////////////////////////////////////////////////////////////////////////////

class TSequoiaServiceContextWrapper
    : public NRpc::TServiceContextWrapper
    , public ISequoiaServiceContext
{
public:
    explicit TSequoiaServiceContextWrapper(ISequoiaServiceContextPtr underlyingContext);

    void SetRequestHeader(std::unique_ptr<NRpc::NProto::TRequestHeader> header) override;

    const NSequoiaClient::ISequoiaTransactionPtr& GetSequoiaTransaction() const override;

    const TResolveResult& GetResolveResultOrThrow() const override;

    const ISequoiaServiceContextPtr& GetUnderlyingContext() const;

private:
    const ISequoiaServiceContextPtr UnderlyingContext_;
};

DEFINE_REFCOUNTED_TYPE(TSequoiaServiceContextWrapper)

////////////////////////////////////////////////////////////////////////////////

struct ISequoiaService
    : public virtual TRefCounted
{
    //! Executes a given request.
    virtual void Invoke(const ISequoiaServiceContextPtr& context) = 0;
};

DEFINE_REFCOUNTED_TYPE(ISequoiaService);

////////////////////////////////////////////////////////////////////////////////

ISequoiaServicePtr CreateSequoiaService(IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
