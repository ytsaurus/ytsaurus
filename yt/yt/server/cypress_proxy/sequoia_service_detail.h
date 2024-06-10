#pragma once

#include "public.h"
#include "sequoia_service.h"

#include <yt/yt/core/ytree/ypath_detail.h>

#include <library/cpp/yt/logging/public.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

class TSequoiaServiceContext
    : public NRpc::TServiceContextBase
    , public ISequoiaServiceContext
{
public:
    template <class... TArgs>
    TSequoiaServiceContext(
        NSequoiaClient::ISequoiaTransactionPtr transaction,
        TArgs&&... args);

    void SetRequestHeader(std::unique_ptr<NRpc::NProto::TRequestHeader> header) override;

    const NSequoiaClient::ISequoiaTransactionPtr& GetSequoiaTransaction() const override;

    const TResolveResult& GetResolveResultOrThrow() const override;
    void SetResolveResult(TResolveResult resolveResult);

    TRange<TResolveStep> GetResolveHistory() const override;
    void SetResolveHistory(std::vector<TResolveStep> resolveHistory);

    std::optional<TResolveStep> TryGetLastResolveStep() const override;

private:
    const NSequoiaClient::ISequoiaTransactionPtr Transaction_;

    std::optional<TResolveResult> ResolveResult_;
    std::vector<TResolveStep> ResolveHistory_;

    // TODO(danilalexeev)
    std::optional<NProfiling::TWallTimer> Timer_;
    const NYTree::NProto::TYPathHeaderExt* CachedYPathExt_ = nullptr;

    const NYTree::NProto::TYPathHeaderExt& GetYPathExt();

    void DoReply() override;
    void LogRequest() override;
    void LogResponse() override;
};

////////////////////////////////////////////////////////////////////////////////

TIntrusivePtr<TSequoiaServiceContext> CreateSequoiaContext(
    TSharedRefArray requestMessage,
    NSequoiaClient::ISequoiaTransactionPtr transaction,
    NLogging::TLogger logger = NLogging::TLogger(),
    NLogging::ELogLevel logLevel = NLogging::ELogLevel::Debug);

////////////////////////////////////////////////////////////////////////////////

class TSequoiaServiceBase
    : public virtual ISequoiaService
{
public:
    void Invoke(const ISequoiaServiceContextPtr& context) override final;

protected:
    DEFINE_YPATH_CONTEXT_IMPL(ISequoiaServiceContext, TTypedSequoiaServiceContext);

    virtual bool DoInvoke(const ISequoiaServiceContextPtr& context) = 0;
};

////////////////////////////////////////////////////////////////////////////////

ISequoiaServicePtr CreateSequoiaService(IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
