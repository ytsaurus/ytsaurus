#include "sequoia_service.h"

#include "private.h"

#include "bootstrap.h"
#include "helpers.h"
#include "node_proxy.h"
#include "node_proxy_base.h"
#include "path_resolver.h"
#include "rootstock_proxy.h"

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/ytlib/cypress_client/proto/cypress_ypath.pb.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/sequoia_client/client.h>
#include <yt/yt/ytlib/sequoia_client/transaction.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/core/ypath/tokenizer.h>

namespace NYT::NCypressProxy {

using namespace NConcurrency;
using namespace NCypressClient::NProto;
using namespace NLogging;
using namespace NObjectClient;
using namespace NRpc;
using namespace NSequoiaClient;
using namespace NYTree;
using namespace NServer;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

class TSequoiaServiceContext
    : public TServiceContextBase
    , public ISequoiaServiceContext
{
public:
    explicit TSequoiaServiceContext(TSharedRefArray requestMessage)
        : TServiceContextBase(
            std::move(requestMessage),
            TMemoryUsageTrackerGuard(),
            GetNullMemoryUsageTracker(),
            CypressProxyLogger(),
            ELogLevel::Debug)
    { }

    // TODO(babenko): these three methods is a temporary workaround
    void SetRequestHeader(std::unique_ptr<NRpc::NProto::TRequestHeader> /*header*/) override
    {
        YT_ABORT();
    }

    void SetReadRequestComplexityLimiter(const TReadRequestComplexityLimiterPtr& /*limiter*/) final
    {
        YT_ABORT();
    }

    TReadRequestComplexityLimiterPtr GetReadRequestComplexityLimiter() final
    {
        YT_ABORT();
    }

private:
    // TODO(danilalexeev)
    std::optional<NProfiling::TWallTimer> Timer_;
    const NYTree::NProto::TYPathHeaderExt* CachedYPathExt_ = nullptr;

    const NYTree::NProto::TYPathHeaderExt& GetYPathExt()
    {
        if (!CachedYPathExt_) {
            CachedYPathExt_ = &RequestHeader_->GetExtension(NYTree::NProto::TYPathHeaderExt::ypath_header_ext);
        }
        return *CachedYPathExt_;
    }

    void DoReply() override
    { }

    void LogRequest() override
    {
        const auto& ypathExt = GetYPathExt();

        TStringBuilder builder;
        builder.AppendFormat("%v.%v %v <- ",
            GetService(),
            GetMethod(),
            ypathExt.target_path());

        TDelimitedStringBuilderWrapper delimitedBuilder(&builder);

        auto requestId = GetRequestId();
        if (requestId) {
            delimitedBuilder->AppendFormat("RequestId: %v", requestId);
        }

        delimitedBuilder->AppendFormat("Mutating: %v", ypathExt.mutating());

        auto mutationId = GetMutationId();
        if (mutationId) {
            delimitedBuilder->AppendFormat("MutationId: %v", mutationId);
        }

        if (RequestHeader_->has_user()) {
            delimitedBuilder->AppendFormat("User: %v", RequestHeader_->user());
        }

        delimitedBuilder->AppendFormat("Retry: %v", IsRetry());

        for (const auto& info : RequestInfos_){
            delimitedBuilder->AppendString(info);
        }

        auto logMessage = builder.Flush();
        NTracing::AnnotateTraceContext([&] (const auto& traceContext) {
            traceContext->AddTag(RequestInfoAnnotation, logMessage);
        });
        YT_LOG_DEBUG(logMessage);

        Timer_.emplace();
    }

    void LogResponse() override
    {
        const auto& ypathExt = GetYPathExt();

        TStringBuilder builder;
        builder.AppendFormat("%v.%v %v -> ",
            GetService(),
            GetMethod(),
            ypathExt.target_path());

        TDelimitedStringBuilderWrapper delimitedBuilder(&builder);

        auto requestId = GetRequestId();
        if (requestId) {
            delimitedBuilder->AppendFormat("RequestId: %v", requestId);
        }

        delimitedBuilder->AppendFormat("Mutating: %v", ypathExt.mutating());

        if (RequestHeader_->has_user()) {
            delimitedBuilder->AppendFormat("User: %v", RequestHeader_->user());
        }

        for (const auto& info : ResponseInfos_) {
            delimitedBuilder->AppendString(info);
        }

        if (Timer_) {
            delimitedBuilder->AppendFormat("WallTime: %v", Timer_->GetElapsedTime());
        }

        delimitedBuilder->AppendFormat("Error: %v", Error_);

        auto logMessage = builder.Flush();
        NTracing::AnnotateTraceContext([&] (const auto& traceContext) {
            traceContext->AddTag(ResponseInfoAnnotation, logMessage);
        });
        YT_LOG_DEBUG(logMessage);
    }
};

////////////////////////////////////////////////////////////////////////////////

ISequoiaServiceContextPtr CreateSequoiaServiceContext(TSharedRefArray requestMessage)
{
    return New<TSequoiaServiceContext>(std::move(requestMessage));
}

////////////////////////////////////////////////////////////////////////////////

class TSequoiaService
    : public ISequoiaService
{
public:
    explicit TSequoiaService(IBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    EInvokeResult TryInvoke(
        const ISequoiaServiceContextPtr& context,
        const TSequoiaSessionPtr& session,
        const TResolveResult& resolveResult) override
    {
        static_assert(std::variant_size<std::decay_t<decltype(resolveResult)>>() == 2);

        TNodeProxyBasePtr proxy;
        if (const auto* cypressResolveResult = std::get_if<TCypressResolveResult>(&resolveResult)) {
            if (context->GetRequestHeader().method() != "Create") {
                return EInvokeResult::ForwardToMaster;
            }

            auto reqCreate = TryParseReqCreate(context);
            if (!reqCreate) {
                // Error during parsing.
                return EInvokeResult::Executed;
            }

            switch (reqCreate->Type) {
                // NB: For rootstock resolve cannot be performed on Сypress
                // proxy, but the transaction has to be started there. We assume
                // path is correct, if this is not the case - prepare in 2PC
                // will fail and the error will be propagated to the user.
                case EObjectType::Rootstock:
                    proxy = CreateRootstockProxy(
                        Bootstrap_,
                        session,
                        TAbsoluteYPath(cypressResolveResult->Path));
                    break;

                // Link may point into Sequoia we cannot check it's cyclicity in
                // master. Of course, checking it here is racy, but there is no
                // better solution for now.
                // TODO(kvk1920): deal with cyclicity checking in common case
                // (subtree copy?).
                case EObjectType::Link:
                    try {
                        ValidateLinkNodeCreation(
                            session,
                            reqCreate->ExplicitAttributes->Get<TRawYPath>(
                                EInternedAttributeKey::TargetPath.Unintern()),
                            resolveResult);
                        // Link should be created in master.
                        return EInvokeResult::ForwardToMaster;
                    } catch (const std::exception& ex) {
                        context->Reply(ex);
                        return EInvokeResult::Executed;
                    }
                    break;

                default:
                    return EInvokeResult::ForwardToMaster;
            }
        } else {
            const auto& sequoiaResolveResult = std::get<TSequoiaResolveResult>(resolveResult);
            proxy = CreateNodeProxy(Bootstrap_, session, sequoiaResolveResult);
        }

        return proxy->Invoke(context);
    }

private:
    IBootstrap* const Bootstrap_;
};

////////////////////////////////////////////////////////////////////////////////

ISequoiaServicePtr CreateSequoiaService(IBootstrap* bootstrap)
{
    return New<TSequoiaService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
