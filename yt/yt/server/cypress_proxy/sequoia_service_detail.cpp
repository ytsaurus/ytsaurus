#include "sequoia_service_detail.h"

namespace NYT::NCypressProxy {

using namespace NRpc;
using namespace NSequoiaClient;

////////////////////////////////////////////////////////////////////////////////

template <class... TArgs>
TSequoiaServiceContext::TSequoiaServiceContext(
    ISequoiaTransactionPtr transaction,
    TArgs&&... args)
    : TServiceContextBase(std::forward<TArgs>(args)...)
    , Transaction_(std::move(transaction))
{ }

void TSequoiaServiceContext::SetRequestHeader(std::unique_ptr<NRpc::NProto::TRequestHeader> header)
{
    RequestHeader_ = std::move(header);
    RequestMessage_ = NRpc::SetRequestHeader(RequestMessage_, *RequestHeader_);
    CachedYPathExt_ = nullptr;
}

const ISequoiaTransactionPtr& TSequoiaServiceContext::GetSequoiaTransaction() const
{
    return Transaction_;
}

const TResolveResult& TSequoiaServiceContext::GetResolveResultOrThrow() const
{
    if (!ResolveResult_) {
        THROW_ERROR_EXCEPTION("Resolve result is missing");
    }
    return *ResolveResult_;
}

void TSequoiaServiceContext::SetResolveResult(TResolveResult resolveResult)
{
    ResolveResult_ = std::move(resolveResult);
}

TRange<TResolveStep> TSequoiaServiceContext::GetResolveHistory() const
{
    return ResolveHistory_;
}

void TSequoiaServiceContext::SetResolveHistory(std::vector<TResolveStep> resolveHistory)
{
    ResolveHistory_ = std::move(resolveHistory);
}

std::optional<TResolveStep> TSequoiaServiceContext::TryGetLastResolveStep() const
{
    return !ResolveHistory_.empty()
        ? std::make_optional(ResolveHistory_.back())
        : std::nullopt;
}

const NYTree::NProto::TYPathHeaderExt& TSequoiaServiceContext::GetYPathExt()
{
    if (!CachedYPathExt_) {
        CachedYPathExt_ = &RequestHeader_->GetExtension(NYTree::NProto::TYPathHeaderExt::ypath_header_ext);
    }
    return *CachedYPathExt_;
}

void TSequoiaServiceContext::DoReply()
{ }

void TSequoiaServiceContext::LogRequest()
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

void TSequoiaServiceContext::LogResponse()
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

////////////////////////////////////////////////////////////////////////////////

TIntrusivePtr<TSequoiaServiceContext> CreateSequoiaContext(
    TSharedRefArray requestMessage,
    ISequoiaTransactionPtr transaction,
    NLogging::TLogger logger,
    NLogging::ELogLevel logLevel)
{
    return New<TSequoiaServiceContext>(
        std::move(transaction),
        std::move(requestMessage),
        TMemoryUsageTrackerGuard(),
        GetNullMemoryUsageTracker(),
        std::move(logger),
        logLevel);
}

////////////////////////////////////////////////////////////////////////////////

void TSequoiaServiceBase::Invoke(const ISequoiaServiceContextPtr& context)
{
    try {
        if (!DoInvoke(context)) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::NoSuchMethod,
                "%Qv method is not supported",
                context->GetMethod());
        }
    } catch (const std::exception& ex) {
        context->Reply(ex);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
