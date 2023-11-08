#include "orchid_ypath_service.h"

#include "private.h"

#include <yt/yt/ytlib/orchid/orchid_service_proxy.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/ypath_detail.h>

#include <yt/yt/core/rpc/service.h>

namespace NYT::NOrchid {

using namespace NRpc;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = OrchidLogger;

////////////////////////////////////////////////////////////////////////////////

class TOrchidYPathService
    : public IYPathService
{
public:
    explicit TOrchidYPathService(TOrchidOptions options)
        : Options_(std::move(options))
    { }

    TResolveResult Resolve(const TYPath& path, const IYPathServiceContextPtr& /*context*/) override
    {
        return TResolveResultHere{path};
    }

    void Invoke(const IYPathServiceContextPtr& context) override
    {
        context->SetRequestInfo();

        // Throw options-provided error, if it is non-trivial.
        Options_.Error.ThrowOnError();

        // TODO(max42): this is the case only for master orchids, but not for general purpose orchid requests.
        if (IsRequestMutating(context->RequestHeader())) {
            THROW_ERROR_EXCEPTION("Orchid nodes are read-only");
        }

        TOrchidServiceProxy proxy(Options_.Channel);
        if (Options_.Timeout) {
            proxy.SetDefaultTimeout(*Options_.Timeout);
        }

        const auto& path = GetRequestTargetYPath(context->RequestHeader());
        const auto& method = context->GetMethod();

        auto requestMessage = context->GetRequestMessage();
        NRpc::NProto::TRequestHeader requestHeader;
        if (!ParseRequestHeader(requestMessage, &requestHeader)) {
            context->Reply(TError("Error parsing request header"));
            return;
        }

        auto remotePath = Options_.RemoteRoot + path;
        SetRequestTargetYPath(&requestHeader, remotePath);

        auto innerRequestMessage = SetRequestHeader(requestMessage, requestHeader);

        auto outerRequest = proxy.Execute();
        outerRequest->SetMultiplexingBand(EMultiplexingBand::Heavy);
        outerRequest->Attachments() = innerRequestMessage.ToVector();

        YT_LOG_DEBUG("Sending request to remote Orchid (Path: %v, Method: %v, RequestId: %v, OriginalRequestId: %v)",
            remotePath,
            method,
            outerRequest->GetRequestId(),
            context->GetRequestId());

        outerRequest->Invoke().Subscribe(BIND(
            &TOrchidYPathService::OnResponse,
            MakeStrong(this),
            context,
            outerRequest->GetRequestId(),
            remotePath,
            method));
    }

    void DoWriteAttributesFragment(
        IAsyncYsonConsumer* /*consumer*/,
        const TAttributeFilter& /*attributeFilter*/,
        bool /*stable*/) override
    {
        YT_ABORT();
    }

    bool ShouldHideAttributes() override
    {
        YT_ABORT();
    }

private:
    const TOrchidOptions Options_;

    void OnResponse(
        const IServiceContextPtr& context,
        TRequestId requestId,
        const TYPath& path,
        std::string method,
        const TOrchidServiceProxy::TErrorOrRspExecutePtr& rspOrError)
    {
        if (rspOrError.IsOK()) {
            YT_LOG_DEBUG("Orchid request succeeded (RequestId: %v, OriginalRequestId: %v)",
                requestId,
                context->GetRequestId());
            const auto& rsp = rspOrError.Value();
            auto innerResponseMessage = TSharedRefArray(rsp->Attachments(), TSharedRefArray::TMoveParts{});
            context->Reply(std::move(innerResponseMessage));
        } else {
            context->Reply(TError("Error executing Orchid request")
                << TErrorAttribute("path", path)
                << TErrorAttribute("method", method)
                << TErrorAttribute("endpoint", Options_.Channel->GetEndpointDescription())
                << TErrorAttribute("remote_root", Options_.RemoteRoot)
                << rspOrError);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

NYTree::IYPathServicePtr CreateOrchidYPathService(TOrchidOptions options)
{
    return New<TOrchidYPathService>(std::move(options));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrchid
