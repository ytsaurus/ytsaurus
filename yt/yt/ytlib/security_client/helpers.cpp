#include "helpers.h"

#include <yt/yt/ytlib/api/native/rpc_helpers.h>
#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/client/security_client/helpers.h>

#include <yt/yt/core/ytree/ypath_proxy.h>

namespace NYT::NSecurityClient {

using namespace NApi::NNative;
using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

THashSet<TString> GetSubjectClosure(
    const TString& subject,
    NObjectClient::TObjectServiceProxy& proxy,
    const NApi::NNative::IConnectionPtr& connection,
    const NApi::TMasterReadOptions& options)
{
    // TODO(max42): Why doing raw YPath request instead of using client method?
    auto batchReq = proxy.ExecuteBatch();
    SetBalancingHeader(batchReq, connection, options);
    for (const auto& path : {GetUserPath(subject), GetGroupPath(subject)}) {
        auto req = TYPathProxy::Get(path + "/@member_of_closure");
        SetCachingHeader(req, connection, options);
        batchReq->AddRequest(req);
    }
    auto batchRsp = WaitFor(batchReq->Invoke())
        .ValueOrThrow();
    for (const auto& rspOrError : batchRsp->GetResponses<TYPathProxy::TRspGet>()) {
        if (rspOrError.IsOK()) {
            auto res = ConvertTo<THashSet<TString>>(TYsonString(rspOrError.Value()->value()));
            res.insert(subject);
            return res;
        } else if (!rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
            THROW_ERROR_EXCEPTION(
                "Failed to get \"member_of_closure\" attribute for subject %Qv",
                subject)
                << rspOrError;
        }
    }
    THROW_ERROR_EXCEPTION(
        "Unrecognized subject %Qv",
        subject);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
