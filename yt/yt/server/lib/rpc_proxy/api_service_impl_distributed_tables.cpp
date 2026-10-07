#include "api_service_impl.h"

#include <yt/yt/client/api/distributed_table_session.h>

#include <yt/yt/client/api/rpc_proxy/request_tags.h>

#include <yt/yt/client/signature/signature.h>

#include <yt/yt/client/ypath/rich.h>

namespace NYT::NRpcProxy {

using namespace NApi::NRpcProxy;
using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NRpc;
using namespace NSignature;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

void TApiService::RegisterDistributedTableMethods(TMultiproxyMethodList* methodList)
{
    auto registerMethod = [&] (EMultiproxyMethodKind methodKind, TMethodDescriptor&& descriptor) {
        RegisterMethodForMultiproxy(methodList, methodKind, descriptor);
    };

    registerMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(StartDistributedWriteSession)
        .SetCancelable(true));
    registerMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(PingDistributedWriteSession));
    registerMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(FinishDistributedWriteSession));
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TApiService, StartDistributedWriteSession)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    TRichYPath path;
    TDistributedWriteSessionStartOptions options;
    ParseRequest(&path, &options, *request);

    context->AnnotateRequest().With(MakeStartDistributedWriteSessionRequestTags(path));

    ExecuteCall(
        context,
        [=] {
            return client->StartDistributedWriteSession(path, options);
        },
        [] (const auto& context, const auto& result) {
            context->Response().set_signed_session(ToProto(ConvertToYsonString(result.Session)));
            for (const auto& cookie : result.Cookies) {
                context->Response().add_signed_cookies(ConvertToYsonString(cookie).ToString());
            }
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, PingDistributedWriteSession)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    TSignedDistributedWriteSessionPtr session;
    TDistributedWriteSessionPingOptions options;
    ParseRequest(&session, &options, *request);

    auto concreteSession = ConvertTo<TDistributedWriteSession>(TYsonStringBuf(session.Underlying()->Payload()));
    context->AnnotateRequest().With(MakePingDistributedWriteSessionRequestTags(concreteSession.PatchInfo.ObjectId));

    ExecuteCall(
        context,
        [=] {
            return client->PingDistributedWriteSession(std::move(session), options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, FinishDistributedWriteSession)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    TDistributedWriteSessionWithResults sessionWithResults;

    TDistributedWriteSessionFinishOptions options;
    ParseRequest(&sessionWithResults, &options, *request);

    auto session = ConvertTo<TDistributedWriteSession>(TYsonStringBuf(sessionWithResults.Session.Underlying()->Payload()));

    context->AnnotateRequest().With(MakeFinishDistributedWriteSessionRequestTags(session.PatchInfo.ObjectId));

    ExecuteCall(
        context,
        [=, this, options = std::move(options), sessionWithResults = std::move(sessionWithResults), sessionId = session.RootChunkListId] {
            std::vector<TFuture<bool>> validation;
            validation.reserve(1 + std::ssize(sessionWithResults.Results));
            validation.push_back(ValidateSignature(sessionWithResults.Session.Underlying()));
            for (const auto& signedResult : sessionWithResults.Results) {
                auto result = ConvertTo<TWriteFragmentResult>(TYsonStringBuf(signedResult.Underlying()->Payload()));
                if (sessionId != result.SessionId) {
                    THROW_ERROR_EXCEPTION(
                        "Found write results with a different session id")
                        .With("finish_distributed_write_session_id", sessionId)
                        .With("write_result_session_id", result.SessionId)
                        .With("cookie_id", result.CookieId);
                }
                validation.push_back(ValidateSignature(signedResult.Underlying()));
            }

            return AllSucceeded(std::move(validation))
                .AsUnique().Apply(BIND([=, options = std::move(options), sessionWithResults = std::move(sessionWithResults)] (std::vector<bool>&& results) {
                    auto allValid = std::ranges::all_of(results, [] (bool value) {
                        return value;
                    });
                    THROW_ERROR_EXCEPTION_UNLESS(
                        allValid,
                        "Signature validation failed for distributed write session finish");
                    return client->FinishDistributedWriteSession(std::move(sessionWithResults), options);
                }));
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
