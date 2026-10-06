#include "api_service_impl.h"

#include "helpers.h"

#include <yt/yt/client/api/distributed_file_session.h>
#include <yt/yt/client/api/file_writer.h>

#include <yt/yt/client/api/rpc_proxy/request_tags.h>

#include <yt/yt/client/signature/signature.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/rpc/stream.h>

namespace NYT::NRpcProxy {

using namespace NApi::NRpcProxy;
using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NRpc;
using namespace NSignature;
using namespace NTracing;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

void TApiService::RegisterDistributedFileMethods(TMultiproxyMethodList* methodList)
{
    auto registerMethod = [&] (EMultiproxyMethodKind methodKind, TMethodDescriptor&& descriptor) {
        RegisterMethodForMultiproxy(methodList, methodKind, descriptor);
    };

    registerMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(StartDistributedWriteFileSession)
        .SetCancelable(true));
    registerMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(PingDistributedWriteFileSession));
    registerMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(FinishDistributedWriteFileSession));
    registerMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(WriteFileFragment)
        .SetStreamingEnabled(true)
        .SetCancelable(true));
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TApiService, StartDistributedWriteFileSession)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    TRichYPath path;
    TDistributedWriteFileSessionStartOptions options;
    ParseRequest(&path, &options, *request);
    context->AnnotateRequest().With(MakeStartDistributedWriteFileSessionRequestTags(path));

    ExecuteCall(
        context,
        [=] {
            return client->StartDistributedWriteFileSession(path, options);
        },
        [] (const auto& context, const auto& result) {
            context->Response().set_signed_session(ToProto(ConvertToYsonString(result.Session)));
            for (const auto& cookie : result.Cookies) {
                context->Response().add_signed_cookies(ConvertToYsonString(cookie).ToString());
            }
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, PingDistributedWriteFileSession)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    TSignedDistributedWriteFileSessionPtr session;
    TDistributedWriteFileSessionPingOptions options;
    ParseRequest(&session, &options, *request);

    auto concreteSession = ConvertTo<TDistributedWriteFileSession>(TYsonStringBuf(session.Underlying()->Payload()));
    context->AnnotateRequest().With(MakePingDistributedWriteFileSessionRequestTags(concreteSession.HostData.FileId));

    ExecuteCall(
        context,
        [=] {
            return client->PingDistributedWriteFileSession(session, options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, FinishDistributedWriteFileSession)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    TDistributedWriteFileSessionWithResults sessionWithResults;

    TDistributedWriteFileSessionFinishOptions options;
    ParseRequest(&sessionWithResults, &options, *request);

    auto session = ConvertTo<TDistributedWriteFileSession>(TYsonStringBuf(sessionWithResults.Session.Underlying()->Payload()));

    context->AnnotateRequest().With(MakeFinishDistributedWriteFileSessionRequestTags(session.HostData.FileId));

    ExecuteCall(
        context,
        [=, this, options = std::move(options), sessionWithResults = std::move(sessionWithResults), sessionId = session.RootChunkListId] {
            std::vector<TFuture<bool>> validation;
            validation.reserve(1 + std::ssize(sessionWithResults.Results));
            validation.push_back(ValidateSignature(sessionWithResults.Session.Underlying()));
            for (const auto& signedResult : sessionWithResults.Results) {
                auto result = ConvertTo<TWriteFileFragmentResult>(TYsonStringBuf(signedResult.Underlying()->Payload()));
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
                        "Signature validation failed for distributed write file session finish");
                    return client->FinishDistributedWriteFileSession(std::move(sessionWithResults), options);
                }));
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, WriteFileFragment)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    PutMethodInfoInTraceContext("write_file_fragment");

    TSignedWriteFileFragmentCookiePtr cookie;

    TFileFragmentWriterOptions options;
    ParseRequest(&cookie, &options, *request);

    auto concreteCookie = ConvertTo<TWriteFileFragmentCookie>(TYsonStringBuf(cookie.Underlying()->Payload()));
    const auto& cookieData = concreteCookie.CookieData;

    context->AnnotateRequest().With(MakeWriteFileFragmentRequestTags(cookieData.FileId, cookieData.MainTransactionId));

    auto isValid = WaitFor(ValidateSignature(cookie.Underlying()))
        .ValueOrThrow();

    if (!isValid) {
        THROW_ERROR_EXCEPTION(
            "Signature validation failed for write file fragment")
                .With("session_id", concreteCookie.SessionId)
                .With("cookie_id", concreteCookie.CookieId);
    }

    auto fileWriter = client->CreateFileFragmentWriter(cookie, options);

    WaitFor(fileWriter->Open())
        .ThrowOnError();

    HandleOutputStreamingRequest(
        context,
        [&] (TSharedRef block) {
            WaitFor(fileWriter->Write(std::move(block)))
                .ThrowOnError();
        },
        [&] {
            WaitFor(fileWriter->Close())
                .ThrowOnError();
            auto writeResult = fileWriter->GetWriteFragmentResult();
            response->set_signed_write_result(ToProto(ConvertToYsonString(writeResult)));
        },
        /*feedbackEnabled*/ false);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
