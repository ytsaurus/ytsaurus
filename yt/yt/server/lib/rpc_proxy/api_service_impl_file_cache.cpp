#include "api_service_impl.h"

namespace NYT::NRpcProxy {

using namespace NApi::NRpcProxy;
using namespace NApi;
using namespace NConcurrency;
using namespace NRpc;
using namespace NYTree;
using namespace NYson;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

void TMasterMetadataApiService::RegisterFileCacheMethods()
{
    RegisterApiMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(GetFileFromCache));
    RegisterApiMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(PutFileToCache));
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TMasterMetadataApiService, GetFileFromCache)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto md5 = request->md5();

    TGetFileFromCacheOptions options;
    SetTimeoutOptions(&options, context.Get());

    options.CachePath = request->cache_path();
    if (request->has_transactional_options()) {
        FromProto(&options, request->transactional_options());
    }
    if (request->has_master_read_options()) {
        FromProto(&options, request->master_read_options());
    }

    context->AnnotateRequest()
        .With("MD5", md5)
        .With("CachePath", options.CachePath);

    ExecuteCall(
        context,
        [=] {
            return client->GetFileFromCache(md5, options);
        },
        [] (const auto& context, const auto& result) {
            auto* response = &context->Response();
            ToProto(response->mutable_result(), result);

            context->AnnotateResponse()
                .With("Path", result.Path);
        });
}

DEFINE_RPC_SERVICE_METHOD(TMasterMetadataApiService, PutFileToCache)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto path = request->path();
    auto md5 = request->md5();

    TPutFileToCacheOptions options;
    SetTimeoutOptions(&options, context.Get());
    SetMutatingOptions(&options, request, context.Get());

    options.CachePath = request->cache_path();
    if (request->has_preserve_expiration_timeout()) {
        options.PreserveExpirationTimeout = request->preserve_expiration_timeout();
    }
    if (request->has_transactional_options()) {
        FromProto(&options, request->transactional_options());
    }
    if (request->has_prerequisite_options()) {
        FromProto(&options, request->prerequisite_options());
    }
    if (request->has_master_read_options()) {
        FromProto(&options, request->master_read_options());
    }

    context->AnnotateRequest()
        .With("Path", path)
        .With("MD5", md5)
        .With("CachePath", options.CachePath);

    ExecuteCall(
        context,
        [=] {
            return client->PutFileToCache(path, md5, options);
        },
        [] (const auto& context, const auto& result) {
            auto* response = &context->Response();
            ToProto(response->mutable_result(), result);

            context->AnnotateResponse()
                .With("Path", result.Path);
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
