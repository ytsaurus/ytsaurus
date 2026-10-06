#include "api_service_impl.h"

#include "helpers.h"

#include <yt/yt/client/api/config.h>
#include <yt/yt/client/api/file_reader.h>
#include <yt/yt/client/api/file_writer.h>

#include <yt/yt/client/api/rpc_proxy/request_tags.h>

#include <yt/yt/client/chunk_client/config.h>

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

void TApiService::RegisterFileMethods(TMultiproxyMethodList* methodList)
{
    auto registerMethod = [&] (EMultiproxyMethodKind methodKind, TMethodDescriptor&& descriptor) {
        RegisterMethodForMultiproxy(methodList, methodKind, descriptor);
    };

    registerMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(ReadFile)
        .SetStreamingEnabled(true)
        .SetCancelable(true));
    registerMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(WriteFile)
        .SetStreamingEnabled(true)
        .SetCancelable(true));
    registerMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(PartitionFile));
    registerMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(ReadFilePartition)
        .SetStreamingEnabled(true)
        .SetCancelable(true));
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TApiService, ReadFile)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    const auto& path = request->path();

    TFileReaderOptions options;
    if (request->has_offset()) {
        options.Offset = request->offset();
    }
    if (request->has_length()) {
        options.Length = request->length();
    }
    if (request->has_config()) {
        options.Config = ConvertTo<TFileReaderConfigPtr>(TYsonString(request->config()));
    }

    if (request->has_transactional_options()) {
        FromProto(&options, request->transactional_options());
    }
    if (request->has_suppressable_access_tracking_options()) {
        FromProto(&options, request->suppressable_access_tracking_options());
    }

    context->AnnotateRequest().With(MakeReadFileRequestTags(*request));

    PutMethodInfoInTraceContext("read_file");

    auto fileReader = WaitFor(client->CreateFileReader(path, options))
        .ValueOrThrow();

    auto outputStream = context->GetResponseAttachmentsStream();

    NApi::NRpcProxy::NProto::TReadFileMeta meta;
    ToProto(meta.mutable_id(), fileReader->GetId());
    meta.set_revision(ToProto(fileReader->GetRevision()));

    auto metaRef = SerializeProtoToRef(meta);
    WaitFor(outputStream->Write(metaRef))
        .ThrowOnError();

    HandleInputStreamingRequest(context, fileReader);
}

DEFINE_RPC_SERVICE_METHOD(TApiService, WriteFile)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto path = FromProto<NYPath::TRichYPath>(request->path());

    TFileWriterOptions options;
    options.ComputeMD5 = request->compute_md5();
    if (request->has_config()) {
        options.Config = ConvertTo<TFileWriterConfigPtr>(TYsonString(request->config()));
    }

    if (request->has_transactional_options()) {
        FromProto(&options, request->transactional_options());
    }
    if (request->has_prerequisite_options()) {
        FromProto(&options, request->prerequisite_options());
    }

    context->AnnotateRequest().With(MakeWriteFileRequestTags(path, *request));

    PutMethodInfoInTraceContext("write_file");

    auto fileWriter = client->CreateFileWriter(path, options);
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
        },
        false /*feedbackEnabled*/);
}

DEFINE_RPC_SERVICE_METHOD(TApiService, PartitionFile)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto path = NYPath::TYPath(request->path());

    TPartitionFileOptions options;
    SetTimeoutOptions(&options, context.Get());

    std::vector<TFileReadRange> ranges;
    ranges.reserve(request->ranges_size());
    for (const auto& protoRange : request->ranges()) {
        TFileReadRange range;
        range.Begin = protoRange.begin();
        if (protoRange.has_end()) {
            range.End = protoRange.end();
        }
        ranges.push_back(std::move(range));
    }

    if (request->has_fetch_chunk_spec_config()) {
        options.FetchChunkSpecConfig = New<TFetchChunkSpecConfig>();
        FromProto(options.FetchChunkSpecConfig, request->fetch_chunk_spec_config());
    }

    options.FetchCookieNodeDescriptors = request->fetch_cookie_node_descriptors();

    if (request->has_transactional_options()) {
        FromProto(&options, request->transactional_options());
    }
    if (request->has_suppressable_access_tracking_options()) {
        FromProto(&options, request->suppressable_access_tracking_options());
    }

    context->AnnotateRequest().With(MakePartitionFileRequestTags(*request));

    PutMethodInfoInTraceContext("partition_file");

    ExecuteCall(
        context,
        [=] {
            return client->PartitionFile(path, ranges, options);
        },
        [] (const auto& context, const auto& result) {
            auto* response = &context->Response();
            ToProto(response->mutable_partitions(), result.Partitions);

            context->AnnotateResponse()
                .With("PartitionCount", result.Partitions.size());
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, ReadFilePartition)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto cookie = ConvertTo<TFilePartitionCookiePtr>(TYsonStringBuf(request->cookie()));

    auto signatureOk = WaitFor(ValidateSignature(cookie.Underlying()))
        .ValueOrThrow();
    if (!signatureOk) {
        THROW_ERROR_EXCEPTION("Signature validation failed");
    }

    TReadFilePartitionOptions options;
    if (request->has_config()) {
        options.Config = ConvertTo<TFileReaderConfigPtr>(TYsonString(request->config()));
    }

    context->AnnotateRequest().With(MakeReadFilePartitionRequestTags(*request));

    PutMethodInfoInTraceContext("read_file_partition");

    auto reader = WaitFor(client->CreateFilePartitionReader(cookie, options))
        .ValueOrThrow();

    auto outputStream = context->GetResponseAttachmentsStream();

    NApi::NRpcProxy::NProto::TRspReadFilePartitionMeta meta;
    ToProto(meta.mutable_id(), reader->GetId());
    meta.set_revision(ToProto(reader->GetRevision()));

    auto metaRef = SerializeProtoToRef(meta);
    WaitFor(outputStream->Write(metaRef))
        .ThrowOnError();

    HandleInputStreamingRequest(context, reader);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
