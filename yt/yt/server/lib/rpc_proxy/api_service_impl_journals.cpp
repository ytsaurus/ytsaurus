#include "api_service_impl.h"

#include <yt/yt/client/api/config.h>
#include <yt/yt/client/api/journal_reader.h>
#include <yt/yt/client/api/journal_writer.h>

#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/core/rpc/stream.h>

namespace NYT::NRpcProxy {

using namespace NApi::NRpcProxy;
using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NRpc;
using namespace NYTree;
using namespace NYson;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

void TApiService::RegisterJournalMethods(TMultiproxyMethodList* methodList)
{
    auto registerMethod = [&] (EMultiproxyMethodKind methodKind, TMethodDescriptor&& descriptor) {
        RegisterMethodForMultiproxy(methodList, methodKind, descriptor);
    };

    registerMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(ReadJournal)
        .SetStreamingEnabled(true)
        .SetCancelable(true));
    registerMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(WriteJournal)
        .SetStreamingEnabled(true)
        .SetCancelable(true));
    registerMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(TruncateJournal));
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TApiService, ReadJournal)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    const auto& path = request->path();

    TJournalReaderOptions options;
    if (request->has_first_row_index()) {
        options.FirstRowIndex = request->first_row_index();
    }
    if (request->has_row_count()) {
        options.RowCount = request->row_count();
    }
    if (request->has_config()) {
        options.Config = ConvertTo<TJournalReaderConfigPtr>(TYsonString(request->config()));
    }

    if (request->has_transactional_options()) {
        FromProto(&options, request->transactional_options());
    }
    if (request->has_suppressable_access_tracking_options()) {
        FromProto(&options, request->suppressable_access_tracking_options());
    }

    context->AnnotateRequest()
        .With("Path", path)
        .With("FirstRowIndex", options.FirstRowIndex)
        .With("RowCount", options.RowCount);

    auto journalReader = client->CreateJournalReader(path, options);
    WaitFor(journalReader->Open())
        .ThrowOnError();

    HandleInputStreamingRequest(
        context,
        [&] {
            auto rows = WaitFor(journalReader->Read())
                .ValueOrThrow();

            if (rows.empty()) {
                return TSharedRef();
            }

            return PackRefs(rows);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, WriteJournal)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    const auto& path = request->path();

    TJournalWriterOptions options;
    if (request->has_config()) {
        options.Config = ConvertTo<TJournalWriterConfigPtr>(TYsonString(request->config()));
    }
    options.EnableMultiplexing = request->enable_multiplexing();
    options.EnableChunkPreallocation = request->enable_chunk_preallocation();
    options.ReplicaLagLimit = request->replica_lag_limit();

    if (request->has_transactional_options()) {
        FromProto(&options, request->transactional_options());
    }
    if (request->has_prerequisite_options()) {
        FromProto(&options, request->prerequisite_options());
    }

    context->AnnotateRequest()
        .With("Path", path)
        .With("EnableMultiplexing", options.EnableMultiplexing)
        .With("EnableChunkPreallocation", options.EnableChunkPreallocation)
        .With("ReplicaLagLimit", options.ReplicaLagLimit);

    auto journalWriter = client->CreateJournalWriter(path, options);
    WaitFor(journalWriter->Open())
        .ThrowOnError();

    HandleOutputStreamingRequest(
        context,
        [&] (const TSharedRef& packedRows) {
            auto rows = UnpackRefs(packedRows);
            WaitFor(journalWriter->Write(rows))
                .ThrowOnError();
        },
        [&] {
            WaitFor(journalWriter->Close())
                .ThrowOnError();
        },
        true /*feedbackEnabled*/);
}

DEFINE_RPC_SERVICE_METHOD(TApiService, TruncateJournal)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    const auto& path = request->path();
    auto rowCount = request->row_count();

    TTruncateJournalOptions options;
    SetTimeoutOptions(&options, context.Get());
    SetMutatingOptions(&options, request, context.Get());
    if (request->has_prerequisite_options()) {
        FromProto(&options, request->prerequisite_options());
    }

    context->AnnotateRequest()
        .With("Path", path)
        .With("RowCount", rowCount);

    ExecuteCall(
        context,
        [=] {
            return client->TruncateJournal(
                path,
                rowCount,
                options);
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
