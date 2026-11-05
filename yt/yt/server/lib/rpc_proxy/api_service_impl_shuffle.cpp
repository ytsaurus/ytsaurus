#include "api_service_impl.h"

#include <yt/yt/client/api/row_batch_reader.h>
#include <yt/yt/client/api/row_batch_writer.h>

#include <yt/yt/client/api/rpc_proxy/row_stream.h>
#include <yt/yt/client/api/rpc_proxy/wire_row_stream.h>

#include <yt/yt/client/security_client/helpers.h>

#include <yt/yt/client/signature/signature.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/rpc/stream.h>

namespace NYT::NRpcProxy {

using namespace NApi::NRpcProxy;
using namespace NApi;
using namespace NCompression;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NRpc;
using namespace NSecurityClient;
using namespace NSignature;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NYson;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

void TApiService::RegisterShuffleMethods()
{
    RegisterApiMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(StartShuffle));
    RegisterApiMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(WriteShuffleData)
        .SetStreamingEnabled(true)
        .SetCancelable(true));
    RegisterApiMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(ReadShuffleData)
        .SetStreamingEnabled(true)
        .SetCancelable(true));
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TApiService, StartShuffle)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto parentTransactionId = FromProto<TTransactionId>(request->parent_transaction_id());

    context->AnnotateRequest()
        .With("PartitionCount", request->partition_count())
        .With("Account", request->account())
        .With("ParentTransactionId", parentTransactionId);

    auto user = context->GetAuthenticationIdentity().User;

    auto checkResult = WaitFor(client->CheckPermission(
        user,
        GetAccountPath(request->account()),
        EPermission::Use))
        .ValueOrThrow();

    if (checkResult.Action == ESecurityAction::Deny) {
        THROW_ERROR_EXCEPTION("User %Qv has been denied %Qlv access to account %Qv",
            user,
            EPermission::Use,
            request->account());
    }

    ExecuteCall(
        context,
        [client = std::move(client), request, parentTransactionId] () {
            TStartShuffleOptions options;
            if (request->has_medium()) {
                options.Medium = request->medium();
            }
            if (request->has_replication_factor()) {
                options.ReplicationFactor = request->replication_factor();
            }
            options.UsePushBasedShuffle = request->use_push_based_shuffle();
            if (request->has_schema()) {
                FromProto(&options.Schema, request->schema());
            }
            if (request->has_config()) {
                options.Config = TYsonString(request->config());
            }
            options.Codec = FromProto<ECodec>(request->codec());
            return client->StartShuffle(
                request->account(),
                request->partition_count(),
                parentTransactionId,
                std::move(options));
        },
        [] (const auto& context, const auto& signedShuffleHandle) {
            auto* response = &context->Response();
            response->set_signed_shuffle_handle(ToProto(ConvertToYsonString(signedShuffleHandle)));
            // TODO(pavook): friendly YSON wrapper.
            auto shuffleHandle = ConvertTo<TShuffleHandlePtr>(TYsonStringBuf(signedShuffleHandle.Underlying()->Payload()));
            context->AnnotateResponse()
                .With("TransactionId", shuffleHandle->TransactionId);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, ReadShuffleData)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto signedShuffleHandle = ConvertTo<TSignedShuffleHandlePtr>(TYsonStringBuf(request->signed_shuffle_handle()));

    // TODO(pavook): friendly YSON wrappers without double-conversions.
    auto shuffleHandle = ConvertTo<TShuffleHandlePtr>(TYsonStringBuf(signedShuffleHandle.Underlying()->Payload()));

    auto isValid = WaitFor(ValidateSignature(signedShuffleHandle.Underlying()))
        .ValueOrThrow();

    if (!isValid) {
        THROW_ERROR_EXCEPTION("Signature validation failed for shuffle handle")
            .With("shuffle_handle", shuffleHandle);
    }

    std::optional<IShuffleClient::TIndexRange> writerIndexRange;
    if (request->has_writer_index_range()) {
        auto writerIndexBegin = YT_OPTIONAL_FROM_PROTO(request->writer_index_range(), begin);
        auto writerIndexEnd = YT_OPTIONAL_FROM_PROTO(request->writer_index_range(), end);

        if (!writerIndexBegin.has_value() || !writerIndexEnd.has_value()) {
            THROW_ERROR_EXCEPTION("One or both writer index range limits are empty")
                .With("begin", writerIndexBegin)
                .With("end", writerIndexEnd);
        }

        if (*writerIndexBegin > *writerIndexEnd) {
            THROW_ERROR_EXCEPTION(
                "Lower limit of mappers range %v cannot be greater than upper limit %v",
                *writerIndexBegin,
                *writerIndexEnd);
        }

        if (*writerIndexBegin < 0) {
            THROW_ERROR_EXCEPTION("Received negative lower limit of writer index range %v", *writerIndexBegin);
        }

        writerIndexRange = std::pair(*writerIndexBegin, *writerIndexEnd);
    }

    context->AnnotateRequest()
        .With("TransactionId", shuffleHandle->TransactionId)
        .With("CoordinatorAddress", shuffleHandle->CoordinatorAddress)
        .With("Account", shuffleHandle->Account)
        .With("PartitionCount", shuffleHandle->PartitionCount)
        .With("PartitionIndex", request->partition_index())
        .With("WriterIndexRange", writerIndexRange);

    auto reader = WaitFor(client->CreateShuffleReader(
        std::move(signedShuffleHandle),
        request->partition_index(),
        writerIndexRange,
        /*options*/ {}))
        .ValueOrThrow();

    auto encoder = CreateWireRowStreamEncoder(reader->GetNameTable());

    auto config = Config_.Acquire();

    bool finished = false;

    HandleInputStreamingRequest(
        context,
        [&] {
            if (finished) {
                return TSharedRef();
            }

            TRowBatchReadOptions options{
                .MaxRowsPerRead = config->ReadBufferRowCount,
                .MaxDataWeightPerRead = config->ReadBufferDataWeight,
            };
            auto batch = ReadRowBatch(reader, options);
            if (!batch) {
                finished = true;
            }

            return encoder->Encode(
                batch ? batch : CreateEmptyUnversionedRowBatch(),
                nullptr);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, WriteShuffleData)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto signedShuffleHandle = ConvertTo<TSignedShuffleHandlePtr>(TYsonStringBuf(request->signed_shuffle_handle()));

    // TODO(pavook): friendly YSON helpers without double conversions.
    auto shuffleHandle = ConvertTo<TShuffleHandlePtr>(TYsonStringBuf(signedShuffleHandle.Underlying()->Payload()));

    auto isValid = WaitFor(ValidateSignature(signedShuffleHandle.Underlying()))
        .ValueOrThrow();

    if (!isValid) {
        THROW_ERROR_EXCEPTION("Signature validation failed for shuffle handle")
            .With("shuffle_handle", shuffleHandle);
    }

    auto partitionColumn = request->partition_column();

    context->AnnotateRequest()
        .With("TransactionId", shuffleHandle->TransactionId)
        .With("CoordinatorAddress", shuffleHandle->CoordinatorAddress)
        .With("Account", shuffleHandle->Account)
        .With("PartitionCount", shuffleHandle->PartitionCount)
        .With("PartitionColumn", partitionColumn);

    auto writerIndex = request->has_writer_index() ? std::optional<int>(request->writer_index()) : std::nullopt;
    if (writerIndex && *writerIndex < 0) {
        THROW_ERROR_EXCEPTION("Received negative writer index %v", *writerIndex);
    }

    TShuffleWriterOptions options;
    options.OverwriteExistingWriterData = request->overwrite_existing_writer_data();
    if (options.OverwriteExistingWriterData && !writerIndex.has_value()) {
        THROW_ERROR_EXCEPTION("Writer index must be set when overwrite existing writer data option is enabled");
    }

    auto writer = WaitFor(
        client->CreateShuffleWriter(std::move(signedShuffleHandle), partitionColumn, writerIndex, options))
        .ValueOrThrow();

    auto decoder = CreateWireRowStreamDecoder(writer->GetNameTable());

    HandleOutputStreamingRequest(
        context,
        [&] (const TSharedRef& block) {
            NApi::NRpcProxy::NProto::TRowsetDescriptor descriptor;
            auto payloadRef = DeserializeRowStreamBlockEnvelope(block, &descriptor, nullptr);

            auto batch = decoder->Decode(payloadRef, descriptor);

            auto rows = batch->MaterializeRows();

            if (!writer->Write(rows)) {
                WaitFor(writer->GetReadyEvent())
                    .ThrowOnError();
            }
        },
        [&] {
            WaitFor(writer->Close())
                .ThrowOnError();
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
