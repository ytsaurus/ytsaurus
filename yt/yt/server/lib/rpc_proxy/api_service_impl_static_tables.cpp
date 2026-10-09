#include "api_service_impl.h"

#include "format_row_stream.h"
#include "helpers.h"

#include <yt/yt/client/arrow/arrow_row_stream_decoder.h>
#include <yt/yt/client/arrow/arrow_row_stream_encoder.h>

#include <yt/yt/client/formats/config.h>

#include <yt/yt/client/api/config.h>
#include <yt/yt/client/api/distributed_table_session.h>
#include <yt/yt/client/api/table_partition_reader.h>
#include <yt/yt/client/api/table_reader.h>
#include <yt/yt/client/api/table_writer.h>

#include <yt/yt/client/api/rpc_proxy/request_tags.h>
#include <yt/yt/client/api/rpc_proxy/row_stream.h>
#include <yt/yt/client/api/rpc_proxy/wire_row_stream.h>

#include <yt/yt/client/chunk_client/config.h>

#include <yt/yt/client/signature/signature.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/wire_protocol.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/rpc/stream.h>

namespace NYT::NRpcProxy {

using namespace NApi::NDetail;
using namespace NApi::NRpcProxy;
using namespace NApi;
using namespace NArrow;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NFormats;
using namespace NRpc;
using namespace NSignature;
using namespace NTableClient;
using namespace NTracing;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

namespace {

[[noreturn]] void ThrowUnsupportedRowsetFormat(NApi::NRpcProxy::NProto::ERowsetFormat rowsetFormat)
{
    THROW_ERROR_EXCEPTION(
        "Unsupported rowset format %Qv",
        NApi::NRpcProxy::NProto::ERowsetFormat_Name(rowsetFormat));
}

IRowStreamEncoderPtr CreateRowStreamEncoder(
    NApi::NRpcProxy::NProto::ERowsetFormat rowsetFormat,
    NApi::NRpcProxy::NProto::ERowsetFormat arrowFallbackRowsetFormat,
    TTableSchemaPtr schema,
    std::optional<std::vector<std::string>> columns,
    TNameTablePtr nameTable,
    NFormats::TControlAttributesConfigPtr controlAttributesConfig,
    std::optional<NFormats::TFormat> format)
{
    auto createNonArrowEncoder = [&] (NApi::NRpcProxy::NProto::ERowsetFormat rowsetFormat) -> IRowStreamEncoderPtr {
        switch (rowsetFormat) {
            case NApi::NRpcProxy::NProto::RF_YT_WIRE:
                return CreateWireRowStreamEncoder(nameTable);
            case NApi::NRpcProxy::NProto::RF_FORMAT:
                if (!format) {
                    THROW_ERROR_EXCEPTION("No format for %Qv", NApi::NRpcProxy::NProto::ERowsetFormat_Name(rowsetFormat));
                }
                return CreateFormatRowStreamEncoder(
                    nameTable,
                    *format,
                    schema,
                    columns,
                    controlAttributesConfig);
            case NApi::NRpcProxy::NProto::RF_ARROW:
                YT_ABORT();
            default:
                ThrowUnsupportedRowsetFormat(rowsetFormat);
        }
    };

    switch (rowsetFormat) {
        case NApi::NRpcProxy::NProto::RF_YT_WIRE:
        case NApi::NRpcProxy::NProto::RF_FORMAT:
            return createNonArrowEncoder(rowsetFormat);
        case NApi::NRpcProxy::NProto::RF_ARROW: {
            if (arrowFallbackRowsetFormat == NApi::NRpcProxy::NProto::RF_ARROW) {
                THROW_ERROR_EXCEPTION("Arrow fallback rowset format must be different from arrow");
            }

            auto fallbackEncoder = createNonArrowEncoder(arrowFallbackRowsetFormat);
            return CreateArrowRowStreamEncoder(schema, std::move(columns), nameTable, fallbackEncoder, controlAttributesConfig);
        }
        default:
            ThrowUnsupportedRowsetFormat(rowsetFormat);
    }
}

IRowStreamDecoderPtr CreateRowStreamDecoder(
    NApi::NRpcProxy::NProto::ERowsetFormat rowsetFormat,
    TTableSchemaPtr schema,
    TNameTablePtr nameTable,
    std::optional<NFormats::TFormat> format)
{
    switch (rowsetFormat) {
        case NApi::NRpcProxy::NProto::RF_YT_WIRE:
            return CreateWireRowStreamDecoder(std::move(nameTable), CreateUnlimitedWireProtocolOptions());

        case NApi::NRpcProxy::NProto::RF_ARROW:
            return CreateArrowRowStreamDecoder(std::move(schema), std::move(nameTable));

        case NApi::NRpcProxy::NProto::RF_FORMAT:
            if (!format) {
                THROW_ERROR_EXCEPTION("No format for %Qv", NApi::NRpcProxy::NProto::ERowsetFormat_Name(rowsetFormat));
            }
            return CreateFormatRowStreamDecoder(std::move(nameTable), std::move(*format), std::move(schema));

        default:
            THROW_ERROR_EXCEPTION("Unsupported rowset format %Qv",
                NApi::NRpcProxy::NProto::ERowsetFormat_Name(rowsetFormat));
    }
}

bool IsColumnarRowsetFormat(NApi::NRpcProxy::NProto::ERowsetFormat format)
{
    return format == NApi::NRpcProxy::NProto::RF_ARROW;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TApiService::RegisterStaticTableMethods()
{
    RegisterApiMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(ReadTable)
        .SetStreamingEnabled(true)
        .SetCancelable(true));
    RegisterApiMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(WriteTable)
        .SetStreamingEnabled(true)
        .SetCancelable(true));
    RegisterApiMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(GetColumnarStatistics));
    RegisterApiMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(PartitionTables));
    RegisterApiMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(ReadTablePartition)
        .SetStreamingEnabled(true)
        .SetCancelable(true));

    RegisterApiMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(WriteTableFragment)
        .SetStreamingEnabled(true)
        .SetCancelable(true));
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TApiService, ReadTable)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    TRichYPath path;
    std::optional<NYson::TYsonStringBuf> rawFormat;
    NApi::NRpcProxy::NProto::ERowsetFormat desiredRowsetFormat;
    NApi::NRpcProxy::NProto::ERowsetFormat arrowFallbackRowsetFormat;

    NApi::TTableReaderOptions options;

    ParseRequest(&path, &rawFormat, &desiredRowsetFormat, &arrowFallbackRowsetFormat, &options, *request);

    std::optional<NFormats::TFormat> format;
    if (rawFormat) {
        ValidateFormat(context->GetAuthenticationIdentity().User, ConvertToNode(*rawFormat));
        format = ConvertTo<NFormats::TFormat>(*rawFormat);
    }

    context->AnnotateRequest().With(MakeReadTableRequestTags(path, *request));

    PutMethodInfoInTraceContext("read_table");

    auto tableReader = WaitFor(client->CreateTableReader(path, options))
        .ValueOrThrow();

    auto controlAttributesConfig = New<NFormats::TControlAttributesConfig>();
    controlAttributesConfig->EnableRowIndex = request->enable_row_index();
    controlAttributesConfig->EnableTableIndex = request->enable_table_index();
    controlAttributesConfig->EnableRangeIndex = request->enable_range_index();

    auto encoder = CreateRowStreamEncoder(
        desiredRowsetFormat,
        arrowFallbackRowsetFormat,
        tableReader->GetTableSchema(),
        path.GetColumns(),
        tableReader->GetNameTable(),
        controlAttributesConfig,
        std::move(format));

    auto outputStream = context->GetResponseAttachmentsStream();
    NApi::NRpcProxy::NProto::TRspReadTableMeta meta;
    meta.set_start_row_index(tableReader->GetStartRowIndex());
    ToProto(meta.mutable_omitted_inaccessible_columns(), tableReader->GetOmittedInaccessibleColumns());
    ToProto(meta.mutable_schema(), tableReader->GetTableSchema());
    meta.mutable_statistics()->set_total_row_count(tableReader->GetTotalRowCount());
    ToProto(meta.mutable_statistics()->mutable_data_statistics(), tableReader->GetDataStatistics());

    auto metaRef = SerializeProtoToRef(meta);
    WaitFor(outputStream->Write(metaRef))
        .ThrowOnError();

    bool finished = false;
    TDuration encodeTime;

    auto makeTimingStatistics = [&] {
        auto timingStatistics = tableReader->GetTimingStatistics();
        auto streamStatistics = context->GetResponseAttachmentsStreamStatistics();
        YT_VERIFY(streamStatistics);
        return NApi::TRemoteTableReaderTimingStatistics{
            .MasterFetchTime = timingStatistics.MasterFetchTime,
            .DataReadTiming = timingStatistics.DataReadTiming,
            .TotalTime = timingStatistics.TotalTime,
            .EncodeTime = encodeTime,
            .WriteStallTime = streamStatistics->WriteStallTime,
            .WindowDrainedTime = streamStatistics->WindowDrainedTime,
        };
    };

    const auto& config = Config_.Acquire();

    HandleInputStreamingRequest(
        context,
        [&] {
            if (finished) {
                return TSharedRef();
            }

            TRowBatchReadOptions options{
                .MaxRowsPerRead = config->ReadBufferRowCount,
                .MaxDataWeightPerRead = config->ReadBufferDataWeight,
                .Columnar = IsColumnarRowsetFormat(request->desired_rowset_format())
            };
            auto batch = ReadRowBatch(tableReader, options);
            if (!batch) {
                finished = true;
            }

            NApi::NRpcProxy::NProto::TRowsetStatistics statistics;
            statistics.set_total_row_count(tableReader->GetTotalRowCount());
            ToProto(statistics.mutable_data_statistics(), tableReader->GetDataStatistics());
            ToProto(statistics.mutable_timing_statistics(), makeTimingStatistics());

            NProfiling::TValueIncrementingTimingGuard<NProfiling::TWallTimer> encodeTimingGuard(&encodeTime);
            return encoder->Encode(
                batch ? batch : CreateEmptyUnversionedRowBatch(),
                &statistics);
        },
        [&] {
            context->AnnotateResponse()
                .With("TimingStatistics", makeTimingStatistics());
        });
}

void TApiService::WriteTableImpl(
    const auto& context,
    const auto& request,
    ITableWriterPtr tableWriter,
    const auto& finalizer)
{
    auto format = GetFormat(context, request);

    THashMap<NApi::NRpcProxy::NProto::ERowsetFormat, IRowStreamDecoderPtr> parserMap;
    auto getOrCreateDecoder = [&] (NApi::NRpcProxy::NProto::ERowsetFormat rowsetFormat) {
        auto it =  parserMap.find(rowsetFormat);
        if (it == parserMap.end()) {
            auto parser = CreateRowStreamDecoder(
                rowsetFormat,
                tableWriter->GetSchema(),
                tableWriter->GetNameTable(),
                format);
            it = parserMap.emplace(rowsetFormat, std::move(parser)).first;
        }
        return it->second;
    };

    auto outputStream = context->GetResponseAttachmentsStream();

    NApi::NRpcProxy::NProto::TWriteTableMeta meta;
    ToProto(meta.mutable_schema(), tableWriter->GetSchema());
    auto metaRef = SerializeProtoToRef(meta);
    WaitFor(outputStream->Write(metaRef))
        .ThrowOnError();

    HandleOutputStreamingRequest(
        context,
        [&] (const TSharedRef& block) {
            NApi::NRpcProxy::NProto::TRowsetDescriptor descriptor;
            auto payloadRef = DeserializeRowStreamBlockEnvelope(block, &descriptor, nullptr);

            ValidateRowsetDescriptor(
                descriptor,
                NApi::NRpcProxy::CurrentWireFormatVersion,
                NApi::NRpcProxy::NProto::RK_UNVERSIONED,
                descriptor.rowset_format());

            auto decoder = getOrCreateDecoder(descriptor.rowset_format());

            auto batch = decoder->Decode(payloadRef, descriptor);

            auto rows = batch->MaterializeRows();

            tableWriter->Write(rows);

            WaitFor(tableWriter->GetReadyEvent())
                .ThrowOnError();
        },
        [&] {
            WaitFor(tableWriter->Close())
                .ThrowOnError();
            finalizer();
        },
        false /*feedbackEnabled*/);
}

void TApiService::PatchTableWriterOptions(TNonNullPtr<NApi::TTableWriterOptions> options)
{
    if (ApiServiceConfig_->EnableLargeColumnarStatistics) {
        options->Config->EnableLargeColumnarStatistics = true;
    }

    // NB: Input comes directly from user and thus requires additional validation.
    options->ValidateAnyIsValidYson = true;
}

DEFINE_RPC_SERVICE_METHOD(TApiService, WriteTable)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    PutMethodInfoInTraceContext("write_table");

    auto path = FromProto<TRichYPath>(request->path());
    context->AnnotateRequest().With(MakeWriteTableRequestTags(path));

    NApi::TTableWriterOptions options;
    std::string tableWriterConfig("{}");
    if (request->has_config()) {
        tableWriterConfig = request->config();
    }

    options.Config = ConvertTo<TTableWriterConfigPtr>(TYsonString(tableWriterConfig));

    PatchTableWriterOptions(&options);

    if (request->has_transactional_options()) {
        FromProto(&options, request->transactional_options());
    }

    auto tableWriter = WaitFor(client->CreateTableWriter(path, options))
        .ValueOrThrow();

    WriteTableImpl(
        context,
        request,
        std::move(tableWriter),
        [] {});
}

DEFINE_RPC_SERVICE_METHOD(TApiService, GetColumnarStatistics)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    std::vector<NYPath::TRichYPath> paths;
    for (const auto& protoSubPath : request->paths()) {
        paths.emplace_back(ConvertTo<NYPath::TRichYPath>(TYsonString(protoSubPath)));
    }

    TGetColumnarStatisticsOptions options;
    SetTimeoutOptions(&options, context.Get());

    options.FetchChunkSpecConfig = New<TFetchChunkSpecConfig>();
    if (request->has_fetch_chunk_spec_config()) {
        FromProto(options.FetchChunkSpecConfig, request->fetch_chunk_spec_config());
    }

    options.FetcherConfig = New<TFetcherConfig>();
    if (request->has_fetcher_config()) {
        FromProto(options.FetcherConfig, request->fetcher_config());
    }

    options.FetcherMode = FromProto<NTableClient::EColumnarStatisticsFetcherMode>(request->fetcher_mode());

    options.EnableEarlyFinish = request->enable_early_finish();

    options.EnableReadSizeEstimation = request->enable_read_size_estimation();

    if (request->has_transactional_options()) {
        FromProto(&options, request->transactional_options());
    }

    context->AnnotateRequest()
        .With("Paths", paths);

    ExecuteCall(
        context,
        [=] {
            return client->GetColumnarStatistics(paths, options);
        },
        [] (const auto& context, const auto& result) {
            auto* response = &context->Response();
            ToProto(response->mutable_statistics(), result);

            context->AnnotateResponse()
                .With("StatisticsCount", result.size());
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, PartitionTables)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    std::vector<TRichYPath> paths;
    for (const auto& path : request->paths()) {
        paths.emplace_back(NYPath::TRichYPath::Parse(path));
    }

    TPartitionTablesOptions options;
    SetTimeoutOptions(&options, context.Get());

    options.FetchChunkSpecConfig = New<TFetchChunkSpecConfig>();
    if (request->has_fetch_chunk_spec_config()) {
        FromProto(options.FetchChunkSpecConfig, request->fetch_chunk_spec_config());
    }

    options.FetcherConfig = New<TFetcherConfig>();
    if (request->has_fetcher_config()) {
        FromProto(options.FetcherConfig, request->fetcher_config());
    }

    options.ChunkSliceFetcherConfig = New<TChunkSliceFetcherConfig>();
    if (request->has_chunk_slice_fetcher_config() && request->chunk_slice_fetcher_config().has_max_slices_per_fetch()) {
        options.ChunkSliceFetcherConfig->MaxSlicesPerFetch = request->chunk_slice_fetcher_config().max_slices_per_fetch();
    }

    options.PartitionMode = FromProto<NTableClient::ETablePartitionMode>(request->partition_mode());

    if (request->has_data_weight_per_partition()) {
        options.DataWeightPerPartition = request->data_weight_per_partition();
    }

    if (request->has_compressed_data_size_per_partition()) {
        options.CompressedDataSizePerPartition = request->compressed_data_size_per_partition();
    }

    if (request->has_max_partition_count()) {
        options.MaxPartitionCount = request->max_partition_count();
    }

    options.AdjustDataWeightPerPartition = request->adjust_data_weight_per_partition();

    options.EnableKeyGuarantee = request->enable_key_guarantee();
    options.EnableCookies = request->enable_cookies();
    options.FetchCookieNodeDescriptors = request->fetch_cookie_node_descriptors();
    options.OmitInaccessibleRows = request->omit_inaccessible_rows();

    if (request->has_transactional_options()) {
        FromProto(&options, request->transactional_options());
    }

    context->AnnotateRequest().With(MakePartitionTablesRequestTags(paths, *request));

    ExecuteCall(
        context,
        [=] {
            return client->PartitionTables(paths, options);
        },
        [] (const auto& context, const auto& result) {
            auto* response = &context->Response();
            ToProto(response->mutable_partitions(), result.Partitions);

            context->AnnotateResponse()
                .With("PartitionCount", result.Partitions.size());
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, ReadTablePartition)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    TTablePartitionCookiePtr cookie;
    std::optional<NYson::TYsonStringBuf> rawFormat;
    NApi::NRpcProxy::NProto::ERowsetFormat desiredRowsetFormat;
    NApi::NRpcProxy::NProto::ERowsetFormat arrowFallbackRowsetFormat;

    NApi::TReadTablePartitionOptions options;

    ParseRequest(&cookie, &rawFormat, &desiredRowsetFormat, &arrowFallbackRowsetFormat, &options, *request);

    YT_VERIFY(cookie);

    auto signatureOk = WaitFor(ValidateSignature(cookie.Underlying()))
        .ValueOrThrow();
    if (!signatureOk) {
        THROW_ERROR_EXCEPTION("Signature validation failed");
    }

    std::optional<NFormats::TFormat> format;
    if (rawFormat) {
        ValidateFormat(context->GetAuthenticationIdentity().User, ConvertToNode(*rawFormat));
        format = ConvertTo<NFormats::TFormat>(*rawFormat);
    }

    context->AnnotateRequest().With(MakeReadTablePartitionRequestTags(*request));

    PutMethodInfoInTraceContext("read_table_partition");

    auto reader = WaitFor(client->CreateTablePartitionReader(cookie, options))
        .ValueOrThrow();

    auto controlAttributesConfig = New<NFormats::TControlAttributesConfig>();
    controlAttributesConfig->EnableRowIndex = request->enable_row_index();
    controlAttributesConfig->EnableTableIndex = request->enable_table_index();
    controlAttributesConfig->EnableRangeIndex = request->enable_range_index();

    auto schemas = GetTableSchemas(reader);
    auto columnFilters = GetColumnFilters(reader);

    if (schemas.size() > 1 || columnFilters.size() > 1) {
        THROW_ERROR_EXCEPTION("Reading multiple table partitions is not supported yet");
    }

    auto encoder = CreateRowStreamEncoder(
        desiredRowsetFormat,
        arrowFallbackRowsetFormat,
        schemas[0],
        columnFilters[0],
        reader->GetNameTable(),
        controlAttributesConfig,
        std::move(format));

    auto outputStream = context->GetResponseAttachmentsStream();

    // For now we don't have any metadata, but we can have it in the future.
    NApi::NRpcProxy::NProto::TRspReadTablePartitionMeta meta;

    auto metaRef = SerializeProtoToRef(meta);
    WaitFor(outputStream->Write(metaRef))
        .ThrowOnError();

    bool finished = false;
    const auto& config = Config_.Acquire();

    HandleInputStreamingRequest(
        context,
        [&] {
            if (finished) {
                return TSharedRef();
            }

            TRowBatchReadOptions options{
                .MaxRowsPerRead = config->ReadBufferRowCount,
                .MaxDataWeightPerRead = config->ReadBufferDataWeight,
                .Columnar = IsColumnarRowsetFormat(request->desired_rowset_format())
            };
            auto batch = ReadRowBatch(reader, options);
            if (!batch) {
                finished = true;
            }

            return encoder->Encode(
                batch ? batch : CreateEmptyUnversionedRowBatch(),
                /*statistics*/ nullptr);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, WriteTableFragment)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    PutMethodInfoInTraceContext("write_table_fragment");

    TSignedWriteFragmentCookiePtr cookie;

    TTableFragmentWriterOptions options;
    ParseRequest(&cookie, &options, *request);

    PatchTableWriterOptions(&options);

    auto concreteCookie = ConvertTo<TWriteFragmentCookie>(TYsonStringBuf(cookie.Underlying()->Payload()));

    context->AnnotateRequest().With(MakeWriteTableFragmentRequestTags(concreteCookie.PatchInfo.ObjectId, concreteCookie.MainTransactionId));

    auto isValid = WaitFor(ValidateSignature(cookie.Underlying()))
        .ValueOrThrow();

    if (!isValid) {
        THROW_ERROR_EXCEPTION(
            "Signature validation failed for write table fragment")
                .With("session_id", concreteCookie.SessionId)
                .With("cookie_id", concreteCookie.CookieId);
    }

    auto tableWriter = WaitFor(client->CreateTableFragmentWriter(cookie, options))
        .ValueOrThrow();

    WriteTableImpl(
        context,
        request,
        tableWriter,
        [&, tableWriter] {
            auto writeResult = tableWriter->GetWriteFragmentResult();
            response->set_signed_write_result(ToProto(ConvertToYsonString(writeResult)));
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
