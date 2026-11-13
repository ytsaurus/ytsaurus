#include "static_table_export.h"

#include <yt/yt/flow/library/cpp/common/process_function.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/runtime_init_context.h>
#include <yt/yt/flow/library/cpp/common/schema.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <yt/yt/flow/library/cpp/computation/passthrough_computation.h>
#include <yt/yt/flow/library/cpp/computation/universal_controller.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/flow/library/cpp/process_function/host/computation_runtime_context.h>

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/fluent.h>

#include <mutex>

namespace NYT::NFlow {

using namespace NApi;
using namespace NTableClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

//! The stream the process function writes table rows to.
const TStreamId ExportStreamId("static_table_export");

TProcessFunctionStaticTableExportParametersPtr GetExportParameters(const TComputationSpec& spec)
{
    return ConvertTo<TProcessFunctionStaticTableExportParametersPtr>(spec.Sinks.begin()->second->Parameters);
}

TStaticTableExportSourceInfo InspectSource(const TComputationSpec& spec)
{
    THROW_ERROR_EXCEPTION_UNLESS(spec.ComputationClassName == TypeName<TSwiftPassthroughOrderedSourceComputation>(),
        "Static table export requires TSwiftPassthroughOrderedSourceComputation");
    auto info = GetStaticTableExportSource(spec).Inspect(spec);
    ValidateStaticTableExportComputation(spec, GetExportParameters(spec), TypeName<TProcessFunctionStaticTableExportSink>(), info.WriteTimeColumn);
    return info;
}

////////////////////////////////////////////////////////////////////////////////

//! The init context of a process function hosted by the sink, which has no state.
class TStatelessRuntimeInitContext
    : public IRuntimeInitContext
{
public:
    TStatelessRuntimeInitContext(const TSinkContextPtr& context, IMapNodePtr parametersNode, TYsonStructPtr parametersObject, std::string prefix = {})
        : Context_(context)
        , ParametersNode_(std::move(parametersNode))
        , ParametersObject_(std::move(parametersObject))
        , Prefix_(std::move(prefix))
    { }

    IMapNodePtr GetParametersNode() const override
    {
        return ParametersNode_;
    }

    TYsonStructPtr GetParametersObject() const override
    {
        return ParametersObject_;
    }

    IResourcePtr GetStaticResource(const TResourceId& resourceId) const override
    {
        return Context_->GetStaticResource(resourceId);
    }

    NProfiling::TProfiler GetProfiler() const override
    {
        return Context_->Profiler;
    }

    NHttp::IClientPtr GetHttpClient() const override
    {
        THROW_ERROR_EXCEPTION_UNLESS(Context_->HttpClient, "HTTP client is not configured");
        return Context_->HttpClient;
    }

    NHttp::IClientPtr GetHttpsClient() const override
    {
        THROW_ERROR_EXCEPTION_UNLESS(Context_->HttpsClient, "HTTPS client is not configured");
        return Context_->HttpsClient;
    }

    TComputationId GetComputationId() const override
    {
        return Context_->Partition->ComputationId;
    }

    TPartitionId GetPartitionId() const override
    {
        return Context_->Partition->PartitionId;
    }

    TFuture<IMutableStateKeyProviderPtr> CreateMutableStateKeyProvider(std::function<IStateHolderPtr()> /*ctor*/) const override
    {
        ThrowNoState();
    }

    TFuture<IJoinedStateKeyProviderPtr> CreateJoinedStateKeyProvider(std::function<IStateHolderPtr()> /*ctor*/) const override
    {
        ThrowNoState();
    }

    IInitContextPtr AsPartition() const override
    {
        ThrowNoState();
    }

    IInitContextPtr AsKey(TKey /*key*/) const override
    {
        ThrowNoState();
    }

    IRuntimeInitContextPtr WithPrefix(TStringBuf prefix) const override
    {
        return New<TStatelessRuntimeInitContext>(Context_, ParametersNode_, ParametersObject_, Prefix_ + std::string(prefix));
    }

    const std::string& GetPrefix() const override
    {
        return Prefix_;
    }

protected:
    IExternalStateManagerPtr GetExternalStateManagerOrThrow(const std::string& /*name*/) const override
    {
        ThrowNoState();
    }

    IExternalStateJoinerPtr GetExternalStateJoinerOrThrow(const std::string& /*name*/) const override
    {
        ThrowNoState();
    }

private:
    const TSinkContextPtr Context_;
    const IMapNodePtr ParametersNode_;
    const TYsonStructPtr ParametersObject_;
    const std::string Prefix_;

    [[noreturn]] static void ThrowNoState()
    {
        THROW_ERROR_EXCEPTION("A process function of static table export cannot use state");
    }
};

//! The runtime context of a process function hosted by the sink, which runs outside epochs.
class TStatelessRuntimeContext
    : public TComputationRuntimeContext
{
public:
    using TComputationRuntimeContext::TComputationRuntimeContext;

    TSystemTimestamp GetWatermark(const TStreamId& /*streamId*/) const override
    {
        ThrowUnavailable();
    }

    TSystemTimestamp GetInputEventWatermark() const override
    {
        ThrowUnavailable();
    }

    TWatermarkStatePtr GetEpochWatermarkState() const override
    {
        ThrowUnavailable();
    }

    TSystemTimestamp GetCurrentTimestamp() const override
    {
        return TSystemTimestamp(TInstant::Now().Seconds());
    }

    TUniqueSeqNo GetEpochUniqueSeqNo() const override
    {
        ThrowUnavailable();
    }

    NConcurrency::IThroughputThrottlerPtr GetThrottlerOrThrow(const TThrottlerId& /*throttlerId*/) override
    {
        ThrowUnavailable();
    }

    NConcurrency::IThroughputThrottlerPtr TryGetThrottler(const TThrottlerId& /*throttlerId*/) override
    {
        ThrowUnavailable();
    }

private:
    [[noreturn]] static void ThrowUnavailable()
    {
        THROW_ERROR_EXCEPTION("A process function of static table export runs outside epochs");
    }
};

class TRowCollector
    : public IOutputCollector
{
public:
    std::vector<TMessage> Messages;

    IOutputCollectorPtr SetParents(
        const std::vector<TInputMessageConstPtr>& /*messages*/,
        const std::vector<TInputTimerConstPtr>& /*timers*/,
        const std::vector<TInputVisitConstPtr>& /*visits*/) override
    {
        return this;
    }

    void AddTimer(TSystemTimestamp /*triggerTimestamp*/, std::optional<TSystemTimestamp> /*eventTimestamp*/) override
    {
        ThrowNoTimers();
    }

    void AddTimer(const TStreamId& /*streamId*/, TSystemTimestamp /*triggerTimestamp*/, std::optional<TSystemTimestamp> /*eventTimestamp*/) override
    {
        ThrowNoTimers();
    }

    void AddTimer(TTimer&& /*timer*/) override
    {
        ThrowNoTimers();
    }

private:
    void DoAddMessage(TMessage&& message, TAddMessageOptions /*options*/) override
    {
        Messages.push_back(std::move(message));
    }

    [[noreturn]] static void ThrowNoTimers()
    {
        THROW_ERROR_EXCEPTION("A process function of static table export cannot add timers");
    }
};

//! Turns source messages into table rows with the process function.
//! A function instance is not thread-safe, so each concurrent parse task borrows its own.
class TFunctionPool
{
public:
    TFunctionPool(const TSinkContextPtr& context, const TProcessFunctionStaticTableExportParametersPtr& parameters)
        : Context_(context)
        , Schema_(parameters->Schema)
        , Spec_(CloneYsonStruct(context->ComputationSpec))
    {
        Spec_->ProcessingFunction = *parameters->ProcessingFunction;
        Spec_->ProcessingFunctionParameters = parameters->ProcessingFunctionParameters
            ? parameters->ProcessingFunctionParameters
            : GetEphemeralNodeFactory()->CreateMap();
        Spec_->OutputStreamIds = {ExportStreamId};
        auto streamSpec = New<TStreamSpec>();
        streamSpec->Schema = Schema_;
        StreamSpecs_ = New<TComputationStreamSpecStorage>(
            New<TStreamSpecs>(THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>>{{ExportStreamId, {{TStreamSpecId(0), streamSpec}}}}),
            Spec_->GroupBySchema,
            context->ConverterCache);
        ParametersObject_ = TRegistry::Get()->ParseProcessFunctionParameters(Spec_);
        // Creating one instance up front reports an unusable function at the job start.
        Idle_.push_back(CreateInstance());
    }

    TStaticTableExportParsedRows Parse(const TOutputMessageConstPtr& message, const TNameTablePtr& nameTable)
    {
        std::unique_ptr<TInstance> instance;
        {
            auto guard = std::lock_guard(Mutex_);
            if (!Idle_.empty()) {
                instance = std::move(Idle_.back());
                Idle_.pop_back();
            }
        }
        if (!instance) {
            instance = CreateInstance();
        }
        auto rows = DoParse(*instance, message, nameTable);
        auto guard = std::lock_guard(Mutex_);
        Idle_.push_back(std::move(instance));
        return rows;
    }

private:
    struct TInstance
    {
        IProcessFunctionPtr Function;
        TIntrusivePtr<TStatelessRuntimeContext> RuntimeContext;
        TIntrusivePtr<TRowCollector> Collector;
    };

    const TSinkContextPtr Context_;
    const TTableSchemaPtr Schema_;
    const TComputationSpecPtr Spec_;
    TComputationStreamSpecStoragePtr StreamSpecs_;
    TYsonStructPtr ParametersObject_;
    std::mutex Mutex_;
    std::vector<std::unique_ptr<TInstance>> Idle_;

    std::unique_ptr<TInstance> CreateInstance() const
    {
        auto context = New<TProcessFunctionContext>();
        context->ComputationSpec = Spec_;
        context->InitContext = New<TStatelessRuntimeInitContext>(Context_, Spec_->ProcessingFunctionParameters, ParametersObject_);
        context->ClientsCache = Context_->ClientsCache;
        context->Invoker = Context_->PoolInvoker;
        context->Logger = Context_->Logger.WithTag("ProcessingFunction", *Spec_->ProcessingFunction);
        context->StatusProfiler = Context_->StatusProfiler->WithPrefix("/process_function");
        auto function = DynamicPointerCast<IProcessFunction>(TRegistry::Get()->CreateProcessFunction(*Spec_->ProcessingFunction, context));
        THROW_ERROR_EXCEPTION_UNLESS(function, "Static table export requires a per-message process function")
            .With("processing_function", *Spec_->ProcessingFunction);
        function->Init(context->InitContext);
        auto runtimeContext = New<TStatelessRuntimeContext>(
            Spec_,
            StreamSpecs_,
            Spec_->GroupBySchema,
            Context_->ConverterCache,
            /*throttlerFactory*/ nullptr,
            ParametersObject_);
        runtimeContext->RefreshEpochState(/*watermarkState*/ nullptr, GetEphemeralNodeFactory()->CreateMap());
        return std::make_unique<TInstance>(TInstance{
            .Function = std::move(function),
            .RuntimeContext = std::move(runtimeContext),
            .Collector = New<TRowCollector>(),
        });
    }

    TStaticTableExportParsedRows DoParse(TInstance& instance, const TOutputMessageConstPtr& message, const TNameTablePtr& nameTable) const
    {
        auto input = New<TInputMessage>(TMessage(*message), TKey());
        instance.Collector->Messages.clear();
        instance.Function->ProcessMessage(input, instance.Collector, instance.RuntimeContext);
        auto messages = std::exchange(instance.Collector->Messages, {});

        std::vector<int> ids;
        ids.reserve(Schema_->GetColumnCount());
        for (const auto& column : Schema_->Columns()) {
            ids.push_back(nameTable->GetIdOrRegisterName(column.Name()));
        }
        // The values point into the payloads, so the buffer holds only the value arrays.
        auto rowBuffer = New<TRowBuffer>(
            TDefaultRowBufferPoolTag(),
            std::max<size_t>(messages.size() * GetUnversionedRowByteSize(Schema_->GetColumnCount()), 1));
        std::vector<TUnversionedRow> rows;
        rows.reserve(messages.size());
        i64 memoryUsage = 0;
        for (auto& output : messages) {
            if (output.PayloadSchema != Schema_ && *output.PayloadSchema != *Schema_) {
                output = ConvertMessageToNewSchema(output, Schema_, Context_->ConverterCache);
            }
            const auto payload = output.Payload.Underlying().Get();
            memoryUsage += output.Payload.Underlying().GetSpaceUsed();
            auto row = rowBuffer->AllocateUnversioned(payload.GetCount());
            for (int index = 0; index < static_cast<int>(payload.GetCount()); ++index) {
                row[index] = payload[index];
                row[index].Id = ids[index];
            }
            rows.push_back(row);
        }
        memoryUsage += rowBuffer->GetCapacity();
        return {
            .Rows = MakeSharedRange(std::move(rows), std::move(rowBuffer), std::move(messages)),
            .MemoryUsage = memoryUsage,
        };
    }
};

//! Takes the columns of the table schema from a source row by name.
TStaticTableExportParsedRows ProjectRow(const TTableSchemaPtr& schema, const TOutputMessageConstPtr& message, const TNameTablePtr& nameTable)
{
    std::vector<TUnversionedValue> values;
    values.reserve(schema->GetColumnCount());
    for (const auto& column : schema->Columns()) {
        auto& value = values.emplace_back(GetColumn(*message, column.Name()));
        value.Id = nameTable->GetIdOrRegisterName(column.Name());
    }
    size_t stringDataSize = 0;
    for (const auto& value : values) {
        if (IsStringLikeType(value.Type)) {
            stringDataSize += value.Length;
        }
    }
    auto rowBuffer = New<TRowBuffer>(TDefaultRowBufferPoolTag(), GetUnversionedRowByteSize(values.size()) + stringDataSize);
    std::vector<TUnversionedRow> rows{rowBuffer->CaptureRow(TUnversionedValueRange(values.data(), values.size()))};
    const i64 memoryUsage = rowBuffer->GetCapacity();
    return {
        .Rows = MakeSharedRange(std::move(rows), std::move(rowBuffer)),
        .MemoryUsage = memoryUsage,
    };
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TProcessFunctionStaticTableExportParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("schema", &TThis::Schema);
    registrar.Parameter("processing_function", &TThis::ProcessingFunction)
        .Optional();
    registrar.Parameter("processing_function_parameters", &TThis::ProcessingFunctionParameters)
        .Default();

    registrar.Postprocessor([] (TThis* parameters) {
        THROW_ERROR_EXCEPTION_IF(parameters->Schema->IsSorted(), "Static table export \"schema\" must not be sorted");
        for (const auto& column : parameters->Schema->Columns()) {
            THROW_ERROR_EXCEPTION_IF(column.Name().starts_with('$'),
                "Static table export \"schema\" must not contain system columns")
                .With("column", column.Name());
        }
        if (parameters->ProcessingFunction) {
            TRegistry::Get()->ValidateStaticProcessFunctionParameters(
                *parameters->ProcessingFunction,
                parameters->ProcessingFunctionParameters ? parameters->ProcessingFunctionParameters : GetEphemeralNodeFactory()->CreateMap());
        } else {
            THROW_ERROR_EXCEPTION_IF(parameters->ProcessingFunctionParameters,
                "Static table export \"processing_function_parameters\" require \"processing_function\"");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

TProcessFunctionStaticTableExportSink::TProcessFunctionStaticTableExportSink(TSinkContextPtr context, TDynamicSinkContextPtr dynamicContext)
    : TProcessFunctionStaticTableExportSink(context, std::move(dynamicContext), GetStaticTableExportSource(*context->ComputationSpec).CreatePartitionSource(context))
{ }

TProcessFunctionStaticTableExportSink::TProcessFunctionStaticTableExportSink(
    TSinkContextPtr context,
    TDynamicSinkContextPtr dynamicContext,
    IStaticTableExportPartitionSourcePtr source)
    : TStaticTableExportSinkBase(std::move(context), std::move(dynamicContext), source->GetPartition())
    , SourceInfo_(InspectSource(*GetContext()->ComputationSpec))
    , Source_(std::move(source))
{
    const auto& schema = GetContext()->StreamSpecStorage->GetSchema(*GetSpec()->InputStreamIds.begin());
    const auto* writeTime = schema->FindColumn(SourceInfo_.WriteTimeColumn);
    THROW_ERROR_EXCEPTION_UNLESS(writeTime && writeTime->GetWireType() == EValueType::Uint64,
        "Static table export requires \"write_time_column\" uint64 on its stream");
    if (!GetParameters()->ProcessingFunction) {
        for (const auto& column : GetParameters()->Schema->Columns()) {
            const auto* streamColumn = schema->FindColumn(column.Name());
            THROW_ERROR_EXCEPTION_UNLESS(streamColumn && *streamColumn->LogicalType() == *column.LogicalType(),
                "Static table export \"schema\" column must be on the stream with the same type unless a process function makes the rows")
                .With("column", column.Name());
        }
    }
}

void TProcessFunctionStaticTableExportSink::ValidateDynamicSpec() const
{
    const auto& dynamicSpec = *GetDynamicContext()->DynamicComputationSpec;
    // A skipped row would leave a gap that no published range can describe.
    THROW_ERROR_EXCEPTION_IF(dynamicSpec.SkipIfExpression, "Static table export forbids row filtering");
    Source_->ValidateDynamicSpec(*GetContext()->ComputationSpec, dynamicSpec);
}

TStaticTableExportBinding TProcessFunctionStaticTableExportSink::MakeBinding()
{
    const auto& parameters = GetParameters();
    TStaticTableExportRowParser parseRows;
    if (parameters->ProcessingFunction) {
        parseRows = [source = Source_, pool = std::make_shared<TFunctionPool>(GetContext(), parameters)] (
            const TOutputMessageConstPtr& message,
            const TNameTablePtr& nameTable,
            int /*partition*/) {
            if (source->IsRejected(*message)) {
                return TStaticTableExportParsedRows{.RejectedRecords = 1};
            }
            return pool->Parse(message, nameTable);
        };
    } else {
        parseRows = [source = Source_, schema = parameters->Schema] (const TOutputMessageConstPtr& message, const TNameTablePtr& nameTable, int /*partition*/) {
            if (source->IsRejected(*message)) {
                return TStaticTableExportParsedRows{.RejectedRecords = 1};
            }
            return ProjectRow(schema, message, nameTable);
        };
    }
    return {
        .Schema = parameters->Schema,
        .Owner = MakeStaticTableExportOwner(GetContext()->PipelinePath.GetPath(), GetContext()->Partition->ComputationId, parameters, SourceInfo_.Owner),
        .SourceIdentity = Source_->GetIdentity(),
        .ParseRows = std::move(parseRows),
    };
}

TStaticTableExportMessage TProcessFunctionStaticTableExportSink::GetExportMessage(const TOutputMessage& message) const
{
    return Source_->GetExportMessage(message);
}

////////////////////////////////////////////////////////////////////////////////

TProcessFunctionStaticTableExportSinkController::TProcessFunctionStaticTableExportSinkController(
    TSinkControllerContextPtr context,
    TDynamicSinkControllerContextPtr dynamicContext)
    : TStaticTableExportSinkControllerBase(std::move(context), std::move(dynamicContext))
    , SourceInfo_(InspectSource(*GetContext()->ComputationSpec))
    , Source_(GetStaticTableExportSource(*GetContext()->ComputationSpec).CreateControllerSource(GetContext(), Logger))
{ }

void TProcessFunctionStaticTableExportSinkController::Init(IInitContextPtr initContext)
{
    Source_->Init(std::move(initContext));
}

void TProcessFunctionStaticTableExportSinkController::Sync()
{
    Source_->Sync();
}

void TProcessFunctionStaticTableExportSinkController::Commit()
{
    Source_->Commit();
    if (const auto topology = Source_->GetTopology()) {
        UpdatePublication({.Identity = topology->Identity, .PartitionCount = topology->PartitionCount});
    }
}

TTableSchemaPtr TProcessFunctionStaticTableExportSinkController::GetExportSchema() const
{
    return GetParameters()->Schema;
}

INodePtr TProcessFunctionStaticTableExportSinkController::MakeOwner() const
{
    return MakeStaticTableExportOwner(GetContext()->PipelinePath.GetPath(), GetContext()->ComputationId, GetParameters(), SourceInfo_.Owner);
}

std::optional<int> TProcessFunctionStaticTableExportSinkController::FindPartition(const TKey& partitionKey, const std::string& identity) const
{
    return Source_->FindPartition(SplitUniversalPartitionKey(partitionKey).second, identity);
}

IAttributeDictionaryPtr TProcessFunctionStaticTableExportSinkController::BuildPublicationAttributes(std::span<const TStaticTableExportRange> ranges) const
{
    auto attributes = CreateEphemeralAttributes();
    auto [name, value] = Source_->BuildMetadata(ranges);
    attributes->Set(name, value);
    return attributes;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
