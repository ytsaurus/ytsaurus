#include "source.h"

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <yt/yt/flow/library/cpp/computation/universal_controller.h>

#include <yt/yt/flow/library/cpp/connectors/queue/queue_info.h>
#include <yt/yt/flow/library/cpp/connectors/queue/source.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/yt/misc/static_initializer.h>

#include <mutex>

namespace NYT::NFlow {

using namespace NTableClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TSourceRegistry
{
    std::mutex Mutex;
    THashMap<std::string, TStaticTableExportSourceFactory> Factories;
};

TSourceRegistry* GetSourceRegistry()
{
    return LeakySingleton<TSourceRegistry>();
}

} // namespace

void RegisterStaticTableExportSource(const std::string& sourceClassName, TStaticTableExportSourceFactory factory)
{
    auto* registry = GetSourceRegistry();
    auto guard = std::lock_guard(registry->Mutex);
    EmplaceOrCrash(registry->Factories, sourceClassName, std::move(factory));
}

const TStaticTableExportSourceFactory& GetStaticTableExportSource(const TComputationSpec& spec)
{
    THROW_ERROR_EXCEPTION_UNLESS(spec.SourceStreams.size() == 1, "Static table export requires one source");
    const auto& sourceClassName = spec.SourceStreams.begin()->second->SourceClassName;
    auto* registry = GetSourceRegistry();
    auto guard = std::lock_guard(registry->Mutex);
    const auto* factory = registry->Factories.FindPtr(sourceClassName);
    THROW_ERROR_EXCEPTION_UNLESS(factory, "Static table export does not support source %Qv", sourceClassName);
    return *factory;
}

////////////////////////////////////////////////////////////////////////////////

namespace {

//! The offset of a queue row; the stream keeps it from the queue rowset.
constexpr TStringBuf RowIndexColumn = "$row_index";

TQueueSourceParametersPtr GetQueueSource(const TComputationSpec& spec)
{
    return DynamicPointerCast<TQueueSourceParameters>(TRegistry::Get()->ParseSourceParameters(spec.SourceStreams.begin()->second));
}

std::string GetQueueIdentity(const TQueueSourceParameters& source)
{
    return MakeSourceIdentity({*source.QueuePath.GetCluster(), TStringBuf(source.QueuePath.GetPath())});
}

TStaticTableExportSourceInfo InspectQueueSource(const TComputationSpec& spec)
{
    const auto source = GetQueueSource(spec);
    THROW_ERROR_EXCEPTION_UNLESS(!source->TryParseFlowQueueMeta && !source->PartitionFilter && source->WriteTimeColumn && source->QueuePath.GetCluster(),
        "Queue static table export requires \"write_time_column\", every partition, a queue cluster and no Flow metadata parsing");
    // clang-format off
    return {
        .WriteTimeColumn = *source->WriteTimeColumn,
        .Owner = BuildYsonNodeFluently()
            .BeginMap()
                .Item("queue_cluster").Value(*source->QueuePath.GetCluster())
                .Item("queue_path").Value(source->QueuePath.GetPath())
            .EndMap()
            ->AsMap(),
    };
    // clang-format on
}

class TQueuePartitionSource
    : public IStaticTableExportPartitionSource
{
public:
    explicit TQueuePartitionSource(const TSinkContextPtr& context)
        : Source_(GetQueueSource(*context->ComputationSpec))
    {
        THROW_ERROR_EXCEPTION_UNLESS(context->Partition->SourceKey, "Queue static table export sink requires a source partition");
        Partition_ = ExtractQueuePartitionIndex(SplitUniversalPartitionKey(*context->Partition->SourceKey).second);
        const auto& schema = context->StreamSpecStorage->GetSchema(*context->SinkSpec->InputStreamIds.begin());
        const auto* rowIndex = schema->FindColumn(RowIndexColumn);
        THROW_ERROR_EXCEPTION_UNLESS(rowIndex && rowIndex->GetWireType() == EValueType::Int64,
            "Queue static table export requires \"$row_index\" int64 on its stream");
    }

    int GetPartition() const override
    {
        return Partition_;
    }

    std::string GetIdentity() const override
    {
        return GetQueueIdentity(*Source_);
    }

    TStaticTableExportMessage GetExportMessage(const TOutputMessage& message) const override
    {
        return {
            .Offset = GetColumnValue<i64>(message, RowIndexColumn),
            .WriteTimestamp = TSystemTimestamp(GetColumnValue<ui64>(message, *Source_->WriteTimeColumn) / 1000),
        };
    }

    bool IsRejected(const TOutputMessage& /*message*/) const override
    {
        return false;
    }

    void ValidateDynamicSpec(const TComputationSpec& /*spec*/, const TDynamicComputationSpec& /*dynamicSpec*/) const override
    { }

private:
    const TQueueSourceParametersPtr Source_;
    int Partition_ = 0;
};

class TQueueControllerSource
    : public IStaticTableExportControllerSource
{
public:
    TQueueControllerSource(const TSinkControllerContextPtr& context, const NLogging::TLogger& logger)
        : Source_(GetQueueSource(*context->ComputationSpec))
        , Info_(New<TQueueInfoController>(
            Source_,
            context->ClientsCache->GetClient(*Source_->QueuePath.GetCluster()),
            context->Invoker,
            logger,
            context->StatusProfiler->WithPrefix("/export_source")))
    { }

    void Init(IInitContextPtr initContext) override
    {
        Info_->Init(initContext->WithPrefix("source"));
    }

    void Sync() override
    {
        Info_->Sync();
    }

    void Commit() override
    {
        Info_->Commit();
    }

    std::optional<TStaticTableExportSourceTopology> GetTopology() const override
    {
        const auto partitionCount = Info_->GetPartitionCount();
        if (!partitionCount) {
            return std::nullopt;
        }
        return TStaticTableExportSourceTopology{.Identity = GetQueueIdentity(*Source_), .PartitionCount = *partitionCount};
    }

    std::optional<int> FindPartition(const TKey& sourceKey, const std::string& identity) const override
    {
        const int partition = ExtractQueuePartitionIndex(sourceKey);
        return sourceKey == GenerateQueueKey(identity, partition) ? std::optional(partition) : std::nullopt;
    }

    std::pair<std::string, INodePtr> BuildMetadata(std::span<const TStaticTableExportRange> ranges) const override
    {
        // clang-format off
        return {"_queue_metadata", BuildYsonNodeFluently()
            .BeginMap()
                .Item("cluster").Value(*Source_->QueuePath.GetCluster())
                .Item("queue_path").Value(Source_->QueuePath.GetPath())
                .Item("partitions").Value(BuildStaticTableExportPartitions(ranges))
            .EndMap()};
        // clang-format on
    }

private:
    const TQueueSourceParametersPtr Source_;
    const TQueueInfoControllerPtr Info_;
};

YT_STATIC_INITIALIZER(RegisterStaticTableExportSource(
    TypeName<TQueueSource>(),
    {
        .Inspect = InspectQueueSource,
        .CreatePartitionSource = [] (const TSinkContextPtr& context) -> IStaticTableExportPartitionSourcePtr {
            return New<TQueuePartitionSource>(context);
        },
        .CreateControllerSource = [] (const TSinkControllerContextPtr& context, const NLogging::TLogger& logger) -> IStaticTableExportControllerSourcePtr {
            return New<TQueueControllerSource>(context, logger);
        },
    }));

} // namespace

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
