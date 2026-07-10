#pragma once

#include "public.h"
#include "spec.h"

#include <yt/yt/flow/library/cpp/connectors/common/ordered_source_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/source_controller_base.h>

#include <yt/yt/flow/library/cpp/common/message_batcher.h>
#include <yt/yt/flow/library/cpp/common/registry.h>

#include <yt/yt/flow/library/cpp/resources/yt_client_factory.h>

#include <yt/yt/client/api/table_reader.h>
#include <yt/yt/client/ypath/rich.h>
#include <yt/yt/core/ypath/public.h>

#include <yt/yt/client/complex_types/yson_format_conversion.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/queue_client/public.h>

namespace NYT::NFlow::NStaticTableConnectorV2 {

////////////////////////////////////////////////////////////////////////////////

struct TPartitionStatus
    : public virtual NYTree::TYsonStruct
{
    i64 CommittedOffsetExclusive{};

    REGISTER_YSON_STRUCT(TPartitionStatus);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPartitionStatus);

////////////////////////////////////////////////////////////////////////////////

class TSource
    : public TIntegerOffsetOrderedSourceBase
{
public:
    using TSourceController = TSourceController;

    YT_FLOW_EXTEND_PARAMETERS(TTableSourceParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicTableSourceParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARTITION_SPEC(TDynamicTableSourcePartitionSpec);

public:
    TSource(
        TSourceContextPtr context,
        TDynamicSourceContextPtr dynamicContext);

    // Creates the converter used for V1 `any` columns. Such columns may carry any wire type
    // depending on how the table was written: String cells contain pre-serialized raw YSON bytes
    // (for example, written via the Python unstructured writer) and are emitted via OnRaw; other
    // native types are serialized with UnversionedValueToYson.
    static NComplexTypes::TYsonServerToClientConverter MakeAnyColumnConverter();

    // Single pass over nameTable × tableSchema.
    // Fills wireTypes[id] for each schema-declared column; leaves EValueType::Min for others.
    // Returns anyConverters: a converter for every schema-declared Any column (V1 any columns get
    // MakeAnyColumnConverter(); V3 complex types get CreateYsonServerToClientConverter).
    // Empty when |tableSchema| is null (weak schema).
    static THashMap<int, NComplexTypes::TYsonServerToClientConverter> InferTableSchema(
        const NTableClient::TNameTablePtr& nameTable,
        const NTableClient::TTableSchemaPtr& tableSchema,
        std::vector<NTableClient::EValueType>& wireTypes);

    // mappingIndex[nameTableIdx] == returnedSchemaColumnNumber.
    // |wireTypes|: pre-built by InferTableSchema; wireTypes[id] == EValueType::Min means "not in schema".
    // |anyConverters|: pre-built by InferTableSchema; cells for columns present in it are normalized
    //   to Any regardless of their physical wire type (Null cells are preserved as-is).
    static std::pair<NTableClient::TTableSchemaPtr, std::vector<int>> GetSchemaAndMappingIndex(
        const TSharedRange<NTableClient::TUnversionedRow>& unversionedRowRange,
        const NTableClient::TNameTablePtr& nameTable,
        const std::vector<NTableClient::EValueType>& wireTypes,
        const THashMap<int, NComplexTypes::TYsonServerToClientConverter>& anyConverters);

    // Converts a cell to an Any cell using the provided converter.
    // If |converter| is non-null: calls the |converter| to produce named-format YSON in |ysonBuffer|.
    //   V3 complex types use CreateYsonServerToClientConverter (positional->named).
    //   V1 any columns use MakeAnyColumnConverter().
    // If |converter| is null (no schema info, e.g. weak schema): retypes to Any keeping bytes as-is.
    // |ysonBuffer| must remain alive as long as the returned value is in use.
    static NTableClient::TUnversionedValue ConvertCellToAny(
        const NTableClient::TUnversionedValue& cell,
        const NComplexTypes::TYsonServerToClientConverter* converter,
        TString& ysonBuffer);

    // Creates a throttler configuration that guarantees forward progress.
    static NConcurrency::TThroughputThrottlerConfigPtr CreateThrottlerConfig(
        double rowsPerSecond,
        TDuration throttlerPeriod);

private:
    void DoInit() final;

    TFuture<std::vector<TRecord>> DoReadNextBatch(
        const TMessageBatcherSettingsPtr& settings,
        TOffset nextOffset,
        std::optional<TOffset> offsetLimit) final;

    void DoReportPersistedOffset(TOffset offsetExclusive) final;

    NYTree::IMapNodePtr GetPartitionStatus() final;

    bool IsFinite() final;

protected:
    const NLogging::TLogger Logger;

private:
    NApi::IClientPtr Client_;
    NConcurrency::IReconfigurableThroughputThrottlerPtr Throttler_;

    std::atomic<i64> PersistedOffsetExclusive_;
    TFuture<NApi::ITableReaderPtr> ReaderFuture_;
    i64 CurrentOffset_ = 0;

    TInstant LastNonEmptyBatchRead_ = TInstant::Zero();
};

DEFINE_REFCOUNTED_TYPE(TSource);

////////////////////////////////////////////////////////////////////////////////

struct TSourceControllerTable
    : public virtual NYTree::TYsonStruct
{
    i64 Era{};

    // Path with cluster.
    NYPath::TRichYPath Path;

    i64 RowCount{};
    i64 ByteSize{};
    TSystemTimestamp EventTimestamp;
    TSystemTimestamp SystemTimestamp;

    // Position of this table's directory name within its (Era, EventTimestamp) group.
    i64 EventOrdinal{};

    i64 DistributedRows{};

    THashMap<TRangeId, std::pair<i64, i64>> DistributingRanges; // rangeId -> (rangeBegin, rangeEnd).

    i64 GetNotDistributedRows() const;

    std::string GetName() const;

    std::tuple<i64, TSystemTimestamp, i64> GetOrderingKey() const; // Era, EventTimestamp, EventOrdinal

    void SkipRemainingRows();

    REGISTER_YSON_STRUCT(TSourceControllerTable);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSourceControllerTable);

struct TClusterProgress
    : public NYTree::TYsonStruct
{
    // Cluster name -> stashed table.
    THashMap<std::string, TSourceControllerTablePtr> ByCluster;

    // Ordering key of the table this progress belongs to.
    i64 Era{};
    TSystemTimestamp EventTimestamp;
    i64 EventOrdinal{};

    REGISTER_YSON_STRUCT(TClusterProgress);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TClusterProgress);

struct TEventNameOrder
    : public NYTree::TYsonStruct
{
    TSystemTimestamp EventTimestamp;
    std::vector<std::string> Names;

    REGISTER_YSON_STRUCT(TEventNameOrder);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TEventNameOrder);

struct TSourceControllerState
    : public NYTree::TYsonStruct
{
    // At least one table discovering was completed.
    bool Inited{};

    // All tables with GetOrderingKey() < DistributingTable->GetOrderingKey() are considered as fully processed.
    TSourceControllerTablePtr DistributingTable;
    bool DistributionFinished{}; // Current table is processed or its processing was interrupted.

    i64 Era{};
    TInstant EraStartInstant;

    // Sizes of tables that are found, but not distributing or processed yet.
    i64 PendingCount{};
    i64 PendingBytes{};

    // For metrics of processed tables.
    i64 ProcessedTables{};
    i64 LostTables{};

    TEventNameOrderPtr EventNameOrder;

    TClusterProgressPtr ClusterProgress;

    // Tables whose ranges were abandoned mid-read
    std::vector<TSourceControllerTablePtr> PendingCleanupTables;

    TInstant ActiveClusterUnavailableSince;

    REGISTER_YSON_STRUCT(TSourceControllerState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSourceControllerState);

////////////////////////////////////////////////////////////////////////////////

struct TListedTables
{
    std::vector<TSourceControllerTablePtr> Tables;
    THashSet<std::string> UnavailableClusters;
};

////////////////////////////////////////////////////////////////////////////////

struct TFailoverDecision
{
    TSourceControllerTablePtr NewTable;
    std::string StashedCluster;
};

////////////////////////////////////////////////////////////////////////////////

class TSourceController
    : public TSourceControllerBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TTableSourceParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicTableSourceParameters);

    TSourceController(
        TSourceControllerContextPtr context,
        TDynamicSourceControllerContextPtr dynamicContext);

    void Init(IInitContextPtr initContext) final;
    void Sync() final;
    void Commit() final;

    void ProcessPartitionStatuses(const THashMap<TKey, TExtendedSourcePartitionStatusPtr>& statuses) override;
    void CheckDistributionFinished();

    void UpdateMetrics();
    std::optional<THashMap<TKey, NYTree::IMapNodePtr>> ListKeys() override;

    std::optional<TStreamTraverseDataPtr> GetFutureKeysStreamTraverseData() override;


    static double GetDesiredRowsPerSecond(
        const TDynamicTableSourceParametersPtr& dynamicParameters,
        const TSourceControllerTablePtr& distributingTable);

    static i64 GetRemainingTableRows(
        const TSourceControllerTablePtr& distributingTable,
        const THashMap<TRangeId, i64>& committedOffsetsExclusive);

    static THashMap<TRangeId, NYTree::IMapNodePtr> DoDistributing(
        const TDynamicTableSourceParametersPtr& dynamicParameters,
        double desiredRangeRowsPerSecond,
        const THashMap<TRangeId, i64>& committedOffsetsExclusive,
        // Table with not distributed range. DistributedRows and DistributingRanges are modified inplace.
        const TSourceControllerTablePtr& distributingTable,
        std::function<TRangeId()> rangeIdGenerator);

    static void FilterTables(
        std::vector<TSourceControllerTablePtr>& tables,
        const TDynamicTableSourceParametersPtr& dynamicSourceParameters,
        const TSourceControllerTablePtr& lastProcessingTable,
        const TEventNameOrderPtr& nameOrder = {});

    static double GetDesiredRangeRowsPerSecond(
        const TDynamicTableSourceParametersPtr& dynamicParameters,
        const TSourceControllerTablePtr& distributingTable);

    static TSystemTimestamp ExtractTimestamp(
        const NYTree::INodePtr& node,
        const TTableTimestampLocatorSpecPtr& locator);

    static std::vector<std::string> GetRequiredTableAttributes(const TTableSourceParametersPtr& sourceSpec);

    static std::vector<TSourceControllerTablePtr> MakeTables(
        const std::vector<std::pair<NYPath::TRichYPath, NYTree::INodePtr>>& tablesInfo,
        const TTableSourceParametersPtr& sourceSpec,
        const TDynamicTableSourceParametersPtr& dynamicSourceSpec,
        const TSourceControllerTablePtr& lastProcessingTable,
        i64 era,
        const TEventNameOrderPtr& nameOrder = {});

    static std::pair<NYPath::TRichYPath, NYTree::INodePtr> ResolveTable(
        const NApi::IClientPtr& client,
        const NYPath::TRichYPath& table,
        const std::vector<std::string>& attributes);

    static void AssignEventOrdinals(
        std::vector<TSourceControllerTablePtr>& tables,
        TSourceControllerState* state);

    static void UpdateControllerState(
        TSourceControllerState* state,
        const std::vector<TSourceControllerTablePtr>& tables,
        const NLogging::TLogger& publicLogger);

    static void ApplyRestartInstantLogic(
        TSourceControllerState* state,
        TInstant restartInstant,
        const NLogging::TLogger& publicLogger);

    // Get all new tables with timestamp >= watermark.
    // Used to estimate lags. And to get some tables to process.
    // Needs some cache.
    // Returned tables are sorted by EventTimestamp.
    // Function overrides must be thread-safe.
    virtual TListedTables GetTables(
        const TTableSourceParametersPtr& sourceSpec,
        const TDynamicTableSourceParametersPtr& dynamicSourceSpec,
        i64 era,
        const TSourceControllerTablePtr& lastProcessingTable,
        const TEventNameOrderPtr& nameOrder);

    static std::vector<std::string> GetPathClusters(const NYPath::TRichYPath& path);

    static bool IsMultiCluster(const TTableSourceParametersPtr& sourceSpec);

    static std::vector<TSourceControllerTablePtr> MergeByName(
        const std::vector<std::vector<TSourceControllerTablePtr>>& perClusterActiveFirst);

    static TSourceControllerTablePtr FindReplicaServingCurrentTable(
        const std::vector<TSourceControllerTablePtr>& tables,
        const std::string& name);

    static TSourceControllerTablePtr MakeFailoverTable(
        const TSourceControllerTablePtr& current,
        const TSourceControllerTablePtr& serving,
        const TSourceControllerTablePtr& resumeFrom);

    static std::optional<TFailoverDecision> DecideFailover(
        const TSourceControllerTablePtr& current,
        const TSourceControllerTablePtr& servingReplica,
        const THashMap<std::string, TSourceControllerTablePtr>& stash,
        const NLogging::TLogger& publicLogger);

    static void StashRangesForCleanup(
        TSourceControllerState* state,
        const TSourceControllerTablePtr& table);

    static void ResetStashIfTableChanged(
        TSourceControllerState* state,
        const TSourceControllerTablePtr& current);

    static void EnsureCurrentPresent(
        std::vector<TSourceControllerTablePtr>& tables,
        const TSourceControllerTablePtr& current);

private:
    bool CheckDistributingTable();

    void ReconcileDistributingTable(TListedTables listed);

    std::vector<TSourceControllerTablePtr> ListClusterTables(
        const std::string& cluster,
        const TTableSourceParametersPtr& sourceSpec,
        const TDynamicTableSourceParametersPtr& dynamicSourceSpec,
        i64 era,
        const TSourceControllerTablePtr& lastProcessingTable,
        const TEventNameOrderPtr& nameOrder);

    TListedTables GetSingleClusterTables(
        const TTableSourceParametersPtr& sourceSpec,
        const TDynamicTableSourceParametersPtr& dynamicSourceSpec,
        i64 era,
        const TSourceControllerTablePtr& lastProcessingTable,
        const TEventNameOrderPtr& nameOrder);

    TListedTables GetMultiClusterTables(
        const TTableSourceParametersPtr& sourceSpec,
        const TDynamicTableSourceParametersPtr& dynamicSourceSpec,
        i64 era,
        const TSourceControllerTablePtr& lastProcessingTable,
        const TEventNameOrderPtr& nameOrder);

private:
    IStatusErrorStatePtr CheckDistributingTableErrorState_;
    TMutableStateClient<TSourceControllerState> State_;
    THashMap<TRangeId, i64> CommittedOffsetsExclusive_;
    TFuture<TListedTables> TablesFuture_;

    struct TMetrics
    {
        TMetrics(NProfiling::TProfiler profiler);

        NProfiling::TGauge ProcessedTables;
        NProfiling::TGauge LostTables;
    } Metrics_;
};

////////////////////////////////////////////////////////////////////////////////

std::pair<i64, i64> GetRowIndexRange(const NYPath::TRichYPath& path);
void SetRowIndexRange(NYPath::TRichYPath& path, i64 lower, i64 upper);

// Compares path & cluster.
bool IsOneTable(NYPath::TRichYPath& a, NYPath::TRichYPath& b);

////////////////////////////////////////////////////////////////////////////////


} // namespace NYT::NFlow::NStaticTableConnectorV2
