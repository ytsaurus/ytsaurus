#include "statistics_reporter.h"

#include "query_context.h"
#include "host.h"

#include <yt/yt/server/lib/job_agent/estimate_size_helpers.h>

#include <yt/yt/server/lib/misc/archive_reporter.h>

#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/misc/statistics.h>

namespace NYT::NClickHouseServer {

using namespace NApi;
using namespace NConcurrency;
using namespace NTableClient;
using namespace NYson;
using namespace NYTree;

using NJobAgent::EstimateSizes;

////////////////////////////////////////////////////////////////////////////////

static const NProfiling::TRegistry ReporterProfiler("/statistics_reporter");

////////////////////////////////////////////////////////////////////////////////

struct TDistributedQueriesTableDescriptor
{
    TDistributedQueriesTableDescriptor()
        : NameTable(New<TNameTable>())
        , Index(NameTable)
    { }

    static const TDistributedQueriesTableDescriptor& Get()
    {
        static const TDistributedQueriesTableDescriptor descriptor;
        return descriptor;
    }

    struct TIndex
    {
        explicit TIndex(const TNameTablePtr& nameTable)
            : InitialQueryId(nameTable->RegisterName("initial_query_id"))
            , QueryId(nameTable->RegisterName("query_id"))
            , InstanceCookie(nameTable->RegisterName("instance_cookie"))
            , InstanceAddress(nameTable->RegisterName("instance_address"))
            , CliqueId(nameTable->RegisterName("clique_id"))
            , StartTime(nameTable->RegisterName("start_time"))
            , FinishTime(nameTable->RegisterName("finish_time"))
            , Statistics(nameTable->RegisterName("statistics"))
            , SelectQueries(nameTable->RegisterName("select_queries"))
            , SecondaryQueryIds(nameTable->RegisterName("secondary_query_ids"))
            , ProxyAddress(nameTable->RegisterName("proxy_address"))
        { }

        //! Queries can create secondary queries and form a tree-like structure.
        //! The root node of this tree is the initial query and it is always a query made by a user.
        const int InitialQueryId;
        const int QueryId;
        const int InstanceCookie;
        const int InstanceAddress;
        const int CliqueId;
        //! Query execution time interval.
        //! All secondary queries created by this query should have nested time intervals.
        //! (with an accuracy of clock skew and async TQueryContext destruction)
        const int StartTime;
        const int FinishTime;
        //! Aggregated statistics for this query and for all secondary queries in the subtree.
        const int Statistics;
        //! List with text of select queries which were executed by StorageDistributor.
        //! Each select query can generate several secondary queries.
        const int SelectQueries;
        //! Secondary queries which were created by this query.
        //! It does not include non-direct children (grandchildren, etc).
        const int SecondaryQueryIds;
        //! A proxy from which the initial query arrived.
        //! Can be null for non-initial distributed queries.
        const int ProxyAddress;
    };

    const TNameTablePtr NameTable;
    const TIndex Index;
};

////////////////////////////////////////////////////////////////////////////////

struct TSecondaryQueriesTableDescriptor
{
    TSecondaryQueriesTableDescriptor()
        : NameTable(New<TNameTable>())
        , Index(NameTable)
    { }

    static const TSecondaryQueriesTableDescriptor& Get()
    {
        static const TSecondaryQueriesTableDescriptor descriptor;
        return descriptor;
    }

    struct TIndex
    {
        explicit TIndex(const TNameTablePtr& nameTable)
            : InitialQueryId(nameTable->RegisterName("initial_query_id"))
            , ParentQueryId(nameTable->RegisterName("parent_query_id"))
            , QueryId(nameTable->RegisterName("query_id"))
            , InstanceCookie(nameTable->RegisterName("instance_cookie"))
            , InstanceAddress(nameTable->RegisterName("instance_address"))
            , StartTime(nameTable->RegisterName("start_time"))
            , FinishTime(nameTable->RegisterName("finish_time"))
            , SelectQueryIndex(nameTable->RegisterName("select_query_index"))
            , Statistics(nameTable->RegisterName("statistics"))
        { }

        const int InitialQueryId;
        //! Id of the distributed query which has created this secondary query.
        //! The initial query can be reached by going to the parent in a few hops.
        const int ParentQueryId;
        const int QueryId;
        const int InstanceCookie;
        const int InstanceAddress;
        const int StartTime;
        const int FinishTime;
        //! Index of the select query in parent's SelectQueries list
        //! which has created this secondary query.
        const int SelectQueryIndex;
        //! Statistics of this query only.
        //! It does not include aggregated statistics from secondary queries.
        const int Statistics;
    };

    const TNameTablePtr NameTable;
    const TIndex Index;
};

////////////////////////////////////////////////////////////////////////////////

struct TAncestorQueryIdsTableDescriptor
{
    TAncestorQueryIdsTableDescriptor()
        : NameTable(New<TNameTable>())
        , Index(NameTable)
    { }

    static const TAncestorQueryIdsTableDescriptor& Get()
    {
        static const TAncestorQueryIdsTableDescriptor descriptor;
        return descriptor;
    }

    struct TIndex
    {
        explicit TIndex(const TNameTablePtr& nameTable)
            : SecondaryQueryId(nameTable->RegisterName("secondary_query_id"))
            , ParentQueryId(nameTable->RegisterName("parent_query_id"))
            , InitialQueryId(nameTable->RegisterName("initial_query_id"))
        { }

        const int SecondaryQueryId;
        const int ParentQueryId;
        const int InitialQueryId;
    };

    const TNameTablePtr NameTable;
    const TIndex Index;
};

////////////////////////////////////////////////////////////////////////////////

class TDistributedQueryRowlet
    : public IArchiveRowlet
{
public:
    TDistributedQueryRowlet(const TQueryContextPtr& queryContext)
        : InitialQueryId_(ToString(queryContext->InitialQueryId))
        , QueryId_(ToString(queryContext->QueryId))
        , InstanceCookie_(queryContext->Host->GetInstanceCookie())
        , InstanceAddress_(*queryContext->Host->GetConfig()->Address)
        , CliqueId_(ToString(queryContext->Host->GetConfig()->CliqueId))
        , StartTime_(queryContext->GetStartTime())
        , FinishTime_(queryContext->GetFinishTime())
        , Statistics_(ConvertToYsonString(queryContext->AggregatedStatistics))
        , SelectQueries_(ConvertToYsonString(queryContext->SelectQueries))
        , SecondaryQueryIds_(ConvertToYsonString(queryContext->SecondaryQueryIds))
        , ProxyAddress_(
            queryContext->QueryKind == EQueryKind::InitialQuery ?
                std::make_optional(queryContext->CurrentAddress) :
                std::nullopt)
    { }

    size_t EstimateSize() const override
    {
        return EstimateSizes(
            InitialQueryId_,
            QueryId_,
            InstanceCookie_,
            InstanceAddress_,
            CliqueId_,
            StartTime_,
            FinishTime_,
            Statistics_,
            SelectQueries_,
            SecondaryQueryIds_,
            ProxyAddress_);
    }

    TUnversionedOwningRow ToRow(int /*archiveVersion*/) const override
    {
        const auto& index = TDistributedQueriesTableDescriptor::Get().Index;

        TUnversionedOwningRowBuilder builder;
        builder.AddValue(MakeUnversionedStringValue(InitialQueryId_, index.InitialQueryId));
        builder.AddValue(MakeUnversionedStringValue(QueryId_, index.QueryId));
        builder.AddValue(MakeUnversionedInt64Value(InstanceCookie_, index.InstanceCookie));
        builder.AddValue(MakeUnversionedStringValue(InstanceAddress_, index.InstanceAddress));
        builder.AddValue(MakeUnversionedStringValue(CliqueId_, index.CliqueId));
        builder.AddValue(MakeUnversionedUint64Value(StartTime_.GetValue(), index.StartTime));
        builder.AddValue(MakeUnversionedUint64Value(FinishTime_.GetValue(), index.FinishTime));
        builder.AddValue(MakeUnversionedAnyValue(Statistics_.AsStringBuf(), index.Statistics));
        builder.AddValue(MakeUnversionedAnyValue(SelectQueries_.AsStringBuf(), index.SelectQueries));
        builder.AddValue(MakeUnversionedAnyValue(SecondaryQueryIds_.AsStringBuf(), index.SecondaryQueryIds));

        if (ProxyAddress_) {
            builder.AddValue(MakeUnversionedStringValue(*ProxyAddress_, index.ProxyAddress));
        }

        return builder.FinishRow();
    }

private:
    TString InitialQueryId_;
    TString QueryId_;
    int InstanceCookie_;
    TString InstanceAddress_;
    TString CliqueId_;
    TInstant StartTime_;
    TInstant FinishTime_;
    TYsonString Statistics_;
    TYsonString SelectQueries_;
    TYsonString SecondaryQueryIds_;
    std::optional<TString> ProxyAddress_;
};

////////////////////////////////////////////////////////////////////////////////

class TSecondaryQueryRowlet
    : public IArchiveRowlet
{
public:
    TSecondaryQueryRowlet(const TQueryContextPtr& queryContext)
        : InitialQueryId_(ToString(queryContext->InitialQueryId))
        , ParentQueryId_(ToString(queryContext->ParentQueryId))
        , QueryId_(ToString(queryContext->QueryId))
        , InstanceCookie_(queryContext->Host->GetInstanceCookie())
        , InstanceAddress_(*queryContext->Host->GetConfig()->Address)
        , StartTime_(queryContext->GetStartTime())
        , FinishTime_(queryContext->GetFinishTime())
        , SelectQueryIndex_(queryContext->SelectQueryIndex)
        , Statistics_(ConvertToYsonString(queryContext->InstanceStatistics))
    { }

    size_t EstimateSize() const override
    {
        return EstimateSizes(
            InitialQueryId_,
            ParentQueryId_,
            QueryId_,
            InstanceCookie_,
            InstanceAddress_,
            StartTime_,
            FinishTime_,
            SelectQueryIndex_,
            Statistics_);
    }

    TUnversionedOwningRow ToRow(int /*archiveVersion*/) const override
    {
        const auto& index = TSecondaryQueriesTableDescriptor::Get().Index;

        TUnversionedOwningRowBuilder builder;
        builder.AddValue(MakeUnversionedStringValue(InitialQueryId_, index.InitialQueryId));
        builder.AddValue(MakeUnversionedStringValue(ParentQueryId_, index.ParentQueryId));
        builder.AddValue(MakeUnversionedStringValue(QueryId_, index.QueryId));
        builder.AddValue(MakeUnversionedInt64Value(InstanceCookie_, index.InstanceCookie));
        builder.AddValue(MakeUnversionedStringValue(InstanceAddress_, index.InstanceAddress));
        builder.AddValue(MakeUnversionedUint64Value(StartTime_.GetValue(), index.StartTime));
        builder.AddValue(MakeUnversionedUint64Value(FinishTime_.GetValue(), index.FinishTime));
        builder.AddValue(MakeUnversionedInt64Value(SelectQueryIndex_, index.SelectQueryIndex));
        builder.AddValue(MakeUnversionedAnyValue(Statistics_.AsStringBuf(), index.Statistics));

        return builder.FinishRow();
    }

private:
    TString InitialQueryId_;
    TString ParentQueryId_;
    TString QueryId_;
    int InstanceCookie_;
    TString InstanceAddress_;
    TInstant StartTime_;
    TInstant FinishTime_;
    int SelectQueryIndex_;
    TYsonString Statistics_;
};

////////////////////////////////////////////////////////////////////////////////

class TAncestorQueryIdsRowlet
    : public IArchiveRowlet
{
public:
    TAncestorQueryIdsRowlet(const TQueryContextPtr& queryContext)
        : SecondaryQueryId_(ToString(queryContext->QueryId))
        , ParentQueryId_(ToString(queryContext->ParentQueryId))
        , InitialQueryId_(ToString(queryContext->InitialQueryId))
    { }

    size_t EstimateSize() const override
    {
        return EstimateSizes(
            SecondaryQueryId_,
            ParentQueryId_,
            InitialQueryId_);
    }

    TUnversionedOwningRow ToRow(int /*archiveVersion*/) const override
    {
        const auto& index = TAncestorQueryIdsTableDescriptor::Get().Index;

        TUnversionedOwningRowBuilder builder;
        builder.AddValue(MakeUnversionedStringValue(SecondaryQueryId_, index.SecondaryQueryId));
        builder.AddValue(MakeUnversionedStringValue(ParentQueryId_, index.ParentQueryId));
        builder.AddValue(MakeUnversionedStringValue(InitialQueryId_, index.InitialQueryId));

        return builder.FinishRow();
    }

private:
    TString SecondaryQueryId_;
    TString ParentQueryId_;
    TString InitialQueryId_;
};

////////////////////////////////////////////////////////////////////////////////

class TQueryStatisticsReporter
    : public IQueryStatisticsReporter
{
public:
    TQueryStatisticsReporter(
        TQueryStatisticsReporterConfigPtr config,
        NNative::IClientPtr client)
        : Config_(std::move(config))
        , DistributedQueryHandler_(
            CreateArchiveReporter(
                Version_,
                Config_,
                Config_->DistributedQueriesHandler,
                TDistributedQueriesTableDescriptor::Get().NameTable,
                "distributed_queries",
                client,
                Reporter_->GetInvoker(),
                ReporterProfiler.WithTag("reporter_type", "distributed_queries")))
        , SecondaryQueryHandler_(
            CreateArchiveReporter(
                Version_,
                Config_,
                Config_->SecondaryQueriesHandler,
                TSecondaryQueriesTableDescriptor::Get().NameTable,
                "secondary_queries",
                client,
                Reporter_->GetInvoker(),
                ReporterProfiler.WithTag("reporter_type", "secondary_queries")))
        , AncestorQueryIdsHandler_(
            CreateArchiveReporter(
                Version_,
                Config_,
                Config_->AncestorQueryIdsHandler,
                TAncestorQueryIdsTableDescriptor::Get().NameTable,
                "ancestor_query_ids",
                client,
                Reporter_->GetInvoker(),
                ReporterProfiler.WithTag("reporter_type", "ancestor_query_ids")))
    { }

    void ReportQueryStatistics(const TQueryContextPtr& queryContext) override
    {
        // A query is 'distributed' if it has created at least 1 secondary query.
        if (!queryContext->SelectQueries.empty()) {
            DistributedQueryHandler_->Enqueue(std::make_unique<TDistributedQueryRowlet>(queryContext));
        }

        if (queryContext->QueryKind == EQueryKind::SecondaryQuery) {
            SecondaryQueryHandler_->Enqueue(std::make_unique<TSecondaryQueryRowlet>(queryContext));
            AncestorQueryIdsHandler_->Enqueue(std::make_unique<TAncestorQueryIdsRowlet>(queryContext));
        }
    }

private:
    const TQueryStatisticsReporterConfigPtr Config_;
    const TActionQueuePtr Reporter_ = New<TActionQueue>("StatisticsReporter");
    const TArchiveVersionHolderPtr Version_ = New<TArchiveVersionHolder>();
    const IArchiveReporterPtr DistributedQueryHandler_;
    const IArchiveReporterPtr SecondaryQueryHandler_;
    const IArchiveReporterPtr AncestorQueryIdsHandler_;
};

////////////////////////////////////////////////////////////////////////////////

IQueryStatisticsReporterPtr CreateQueryStatisticsReporter(
    TQueryStatisticsReporterConfigPtr config,
    NNative::IClientPtr client)
{
    return New<TQueryStatisticsReporter>(
        std::move(config),
        std::move(client));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
