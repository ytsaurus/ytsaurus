#include "export_queue.h"

#include "config.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/transaction.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_owner_ypath_proxy.h>
#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/chunk_spec_fetcher.h>
#include <yt/yt/ytlib/chunk_client/chunk_teleporter.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/input_chunk.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/object_client/helpers.h>
#include <yt/yt/ytlib/object_client/object_service_proxy.h>
#include <yt/yt/ytlib/object_client/object_ypath_proxy.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/yt/ytlib/transaction_client/transaction_manager.h>

#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/ytree/helpers.h>
#include <yt/yt/core/ytree/ypath_proxy.h>

#include <library/cpp/yt/containers/enum_indexed_array.h>

#include <library/cpp/cron_expression/cron_expression.h>

#include <util/generic/map.h>

namespace NYT::NApi::NInternalProxy {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NHiveClient;
using namespace NLogging;
using namespace NObjectClient;
using namespace NQueueClient;
using namespace NSecurityClient;
using namespace NTableClient;
using namespace NTableClient::NProto;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

// The following terminology is used for queue static table exports.
//
// A single call of this handle performs an _export_, which creates several _exported tables_,
// and each _exported table_ corresponds to a distinct _export (unix) timestamp_.
//
// Queue rows are divided in ranges. Each range is described by unix timestamp range
// [previous export unix ts; export unix ts), and the right bound of the range is used to describe it.
//
// Row belongs to the range with export unix timestamp T, iff unix timestamp from its chunk max timestamp
// lies in that range or, in case the rows of that range have already been exported, the first range that
// has not been exported yet. The latter can happen if the row was flushed too late to be a part of the
// exported table it belongs to.
//
// Both TInstant and Unix Time measure time since epoch (timezone is UTC).
// TInstant has granularity in milliseconds and we convert it to Unix Time by discarding milliseconds.
// Unix timestamp T corresponds to TInstant::Seconds(T).
// Most of calculations are done with unix time, since we chose our granularity to be in seconds.

//! Find export unix ts range containing #now for a given #schedule.
//!
//! Returns [#beginTs; endTs), where endTs is also export unix ts describing the range.
//! Alternatively, beginTs is export unix ts that comes before endTs.
std::pair<ui64, ui64> GetExportUnixTsRange(
    TInstant now,
    const TQueueExportSchedule& schedule)
{
    switch (schedule.GetType()) {
        case EQueueExportScheduleType::Period: {
            auto period = schedule.GetPeriod().Seconds();
            THROW_ERROR_EXCEPTION_IF(period == 0, "Queue export period must be at least a second")
                .With("period", schedule.GetPeriod());

            std::pair<ui64, ui64> result;
            result.first = (now.Seconds() / period) * period;
            result.second = result.first + period;
            return result;
        }
        case EQueueExportScheduleType::CronExpression: {
            static const auto UTCTimezone = NDatetime::TTimeZone{};

            TCronExpression cronExpression{schedule.GetCronExpression().Underlying()};
            auto nowCivilSecond = NDatetime::Convert(now, UTCTimezone);
            auto endCivilSecond = cronExpression.CronNext(nowCivilSecond);
            return std::pair<ui64, ui64>{
                NDatetime::Convert(cronExpression.CronPrev(endCivilSecond), UTCTimezone).Seconds(),
                NDatetime::Convert(endCivilSecond, UTCTimezone).Seconds()};
        }
    }
    YT_ABORT();
}

std::pair<ui64, ui64> GetExportUnixTsRange(
    ui64 unixTs,
    const TQueueExportSchedule& schedule)
{
    return GetExportUnixTsRange(TInstant::Seconds(unixTs), schedule);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

ui64 GetExportUnixTsUpperBound(
    TInstant now,
    const TQueueExportSchedule& schedule)
{
    return GetExportUnixTsRange(now, schedule).first;
}

ui64 GetLastExportPeriod(ui64 exportUnixTs, const TQueueExportSchedule& schedule)
{
    switch (schedule.GetType()) {
        case EQueueExportScheduleType::Period:
            return schedule.GetPeriod().Seconds();
        case EQueueExportScheduleType::CronExpression: {
            static const auto UTCTimezone = NDatetime::TTimeZone{};

            TCronExpression cronExpression{schedule.GetCronExpression().Underlying()};
            auto exportUnixTsCivilSecond = NDatetime::Convert(TInstant::Seconds(exportUnixTs), UTCTimezone);
            if (cronExpression.CronPrev(cronExpression.CronNext(exportUnixTsCivilSecond)) != exportUnixTsCivilSecond) {
                THROW_ERROR_EXCEPTION("Value of exportUnixTs is not a valid export timestamp")
                    .With("export_unix_ts", exportUnixTs)
                    .With("cron_expression", schedule.GetCronExpression().Underlying());
            }
            return exportUnixTs - NDatetime::Convert(cronExpression.CronPrev(exportUnixTsCivilSecond), UTCTimezone).Seconds();
        }
    }
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

using TChunkSpecPtrs = std::vector<const TChunkSpec*>;

//! Number of rows in the chunk described by #chunkSpec.
i64 GetChunkRowCount(const TChunkSpec& chunkSpec)
{
    return GetProtoExtension<TMiscExt>(chunkSpec.chunk_meta().extensions()).row_count();
}

//! Row index of the row right after the last row of the chunk described by #chunkSpec.
i64 GetChunkEndOffset(const TChunkSpec& chunkSpec)
{
    return chunkSpec.table_row_index() + GetChunkRowCount(chunkSpec);
}

TTimestamp GetChunkMaxTimestamp(const TChunkSpec& chunkSpec)
{
    return FromProto<TTimestamp>(GetProtoExtension<TMiscExt>(chunkSpec.chunk_meta().extensions()).max_timestamp());
}

////////////////////////////////////////////////////////////////////////////////

class TQueueExportSession
    : public TRefCounted
{
public:
    TQueueExportSession(
        NNative::IClientPtr client,
        IInvokerPtr invoker,
        TYPath queuePath,
        TYPath exportDirectory,
        THashMap<i64, TChunkId> lastExportedChunkIds,
        THashMap<i64, i64> startOffsets,
        TTimestamp startTimestamp,
        TQueueExportSchedule schedule,
        TExportQueueIntoStaticTableOptions options,
        TQueueExportRunnerConfigPtr config,
        TLogger logger)
        : Client_(std::move(client))
        , Invoker_(std::move(invoker))
        , Connection_(Client_->GetNativeConnection())
        , Queue_(std::move(queuePath))
        , ExportDirectory_(std::move(exportDirectory))
        , LastExportedChunkIds_(std::move(lastExportedChunkIds))
        , StartOffsets_(std::move(startOffsets))
        , StartTimestamp_(startTimestamp)
        , Schedule_(std::move(schedule))
        , Options_(std::move(options))
        , Config_(std::move(config))
        , Logger(std::move(logger))
    { }

    TFuture<TExportQueueIntoStaticTableResult> Run()
    {
        return BIND(&TQueueExportSession::DoRun, MakeStrong(this))
            .AsyncVia(Invoker_)
            .Run();
    }

private:
    const NNative::IClientPtr Client_;
    const IInvokerPtr Invoker_;
    const NNative::IConnectionPtr Connection_;

    const TYPath Queue_;
    const TYPath ExportDirectory_;
    const THashMap<i64, TChunkId> LastExportedChunkIds_;
    const THashMap<i64, i64> StartOffsets_;
    const TTimestamp StartTimestamp_;
    const TQueueExportSchedule Schedule_;
    const TExportQueueIntoStaticTableOptions Options_;
    const TQueueExportRunnerConfigPtr Config_;

    const TLogger Logger;

    //! Subtransaction of the caller's transaction all the Cypress requests are performed in.
    ITransactionPtr Transaction_;

    //! Export unix ts of the first exported table that can be created by this handle.
    ui64 FirstExportUnixTs_;
    //! Upper bound on export unix ts of exported tables created by this handle.
    //! NB: We use the host's physical time to compute this value.
    //! This is fine, see the comment for DoRun below.
    ui64 ExportUnixTsUpperBound_;
    //! Corresponds to the queue being exported.
    TUserObject QueueObject_;
    TTableSchemaPtr QueueSchema_;
    TMasterTableSchemaId QueueSchemaId_;
    //! Original chunk specs fetched from master.
    std::vector<TChunkSpec> ChunkSpecs_;
    //! Grouping of chunk specs by their corresponding export unix ts.
    TMap<ui64, std::vector<const TChunkSpec*>> ChunkSpecsToExportByUnixTs_;
    //! Ids of hunk chunks referenced by corresponding group of chunk specs scheduled for export.
    THashMap<ui64, std::vector<TChunkId>> HunkChunkIdsToExportByUnixTs_;

    TExportQueueIntoStaticTableResult Result_;

    struct TTaskPart final
    {
        //! Export unix ts of exported table to be created.
        const ui64 ExportUnixTs;
        //! Lower bound of the unix ts range of the rows it holds, i.e. export unix ts of the previous exported table.
        const ui64 UnixTsLowerBound;

        //! Chunk specs corresponding to exported table to be created.
        const std::vector<const TChunkSpec*>& ChunkSpecsToExport;

        //! Ids of hunk chunks referenced by chunks corresponding to this task part.
        const std::vector<TChunkId>* HunkChunkIdsToExport = nullptr;

        //! Name of the exported table to be created. Filled in once the task part starts running, since
        //! computing it may fail and such a failure has to be reported as a failure of this task part.
        std::string TableName;

        //! Options used for Cypress requests.
        TTransactionalOptions TransactionOptions;
        //! Corresponds to the actual output table created.
        TUserObject DestinationObject;
        TTransactionPtr UploadTransaction;

        //! Data statistics collected from attaching chunks.
        TDataStatistics DataStatistics;

        bool ShouldExportHunkChunks() const
        {
            return HunkChunkIdsToExport && !HunkChunkIdsToExport->empty();
        }
    };

    std::vector<TTaskPart> TaskParts_;

    //! Performs the following steps:
    //!   1) Starts a subtransaction of the caller's transaction, obtains a snapshot lock on the input queue.
    //!   2) Determines export unix ts upper bound as the most recent exported table that can be created according to the current physical time.
    //!   3) Fetches chunk specs, skipping dynamic stores, already exported chunks and chunks with timestamp larger than
    //!      the export unix timestamp upper bound.
    //!   4) Groups together chunk specs by export unix ts corresponding to their export unix ts.
    //!   5) For tables with hunk chunks drops groups starting with the first one that has a reference to an unsealed hunk chunk.
    //!   6) Creates at most #GetTableCountLimit exported tables, stopping at the first failure.
    //!
    //! NB: We use the host's physical time to compute the unix ts of the next table to export.
    //! We rely on this time being mostly monotonous and not too different from the cluster time obtained via timestamp generation.
    //! In any case, we will only produce an output table if its unix ts is strictly greater than the one of the last exported table.
    //! If the physical time diverges from the cluster time, the only effect is that some chunks might end up being
    //! exported into later tables, which is perfectly fine.
    TExportQueueIntoStaticTableResult DoRun()
    {
        YT_TLOG_INFO("Started exporting queue into static tables")
            .With("LastExportedChunkIds", LastExportedChunkIds_)
            .With("StartOffsets", StartOffsets_)
            .With("StartTimestamp", StartTimestamp_)
            .With("Schedule", Schedule_);
        auto logFinally = Finally([&] {
            YT_TLOG_INFO("Finished exporting queue into static tables");
        });

        StartTransaction();

        try {
            LockQueue();
            PrepareQueueForExport();

            if (ComputeExportUnixTsBounds()) {
                FetchChunkSpecs();
                SelectChunkSpecsToExport();
                PrepareTaskParts();
                RunTaskParts();
            }
        } catch (const std::exception&) {
            YT_UNUSED_FUTURE(Transaction_->Abort());
            throw;
        }

        CommitTransaction();

        return Result_;
    }

    void StartTransaction()
    {
        if (!Options_.TransactionId) {
            THROW_ERROR_EXCEPTION("Transaction is required for exporting queue %v", Queue_);
        }

        Transaction_ = WaitFor(Client_->StartTransaction(
            ETransactionType::Master,
            TTransactionStartOptions{
                .ParentId = Options_.TransactionId,
            }))
            .ValueOrThrow();

        YT_TLOG_INFO("Started export transaction")
            .With("TransactionId", Transaction_->GetId())
            .With("ParentTransactionId", Options_.TransactionId);
    }

    void CommitTransaction()
    {
        auto commitResultOrError = WaitFor(Transaction_->Commit());
        THROW_ERROR_EXCEPTION_IF_FAILED(
            commitResultOrError,
            "Error committing export transaction for queue %v",
            Queue_);
    }

    void LockQueue()
    {
        TLockNodeOptions lockOptions;
        lockOptions.TransactionId = Transaction_->GetId();
        auto queueObjectId = WaitFor(Client_->LockNode(Queue_, ELockMode::Snapshot, lockOptions))
            .ValueOrThrow()
            .NodeId;

        QueueObject_ = TUserObject(FromObjectId(queueObjectId), Transaction_->GetId());
    }

    i64 GetTableCountLimit() const
    {
        auto hardLimit = Config_->MaxTableCountPerCall;
        return std::min<i64>(Options_.TableCountLimit.value_or(hardLimit), hardLimit);
    }

    void PrepareTaskParts()
    {
        TaskParts_.reserve(ChunkSpecsToExportByUnixTs_.size());

        auto tableCountLimit = GetTableCountLimit();
        int preparedTableCount = 0;
        auto unixTsLowerBound = UnixTimeFromTimestamp(StartTimestamp_);

        for (const auto& [exportUnixTs, chunkSpecs] : ChunkSpecsToExportByUnixTs_) {
            if (preparedTableCount >= tableCountLimit) {
                break;
            }

            TaskParts_.push_back(TTaskPart{
                .ExportUnixTs = exportUnixTs,
                .UnixTsLowerBound = unixTsLowerBound,
                .ChunkSpecsToExport = chunkSpecs,
            });
            unixTsLowerBound = exportUnixTs;

            auto hunkChunkIdsIt = HunkChunkIdsToExportByUnixTs_.find(exportUnixTs);
            if (hunkChunkIdsIt != HunkChunkIdsToExportByUnixTs_.end()) {
                TaskParts_.back().HunkChunkIdsToExport = &hunkChunkIdsIt->second;
            }

            ++preparedTableCount;
        }

        Result_.PreparedTableCount = preparedTableCount;
        Result_.HasMore = std::ssize(ChunkSpecsToExportByUnixTs_) > preparedTableCount;

        YT_TLOG_DEBUG("Prepared for creating exported tables")
            .With("TableCount", preparedTableCount)
            .With("AvailableTableCount", ChunkSpecsToExportByUnixTs_.size())
            .With("TableCountLimit", tableCountLimit);
    }

    void RunTaskParts()
    {
        Result_.Tables.reserve(TaskParts_.size());

        for (auto& taskPart : TaskParts_) {
            auto taskPartError = RunTaskPart(taskPart, Transaction_);
            Result_.Tables.push_back(BuildExportedTable(taskPart, taskPartError));

            if (!taskPartError.IsOK()) {
                YT_TLOG_ERROR("Queue export task part failed")
                    .With(taskPartError);
                break;
            }
        }

        YT_TLOG_DEBUG("Finished creating exported tables")
            .With("PreparedTableCount", Result_.PreparedTableCount)
            .With("AttemptedTableCount", Result_.Tables.size())
            .With("HasMore", Result_.HasMore);
    }

    TError RunTaskPart(TTaskPart& taskPart, const ITransactionPtr& parentTransaction)
    {
        if (taskPart.ChunkSpecsToExport.empty()) {
            return TError();
        }

        try {
            GuardedRunTaskPart(taskPart, parentTransaction);
        } catch (const std::exception& ex) {
            return TError("Queue export task part failed")
                .With(ex)
                .With("export_unix_ts", taskPart.ExportUnixTs);
        }
        return TError();
    }

    void GuardedRunTaskPart(TTaskPart& taskPart, const ITransactionPtr& parentTransaction)
    {
        taskPart.TableName = GetOutputTableName(taskPart.ExportUnixTs);

        auto taskPartTransaction = WaitFor(parentTransaction->StartTransaction(ETransactionType::Master))
            .ValueOrThrow();
        taskPart.TransactionOptions.TransactionId = taskPartTransaction->GetId();

        CreateOutputTable(taskPart);
        BeginUpload(taskPart);
        TeleportChunkMeta(taskPart);
        AttachChunks(taskPart);
        EndUpload(taskPart);

        WaitFor(taskPartTransaction->Commit())
            .ThrowOnError();
    }

    //! Builds the description of the exported table corresponding to #taskPart.
    //!
    //! \note A failed task part reports no tablets, since its subtransaction has been aborted.
    NQueueClient::TQueueExportStaticTable BuildExportedTable(const TTaskPart& taskPart, const TError& error)
    {
        NQueueClient::TQueueExportStaticTable table{
            .StartTimestamp = TimestampFromUnixTime(taskPart.UnixTsLowerBound),
            .FinishTimestamp = TimestampFromUnixTime(taskPart.ExportUnixTs),
            .TableName = taskPart.TableName,
            .Error = error,
        };

        if (!error.IsOK()) {
            return table;
        }

        for (const auto* chunkSpec : taskPart.ChunkSpecsToExport) {
            auto [tabletIt, inserted] = table.Tablets.try_emplace(chunkSpec->tablet_index());
            auto& tablet = tabletIt->second;
            if (inserted) {
                tablet.StartOffset = chunkSpec->table_row_index();
            }
            ++tablet.ChunkCount;
            tablet.FinishOffset = GetChunkEndOffset(*chunkSpec);
            tablet.MaxTimestamp = std::max(tablet.MaxTimestamp, GetChunkMaxTimestamp(*chunkSpec));
            tablet.LastChunk = FromProto<TChunkId>(chunkSpec->chunk_id());
        }

        return table;
    }

    ui64 GetMinExportUnixTs(TTimestamp timestamp)
    {
        // NB: The timestamp is in range [unixTs, unixTs + 1). Since our granularity is in seconds, we can compute the
        // next export unix ts as the strict next tick for the lower bound.
        return GetExportUnixTsRange(UnixTimeFromTimestamp(timestamp), Schedule_).second;
    }

    static void GetAndFillBasicAttributes(
        const TLogger& logger,
        const NNative::IClientPtr& client,
        TUserObject& object,
        bool populateSecurityTags)
    {
        const auto& Logger = logger;

        YT_TLOG_DEBUG("Started collecting basic attributes");

        auto proxy = CreateObjectServiceReadProxy(client, TMasterReadOptions().ReadFrom);
        auto req = TObjectYPathProxy::GetBasicAttributes(object.GetPath());
        req->set_populate_security_tags(populateSecurityTags);
        SetTransactionId(req, *object.TransactionId);

        auto rspOrError = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(
            rspOrError,
            "Error getting basic attributes of queue %v", object.GetPath());

        const auto& rsp = rspOrError.Value();

        object.ObjectId = FromProto<TObjectId>(rsp->object_id());
        object.Type = TypeFromId(object.ObjectId);
        object.ExternalCellTag = FromProto<TCellTag>(rsp->external_cell_tag());
        object.ExternalTransactionId = rsp->has_external_transaction_id()
            ? FromProto<TTransactionId>(rsp->external_transaction_id())
            : *object.TransactionId;
        if (populateSecurityTags) {
            object.SecurityTags =
                FromProto<std::vector<TSecurityTag>>(rsp->security_tags().items());
        }

        YT_TLOG_DEBUG("Finished collecting basic attributes");
    }

    IAttributeDictionaryPtr FetchNodeAttributes(const TYPath& path, const std::vector<TStringBuf>& attributeKeys) const
    {
        YT_TLOG_DEBUG("Started fetching attributes")
            .With("Path", path)
            .With("PathRequestedAttributes", attributeKeys);

        // TODO(achulkov2): Change to simple Client_->GetNode with attributes.
        auto proxy = CreateObjectServiceReadProxy(Client_, TMasterReadOptions().ReadFrom);
        auto req = TYPathProxy::Get(path + "/@");
        SetTransactionId(req, Transaction_->GetId());
        ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);

        auto rspOrError = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(
            rspOrError,
            "Error fetching attributes %v for path %v",
            attributeKeys,
            path);

        YT_TLOG_DEBUG("Finished fetching attributes")
            .With("Path", path)
            .With("PathRequestedAttributes", attributeKeys);
        return ConvertToAttributes(TYsonString(rspOrError.Value()->value()));
    }

    void PrepareQueueForExport()
    {
        GetAndFillBasicAttributes(Logger, Client_, QueueObject_, /*populateSecurityTags*/ true);
        YT_VERIFY(QueueObject_.GetObjectIdPath() == QueueObject_.GetPath());

        if (QueueObject_.Type != EObjectType::Table) {
            THROW_ERROR_EXCEPTION(
                "Invalid type of %v: expected %Qlv, %Qlv found",
                Queue_,
                EObjectType::Table,
                QueueObject_.Type);
        }

        auto attributes = FetchNodeAttributes(
            QueueObject_.GetPath(), {"chunk_count", "dynamic", "schema", "schema_id"});

        if (!attributes->Get<bool>("dynamic")) {
            THROW_ERROR_EXCEPTION("Queue %v should be a dynamic table", Queue_);
        }

        QueueSchema_ = attributes->Get<TTableSchemaPtr>("schema");
        QueueSchemaId_ = attributes->Get<TMasterTableSchemaId>("schema_id");
        if (QueueSchema_->IsSorted()) {
            THROW_ERROR_EXCEPTION("Queue %v should be an ordered dynamic table", Queue_);
        }

        if (!Options_.EnableExportFromQueueWithHunks && QueueSchema_->HasHunkColumns()) {
            THROW_ERROR_EXCEPTION("Exporting queue %v that contains hunk columns is disabled", Queue_);
        }

        QueueObject_.ChunkCount = attributes->Get<i64>("chunk_count");
    }

    //! Returns |false| iff there is provably nothing to export.
    bool ComputeExportUnixTsBounds()
    {
        // NB(apachee): It is possible that rows corresponding to the last exported table were flushed late,
        // in which case we export those rows to the next exported table.
        FirstExportUnixTs_ = GetExportUnixTsRange(UnixTimeFromTimestamp(StartTimestamp_), Schedule_).second;
        ExportUnixTsUpperBound_ = GetExportUnixTsUpperBound(TInstant::Now(), Schedule_);

        YT_TLOG_DEBUG("Computed export unix ts bounds")
            .With("FirstExportUnixTs", FirstExportUnixTs_)
            .With("ExportUnixTsUpperBound", ExportUnixTsUpperBound_);

        return FirstExportUnixTs_ <= ExportUnixTsUpperBound_;
    }

    THashSet<TChunkId> FetchUnsealedHunkChunkIds(const std::vector<TChunkId>& hunkChunkIds) const
    {
        for (auto hunkChunkId : hunkChunkIds) {
            if (QueueObject_.ExternalCellTag != CellTagFromId(hunkChunkId)) {
                THROW_ERROR_EXCEPTION("Encountered hunk chunk with unexpected cell tag")
                    .With("actual_cell_tag", CellTagFromId(hunkChunkId))
                    .With("expected_cell_tag", QueueObject_.ExternalCellTag);
            }
        }

        auto proxy = CreateObjectServiceReadProxy(
            Client_,
            TMasterReadOptions().ReadFrom,
            QueueObject_.ExternalCellTag);

        THashSet<TChunkId> unsealedHunkChunkIds;
        auto fetchChunkBatch = [&] (i64 lowerChunkIndex, i64 upperChunkIndex) {
            YT_TLOG_DEBUG("Started fetching seal state for a batch of hunk chunks")
                .With("LowerChunkIndex", lowerChunkIndex)
                .With("UpperChunkIndex", upperChunkIndex);

            auto batchReq = proxy.ExecuteBatch();

            for (int chunkIndex = lowerChunkIndex; chunkIndex < upperChunkIndex; ++chunkIndex) {
                auto hunkChunkId = hunkChunkIds[chunkIndex];
                auto req = TYPathProxy::Get(FromObjectId(hunkChunkId) + "/@sealed");
                batchReq->AddRequest(req);
            }

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            auto cumulativeError = GetCumulativeError(batchRspOrError);
            THROW_ERROR_EXCEPTION_UNLESS(cumulativeError.IsOK(),
                "Error fetching hunk chunk seal state from master cell")
                    .With("master_cell_tag", QueueObject_.ExternalCellTag)
                    .With(cumulativeError);

            auto responses = batchRspOrError.Value()->GetResponses<TYPathProxy::TRspGet>();
            THROW_ERROR_EXCEPTION_IF(std::ssize(responses) != upperChunkIndex - lowerChunkIndex,
                "Master cell returned hunk chunk seal state response of unexpected size")
                .With("master_cell_tag", QueueObject_.ExternalCellTag)
                .With("actual_response_size", responses.size())
                .With("expected_response_size", upperChunkIndex - lowerChunkIndex);

            for (int responseIndex = 0; responseIndex < std::ssize(responses); ++responseIndex) {
                auto maybeResponseValue = responses[responseIndex].ValueOrThrow();
                THROW_ERROR_EXCEPTION_UNLESS(maybeResponseValue,
                    "Upon fetching chunk seal states unexpectedely encountered null subresponse")
                    .With("response_index", responseIndex)
                    .With("total_response_size", responses.size())
                    .With("chunk_id", hunkChunkIds[lowerChunkIndex + responseIndex]);

                auto responseValue = maybeResponseValue->value();
                auto sealed = ConvertTo<bool>(TYsonString(responseValue));
                if (!sealed) {
                    auto hunkChunkId = hunkChunkIds[lowerChunkIndex + responseIndex];
                    unsealedHunkChunkIds.insert(hunkChunkId);
                }
            }
        };

        for (i64 lowerChunkIndexToFetch = 0;
            lowerChunkIndexToFetch < std::ssize(hunkChunkIds);
            lowerChunkIndexToFetch += Config_->MaxHunkChunkCountPerFetch)
        {
            fetchChunkBatch(
                lowerChunkIndexToFetch,
                std::min(
                    lowerChunkIndexToFetch + Config_->MaxHunkChunkCountPerFetch,
                    std::ssize(hunkChunkIds)));
        }

        YT_TLOG_DEBUG("Finished fetching hunk chunk seal state")
            .With("HunkChunkCount", hunkChunkIds.size())
            .With("UnsealedHunkChunkCount", unsealedHunkChunkIds.size());

        return unsealedHunkChunkIds;
    }

    // For each time segment we find all its corresponding hunk chunks and filter out
    // all the time segments starting from the one that contains unsealed hunk chunk (if present),
    // because journal hunk chunks must be sealed before being exported.
    void FilterChunkSpecsByHunkChunkSealState()
    {
        std::vector<TChunkId> allHunkChunkIds;
        THashMap<ui64, std::vector<TChunkId>> hunkChunkIdsByUnixTs;

        for (const auto& [exportUnixTs, chunkSpecs] : ChunkSpecsToExportByUnixTs_) {
            auto& currentHunkChunkIds = hunkChunkIdsByUnixTs[exportUnixTs];
            for (const auto* chunkSpec : chunkSpecs) {
                if (auto hunkChunkRefsExt = FindProtoExtension<THunkChunkRefsExt>(chunkSpec->chunk_meta().extensions())) {
                    for (const auto& ref : hunkChunkRefsExt->refs()) {
                        currentHunkChunkIds.push_back(FromProto<TChunkId>(ref.chunk_id()));
                    }
                }
            }

            SortUnique(currentHunkChunkIds);
            allHunkChunkIds.insert(allHunkChunkIds.end(), currentHunkChunkIds.begin(), currentHunkChunkIds.end());
        }

        SortUnique(allHunkChunkIds);

        if (allHunkChunkIds.empty()) {
            return;
        }

        auto unsealedHunkChunkIds = FetchUnsealedHunkChunkIds(allHunkChunkIds);

        for (auto chunkSpecsIt = ChunkSpecsToExportByUnixTs_.begin();
            chunkSpecsIt != ChunkSpecsToExportByUnixTs_.end();
            ++chunkSpecsIt)
        {
            auto exportUnixTs = chunkSpecsIt->first;
            auto& hunkChunkIds = GetOrCrash(hunkChunkIdsByUnixTs, exportUnixTs);

            for (auto hunkChunkId : hunkChunkIds) {
                if (unsealedHunkChunkIds.contains(hunkChunkId)) {
                    auto skippedGroupCount = std::distance(chunkSpecsIt, ChunkSpecsToExportByUnixTs_.end());

                    YT_TLOG_DEBUG("Stopped selecting chunks for export due to unsealed hunk chunks")
                        .With("ExportUnixTs", exportUnixTs)
                        .With("UnsealedHunkChunkId", hunkChunkId)
                        .With("SkippedGroupCount", skippedGroupCount)
                        .With("TotalGroupCount", ChunkSpecsToExportByUnixTs_.size());

                    // TODO(akozhikhov): Consider erasing only the suffix of the chunk spec list that has a reference to
                    // unsealed journal chunk. This can potentially speed up exporing of the data, however the degree of
                    // such speed up is not obvious and shall be researched relying on actual use cases.
                    ChunkSpecsToExportByUnixTs_.erase(chunkSpecsIt, ChunkSpecsToExportByUnixTs_.end());
                    return;
                }
            }

            if (!hunkChunkIds.empty()) {
                EmplaceOrCrash(HunkChunkIdsToExportByUnixTs_, exportUnixTs, std::move(hunkChunkIds));
            }
        }
    }

    //! Finds the first chunk spec of a tablet that has to be exported, i.e. the one right after the last exported chunk.
    //! If there is no such chunk in the queue, the tablet is exported from the beginning.
    TChunkSpecPtrs::const_iterator FindFirstChunkSpecToExport(
        i64 tabletIndex,
        const TChunkSpecPtrs& chunkSpecs)
    {
        auto lastExportedChunkId = GetOrDefault(LastExportedChunkIds_, tabletIndex, NullChunkId);
        auto lastExportedSpecIt = std::find_if(chunkSpecs.begin(), chunkSpecs.end(), [&] (const auto* chunkSpec) {
            return FromProto<TChunkId>(chunkSpec->chunk_id()) == lastExportedChunkId;
        });

        if (lastExportedSpecIt == chunkSpecs.end()) {
            return chunkSpecs.begin();
        }

        // NB: The start offset is only used to verify the bookkeeping of the caller.
        auto startOffset = GetOrDefault(StartOffsets_, tabletIndex, 0);
        const auto& lastExportedChunkSpec = **lastExportedSpecIt;
        if (GetChunkEndOffset(lastExportedChunkSpec) != startOffset) {
            auto error = TError("Mismatch between last chunk from tablet progress and row count from progress")
                .With("tablet_index", tabletIndex)
                .With("tablet_progress_last_chunk", lastExportedChunkId)
                .With("tablet_progress_row_count", startOffset)
                .With("found_chunk_row_index", lastExportedChunkSpec.table_row_index())
                .With("found_chunk_row_count", GetChunkRowCount(lastExportedChunkSpec));
            YT_TLOG_WARNING("Last exported chunk does not end at the start offset")
                .With(error);
            Result_.TabletOffsetMismatchErrors.push_back(std::move(error));
        }

        return lastExportedSpecIt + 1;
    }

    void SelectChunkSpecsToExport()
    {
        std::map<i64, std::vector<const TChunkSpec*>> tabletToChunkSpecs;
        for (const auto& chunkSpec : ChunkSpecs_) {
            tabletToChunkSpecs[chunkSpec.tablet_index()].push_back(&chunkSpec);
        }

        for (const auto& [tabletIndex, chunkSpecs] : tabletToChunkSpecs) {
            auto specToExportIt = FindFirstChunkSpecToExport(tabletIndex, chunkSpecs);

            // NB(apachee): Monotonically increasing export unix ts to provide intra-tablet chunk order.
            // It is required in case of weak commit ordering, as in this case latter chunks might have smaller max timestamps.
            ui64 accumulatedMinExportUnixTs = FirstExportUnixTs_;

            for (; specToExportIt != chunkSpecs.end(); ++specToExportIt) {
                auto* chunkSpec = *specToExportIt;
                // TODO(achulkov2): Get rid of this allocation?
                TInputChunkPtr chunk = New<TInputChunk>(*chunkSpec);
                // NB: This is guaranteed by setting omit_dynamic_stores(true) while fetching.
                YT_VERIFY(!chunk->IsDynamicStore());

                auto chunkFormat = FromProto<EChunkFormat>(chunkSpec->chunk_meta().format());
                ValidateTableChunkFormatVersioned(chunkFormat, /*versioned*/ false);

                auto miscExt = GetProtoExtension<TMiscExt>(chunkSpec->chunk_meta().extensions());
                auto maxTimestamp = FromProto<TTimestamp>(miscExt.max_timestamp());
                accumulatedMinExportUnixTs = std::max(accumulatedMinExportUnixTs, GetMinExportUnixTs(maxTimestamp));
                // We only export chunks which are compatible with the export unix ts upper bound.
                if (accumulatedMinExportUnixTs > ExportUnixTsUpperBound_) {
                    // NB: Latter chunks might have smaller max timestamps in case of weak commit ordering, but we do
                    // not export any of them to maintain intra-tablet chunk order within the exported tables.
                    break;
                }

                ChunkSpecsToExportByUnixTs_[accumulatedMinExportUnixTs].push_back(chunkSpec);
            }
        }

        if (QueueSchema_->HasHunkColumns()) {
            FilterChunkSpecsByHunkChunkSealState();
        }
    }

    void FetchChunkSpecs()
    {
        YT_TLOG_DEBUG("Started fetching chunk specs")
            .With("Count", QueueObject_.ChunkCount);

        auto prepareFetchRequest = [&] (const TChunkOwnerYPathProxy::TReqFetchPtr& request, int /*index*/) {
            request->add_extension_tags(TProtoExtensionTag<TMiscExt>::Value);
            request->add_extension_tags(TProtoExtensionTag<THunkChunkRefsExt>::Value);
            request->set_omit_dynamic_stores(true);
            SetTransactionId(request, *QueueObject_.TransactionId);
        };

        auto chunkSpecFetcher = New<TMasterChunkSpecFetcher>(
            Client_,
            Connection_->GetNodeDirectory(),
            Connection_->GetInvoker(),
            TMasterChunkSpecFetcherOptions{
                .MaxChunksPerFetch = Connection_->GetConfig()->MaxChunksPerFetch,
                .MaxChunksPerLocateRequest = Connection_->GetConfig()->MaxChunksPerLocateRequest,
                .FetchRequestInitializer = prepareFetchRequest,
            },
            Logger);

        chunkSpecFetcher->Add(
            QueueObject_.ObjectId,
            QueueObject_.ExternalCellTag,
            // XXX(achulkov2, gritukan): YT-11825
            /*chunkCount*/ -1);

        WaitFor(chunkSpecFetcher->Fetch())
            .ThrowOnError();

        ChunkSpecs_ = chunkSpecFetcher->GetChunkSpecsOrderedNaturally();
        YT_TLOG_DEBUG("Finished fetching chunk specs")
            .With("Count", ChunkSpecs_.size());
    }

    std::string GetOutputTableName(ui64 unixTs)
    {
        auto periodInSeconds = GetLastExportPeriod(unixTs, Schedule_);

        if (!Options_.UseUpperBoundForTableNames) {
            unixTs -= periodInSeconds;
        }

        auto instant = TInstant::Seconds(unixTs);

        auto outputTableName = Options_.OutputTableNamePattern;

        std::vector<std::pair<std::string, std::string>> variables = {
            {"%UNIX_TS", ToString(unixTs)},
            {"%PERIOD", ToString(periodInSeconds)},
            {"%ISO", instant.ToStringUpToSeconds()},
        };

        // Replace all occurrences of variables with their values.
        for (const auto& [variable, value] : variables) {
            for (size_t position = 0; (position = outputTableName.find(variable, position)) != std::string::npos; ) {
                outputTableName.replace(position, variable.length(), value);
            }
        }

        return instant.FormatGmTime(outputTableName.c_str());
    }

    void CreateOutputTable(TTaskPart& taskPart)
    {
        auto destinationPath = Format(
            "%s/%v",
            ExportDirectory_,
            taskPart.TableName);
        taskPart.DestinationObject = TUserObject(destinationPath, taskPart.TransactionOptions.TransactionId);

        TCreateNodeOptions createOptions;
        createOptions.TransactionId = taskPart.TransactionOptions.TransactionId;
        createOptions.Attributes = CreateEphemeralAttributes();
        if (Options_.ExportTtl) {
            // NB(apachee): We use task part export ts to calculate expiration time for easier #ExportTtl logic. This may potentially
            // lead to exported table being deleted right away leading to some overhead for queue agent and master, but this should
            // rarely be the case, so we ignore it at this moment.
            // NB(apachee): For TInstant <-> Unix Time conversion clarification see the comment at the top.
            // TODO(apachee): Implement exported table skipping mechanics to further reduce queue agent and master load.
            createOptions.Attributes->Set("expiration_time", TInstant::Seconds(taskPart.ExportUnixTs) + Options_.ExportTtl);
        }
        if (QueueSchema_->HasHunkColumns()) {
            // NB: Teleportation of (journal) hunk chunks is not allowed.
            createOptions.Attributes->Set("external_cell_tag", QueueObject_.ExternalCellTag);
            createOptions.Attributes->Set("has_hunk_chunk_list", true);
        }
        try {
            WaitFor(Client_->CreateNode(taskPart.DestinationObject.GetPath(), EObjectType::Table, createOptions))
                .ThrowOnError();
        } catch (const TErrorException& ex) {
            if (ex.Error().FindMatching(NYTree::EErrorCode::AlreadyExists)) {
                // NB(apachee): Enrich exported table already exists error with more details and possible solutions.
                THROW_ERROR_EXCEPTION(
                    "Generated output table name uniqueness invariant violated: output table name pattern should be unique for each export unix ts, "
                    "you may need to adjust (usually increase) export period or export cron schedule to match your output table name pattern")
                    .With("output_table_name_pattern", Options_.OutputTableNamePattern)
                    .With("schedule", ToString(Schedule_))
                    .With("export_unix_ts", taskPart.ExportUnixTs)
                    .With(ex);
            } else {
                throw;
            }
        }

        YT_TLOG_DEBUG("Created output node for export")
            .With("DestinationPath", taskPart.DestinationObject.GetPath())
            .With("OutputTableNamePattern", Options_.OutputTableNamePattern)
            .With("UseUpperBoundForTableNames", Options_.UseUpperBoundForTableNames)
            .With("ExportTtl", Options_.ExportTtl)
            .With("ExportUnixTs", taskPart.ExportUnixTs);

        GetAndFillBasicAttributes(Logger, Client_, taskPart.DestinationObject, /*populateSecurityTags*/ false);
    }

    TCellTagList GetAffectedCellTags(
        const TTaskPart& taskPart,
        const std::optional<TCellTag> cellTagToExclude)
    {
        THashSet<TCellTag> cellTags;

        auto populateCellTags = [&] (TChunkId chunkId) {
            auto cellTag = CellTagFromId(chunkId);
            cellTags.insert(cellTag);
        };

        for (const auto& chunkSpec : taskPart.ChunkSpecsToExport) {
            auto chunkId = FromProto<TChunkId>(chunkSpec->chunk_id());
            populateCellTags(chunkId);
        }
        // NB: This is done just to ensure later on that all the chunks (including hunk ones) actually reside
        // on the destitaion cell as we are not allowing teleporation in case of presence of (journal) hunk chunks.
        if (taskPart.HunkChunkIdsToExport) {
            for (const auto& hunkChunkId : *taskPart.HunkChunkIdsToExport) {
                populateCellTags(hunkChunkId);
            }
        }

        cellTags.insert(taskPart.DestinationObject.ExternalCellTag);

        if (cellTagToExclude) {
            cellTags.erase(*cellTagToExclude);
        }

        return {cellTags.begin(), cellTags.end()};
    }

    void BeginUpload(TTaskPart& taskPart)
    {
        auto destinationObjectCellTag = CellTagFromId(taskPart.DestinationObject.ObjectId);

        auto cellTags = GetAffectedCellTags(
            taskPart,
            /*cellTagToExclude*/ destinationObjectCellTag);

        if (QueueSchema_->HasHunkColumns() &&
            !cellTags.empty() &&
            (cellTags.size() != 1 || cellTags[0] != taskPart.DestinationObject.ExternalCellTag))
        {
            THROW_ERROR_EXCEPTION("Cannot perform cross-cell export of a queue that contains hunks")
                .With("queue_object_cell_tag", QueueObject_.ExternalCellTag)
                .With("destination_object_cell_tag", destinationObjectCellTag)
                .With("destination_object_external_cell_tag", taskPart.DestinationObject.ExternalCellTag)
                .With("affected_cell_tags", ToString(cellTags));
        }

        auto proxy = CreateObjectServiceWriteProxy(Client_, destinationObjectCellTag);

        auto req = TChunkOwnerYPathProxy::BeginUpload(taskPart.DestinationObject.GetObjectIdPath());
        req->set_update_mode(ToProto(EUpdateMode::Overwrite));
        req->set_lock_mode(ToProto(ELockMode::Exclusive));
        if (CanUseSchemaId(taskPart)) {
            ToProto(req->mutable_table_schema_id(), QueueSchemaId_);
        } else {
            ToProto(req->mutable_table_schema(), QueueSchema_);
        }
        req->set_schema_mode(ToProto(ETableSchemaMode::Strong));

        req->set_upload_transaction_title(Format(
            "Exporting queue %v to static table %v",
            Queue_,
            taskPart.DestinationObject.GetPath()));

        ToProto(req->mutable_upload_transaction_secondary_cell_tags(), cellTags);
        req->set_upload_transaction_timeout(
            ToProto(Connection_->GetConfig()->UploadTransactionTimeout));
        GenerateMutationId(req);

        SetTransactionId(req, taskPart.TransactionOptions.TransactionId);

        auto rspOrError = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(
            rspOrError, "Error starting upload to %v", Queue_);

        const auto& rsp = rspOrError.Value();

        auto uploadTransactionId = FromProto<TTransactionId>(rsp->upload_transaction_id());

        TTransactionAttachOptions attachOptions = {};
        attachOptions.AutoAbort = true;
        attachOptions.Ping = true;
        attachOptions.PingAncestors = true;
        attachOptions.PingPeriod = Connection_->GetConfig()->UploadTransactionPingPeriod;
        taskPart.UploadTransaction = Client_->GetTransactionManager()->Attach(
            uploadTransactionId,
            attachOptions);

        YT_TLOG_DEBUG("Started upload transaction for queue export")
            .With("DestinationPath", taskPart.DestinationObject.GetPath())
            .With("UploadTransactionId", taskPart.UploadTransaction->GetId())
            .With("OutputTableSchemaId", QueueSchemaId_);
    }

    void TeleportChunkMeta(const TTaskPart& taskPart)
    {
        YT_VERIFY(taskPart.UploadTransaction);

        auto teleporter = New<TChunkTeleporter>(
            Client_->GetNativeConnection()->GetConfig(),
            Client_,
            Client_->GetNativeConnection()->GetInvoker(),
            taskPart.UploadTransaction->GetId(),
            Logger);

        for (const auto* chunkSpec : taskPart.ChunkSpecsToExport) {
            teleporter->RegisterChunk(
                FromProto<TChunkId>(chunkSpec->chunk_id()),
                taskPart.DestinationObject.ExternalCellTag);
        }
        WaitFor(teleporter->Run())
            .ThrowOnError();
    }

    TEnumIndexedArray<EChunkListContentType, TChunkListId> GetChunkListIds(const TTaskPart& taskPart)
    {
        YT_VERIFY(taskPart.UploadTransaction);

        auto proxy = CreateObjectServiceWriteProxy(Client_, taskPart.DestinationObject.ExternalCellTag);
        auto req = TChunkOwnerYPathProxy::GetUploadParams(taskPart.DestinationObject.GetObjectIdPath());
        SetTransactionId(req, taskPart.UploadTransaction->GetId());
        if (taskPart.ShouldExportHunkChunks()) {
            req->set_fetch_hunk_chunk_list_id(true);
        }

        auto rspOrError = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(
            rspOrError, "Error requesting upload parameters for %v", taskPart.DestinationObject.GetPath());

        const auto& rsp = rspOrError.Value();
        TEnumIndexedArray<EChunkListContentType, TChunkListId> result;
        result[EChunkListContentType::Main] = FromProto<TChunkListId>(rsp->chunk_list_id());
        if (taskPart.ShouldExportHunkChunks()) {
            if (!rsp->has_hunk_chunk_list_id()) {
                THROW_ERROR_EXCEPTION("Hunk chunk list id was not received");
            }

            result[EChunkListContentType::Hunk] = FromProto<TChunkListId>(rsp->hunk_chunk_list_id());
        }

        YT_TLOG_DEBUG("Fetched chunk list ids")
            .With("FetchResult", result);

        return result;
    }

    void AttachChunks(TTaskPart& taskPart)
    {
        YT_VERIFY(taskPart.UploadTransaction);

        YT_TLOG_DEBUG("Started chunk upload")
            .With("DestinationPath", taskPart.DestinationObject.GetPath())
            .With("UploadTransactionId", taskPart.UploadTransaction->GetId())
            .With("ChunkCount", taskPart.ChunkSpecsToExport.size());

        TChunkServiceProxy proxy(Client_->GetMasterChannelOrThrow(
            EMasterChannelKind::Leader,
            taskPart.DestinationObject.ExternalCellTag));

        auto chunkListIds = GetChunkListIds(taskPart);

        auto mainReq = proxy.AttachChunkTrees();
        GenerateMutationId(mainReq);
        SetSuppressUpstreamSync(&mainReq->Header(), true);

        ToProto(mainReq->mutable_parent_id(), chunkListIds[EChunkListContentType::Main]);
        for (const auto* chunkSpec : taskPart.ChunkSpecsToExport) {
            *mainReq->add_child_ids() = chunkSpec->chunk_id();
        }
        mainReq->set_request_statistics(true);

        std::vector<TFuture<TChunkServiceProxy::TRspAttachChunkTreesPtr>> reqFutures;
        reqFutures.push_back(mainReq->Invoke());

        if (taskPart.ShouldExportHunkChunks()) {
            auto hunkReq = proxy.AttachChunkTrees();
            GenerateMutationId(hunkReq);
            SetSuppressUpstreamSync(&hunkReq->Header(), true);

            ToProto(hunkReq->mutable_parent_id(), chunkListIds[EChunkListContentType::Hunk]);
            for (auto hunkChunkId : *taskPart.HunkChunkIdsToExport) {
                ToProto(hunkReq->add_child_ids(), hunkChunkId);
            }
            hunkReq->set_request_statistics(true);

            reqFutures.push_back(hunkReq->Invoke());
        }

        auto responsesOrError = WaitFor(AllSucceeded(std::move(reqFutures)));
        THROW_ERROR_EXCEPTION_IF_FAILED(
            responsesOrError,
            "Error attaching chunks to %v",
            taskPart.DestinationObject.GetPath());

        taskPart.DataStatistics = {};
        for (const auto& response : responsesOrError.Value()) {
            taskPart.DataStatistics += response->statistics();
        }

        YT_TLOG_DEBUG("Finished chunk upload")
            .With("DestinationPath", taskPart.DestinationObject.GetPath())
            .With("UploadTransactionId", taskPart.UploadTransaction->GetId())
            .With("ChunkCount", taskPart.ChunkSpecsToExport.size());
    }

    void EndUpload(const TTaskPart& taskPart)
    {
        YT_VERIFY(taskPart.UploadTransaction);

        auto proxy = CreateObjectServiceWriteProxy(Client_, CellTagFromId(taskPart.DestinationObject.ObjectId));

        auto req = TChunkOwnerYPathProxy::EndUpload(taskPart.DestinationObject.GetObjectIdPath());
        *req->mutable_statistics() = taskPart.DataStatistics;

        std::vector<TSecurityTag> inferredSecurityTags;
        inferredSecurityTags.insert(
            inferredSecurityTags.end(),
            QueueObject_.SecurityTags.begin(),
            QueueObject_.SecurityTags.end());
        SortUnique(inferredSecurityTags);

        auto securityTags = taskPart.DestinationObject.Path.GetSecurityTags().value_or(inferredSecurityTags);

        ToProto(req->mutable_security_tags()->mutable_items(), securityTags);
        SetTransactionId(req, taskPart.UploadTransaction->GetId());
        GenerateMutationId(req);

        auto rspOrError = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(
            rspOrError, "Error ending upload to %v", taskPart.DestinationObject.GetPath());

        taskPart.UploadTransaction->Detach();
    }

    bool CanUseSchemaId(const TTaskPart& taskPart) const
    {
        return CellTagFromId(QueueObject_.ObjectId) == CellTagFromId(taskPart.DestinationObject.ObjectId);
    }
};

DEFINE_REFCOUNTED_TYPE(TQueueExportSession)

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TQueueExportRunner::TQueueExportRunner(TQueueExportRunnerConfigPtr config)
    : Config_(std::move(config))
{ }

TFuture<TExportQueueIntoStaticTableResult> TQueueExportRunner::ExportQueueIntoStaticTable(
    NNative::IClientPtr client,
    TYPath queuePath,
    TYPath exportDirectory,
    THashMap<i64, TChunkId> lastExportedChunkIds,
    THashMap<i64, i64> startOffsets,
    TTimestamp startTimestamp,
    TQueueExportSchedule schedule,
    TExportQueueIntoStaticTableOptions options,
    TLogger logger,
    IInvokerPtr invoker) const
{
    auto sessionLogger = logger
        .WithTag("QueuePath", queuePath)
        .WithTag("ExportDirectory", exportDirectory);
    auto session = New<TQueueExportSession>(
        std::move(client),
        std::move(invoker),
        std::move(queuePath),
        std::move(exportDirectory),
        std::move(lastExportedChunkIds),
        std::move(startOffsets),
        startTimestamp,
        std::move(schedule),
        std::move(options),
        Config_,
        std::move(sessionLogger));
    return session->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NInternalProxy
