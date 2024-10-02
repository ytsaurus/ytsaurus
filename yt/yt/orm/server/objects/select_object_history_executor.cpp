#include "select_object_history_executor.h"

#include "attribute_matcher.h"
#include "batch_size_backoff.h"
#include "db_config.h"
#include "fetchers.h"
#include "history_query.h"
#include "history_manager.h"
#include "object.h"
#include "object_manager.h"
#include "object_reflection.h"
#include "query_executor_helpers.h"

#include <yt/yt/orm/server/access_control/access_control_manager.h>

#include <yt/yt/orm/server/master/bootstrap.h>
#include <yt/yt/orm/server/master/config.h>

#include <yt/yt/orm/library/attributes/merge_attributes.h>

#include <yt/yt/orm/library/mpl/projection.h>

#include <yt/yt/client/table_client/unversioned_value.h>
#include <yt/yt/client/table_client/unversioned_value.h>

#include <library/cpp/iterator/zip.h>

#include <functional>

namespace NYT::NOrm::NServer::NObjects {

using namespace NYT::NApi;
using namespace NYT::NTableClient;
using namespace NYT::NYson;
using namespace NYT::NQueryClient::NAst;

using namespace NAccessControl;

using NYT::NQueryClient::TSourceLocation;

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TSelectObjectHistoryOptions& options,
    TStringBuf /*format*/)
{
    TDelimitedStringBuilderWrapper wrapper(builder);

    builder->AppendChar('{');
    wrapper->AppendFormat("Uuid: %v", options.Uuid);
    wrapper->AppendFormat("Limit: %v", options.Limit);
    wrapper->AppendFormat("TimeInterval: %v", options.TimeInterval);
    wrapper->AppendFormat("DescendingTimeOrder: %v", options.DescendingTimeOrder);
    wrapper->AppendFormat("FetchRootObject: %v", options.FetchRootObject);
    wrapper->AppendFormat("IndexMode: %v", options.IndexMode);
    wrapper->AppendFormat("AllowTimeModeConversion: %v", options.AllowTimeModeConversion);
    wrapper->AppendFormat("Filter: %v", options.Filter);
    builder->AppendChar('}');
}

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

void FillContinuationToken(
    TSelectObjectHistoryContinuationToken* continuationToken,
    const TSelectObjectHistoryContinuationTokenSchema& tokenSchema,
    EHistoryReadPhase phase,
    std::vector<TYsonString>& parsedKeys)
{
    // TODO(grigminakov): Optimize entries initialization.
    continuationToken->EvaluatedEntries.resize(tokenSchema.Entries.size());
    for (int i = 0; i < std::ssize(tokenSchema.Entries); ++i) {
        continuationToken->EvaluatedEntries[i].AsNotEvaluated() = tokenSchema.Entries[i];
    }

    YT_VERIFY(tokenSchema.Entries.size() == continuationToken->EvaluatedEntries.size());
    YT_VERIFY(parsedKeys.size() == tokenSchema.Entries.size());

    // Update continuation token.
    for (int i = 0; i < std::ssize(tokenSchema.Entries); ++i) {
        continuationToken->EvaluatedEntries[i].YsonValue = std::move(parsedKeys[i]);
    }

    continuationToken->ReadSource = tokenSchema.ReadSource->GetName();
    continuationToken->ReadPhase = phase;
}

////////////////////////////////////////////////////////////////////////////////

TSelectObjectHistoryOptions PreprocessOptions(TSelectObjectHistoryOptions options, int configuredLimit)
{
    if (!options.Limit) {
        options.Limit = configuredLimit - 1;
    } else if (options.Limit > configuredLimit) {
        THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidRequestArguments,
            "The number of requested events exceeds the limit: %v > %v",
            options.Limit,
            configuredLimit);
    } else if (options.Limit < 0) {
        THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidRequestArguments,
            "Expected non-negative number of requested events, but found %v",
            options.Limit);
    }
    return options;
}

TAttributeSelector PreprocessAttributeSelector(TAttributeSelector selector, bool fetchRootObject)
{
    if (fetchRootObject) {
        NAttributes::SortAndRemoveNestedPaths(selector.Paths);
    }

    return selector;
}

////////////////////////////////////////////////////////////////////////////////

class TSelectObjectHistoryExecutor
   : public ISelectObjectHistoryExecutor
   , public TQueryExecutorBase
{
public:
    TSelectObjectHistoryExecutor(
        TTransactionPtr transaction,
        TObjectTypeValue objectType,
        const TObjectKey& objectKey,
        const TAttributeSelector& attributeSelector,
        const std::optional<TAttributeSelector>& distinctBySelector,
        std::optional<TSelectObjectHistoryContinuationToken> continuationToken,
        TSelectObjectHistoryOptions options)
        : TQueryExecutorBase(transaction)
        , HistoryManager_(Bootstrap_->MakeHistoryManager(transaction.Get()))
        , ObjectType_(objectType)
        , ObjectKey_(objectKey)
        , AttributeSelector_(PreprocessAttributeSelector(attributeSelector, options.FetchRootObject))
        , DistinctBySelector_(distinctBySelector)
        , Config_(transaction->GetConfig()->SelectObjectHistory)
        , ContinuationToken_(std::move(continuationToken))
        , Options_(PreprocessOptions(std::move(options), Config_->OutputEventLimit))
        , Collector_(New<THistoryEventCollector>(
            /*distinct*/ DistinctBySelector_.has_value(),
            /*onEvent*/ [this] (THistoryEvent event) {
                Result_.Events.push_back(std::move(event));
            },
            /*limit*/ Options_.Limit))
    { }

    TSelectObjectHistoryResult Execute() && override
    {
        if (ContinuationToken_) {
            ValidateSelectContinuationTokenVersion(*ContinuationToken_, Bootstrap_);
        } else {
            // Provided continuation token with an empty evaluated order is possible here.
            InitializeSelectContinuationTokenVersion(ContinuationToken_.emplace(), Bootstrap_);
            ContinuationToken_->ReadPhase = EHistoryReadPhase::Uninitialized;
        }

        const auto& configsSnapshot = Transaction_->GetSession()->GetConfigsSnapshot();

        bool useIndex = CanQueryHistoryIndex();
        const auto* historyIndexTable = HistoryManager_->GetReadTable(*ContinuationToken_);
        if (Options_.Uuid) {
            THROW_ERROR_EXCEPTION_IF(!historyIndexTable->UseUuidInKey && !Transaction_->FullScanAllowed(),
                "Uuid is not present in history table key, but passed in select history request: "
                "query would be inefficient");
        }

        EnsureTimeMode(
            &Options_.TimeInterval,
            HistoryManager_->ForceAllowTimeModeConversion() || Options_.AllowTimeModeConversion,
            historyIndexTable->TimeMode);

        auto lastTrimTime = configsSnapshot.ObjectManager->GetHistoryLastTrimTime(
            historyIndexTable->PrimaryTable->GetName());
        if (HasValue(lastTrimTime)) {
            Result_.LastTrimTime = std::get<TInstant>(lastTrimTime);
        }
        if (useIndex) {
            lastTrimTime = std::max(
                lastTrimTime,
                configsSnapshot.ObjectManager->GetHistoryLastTrimTime(historyIndexTable->GetName()));
        }
        auto adjustedLastTrimTime = lastTrimTime;
        EnsureTimeMode(&adjustedLastTrimTime, /*allowTimeModeConversion*/ true, historyIndexTable->TimeMode);

        if ((HasValue(Options_.TimeInterval.Begin) && Options_.TimeInterval.Begin < adjustedLastTrimTime) ||
            (HasValue(Options_.TimeInterval.End) && Options_.TimeInterval.End < adjustedLastTrimTime))
        {
            THROW_ERROR_EXCEPTION(NClient::EErrorCode::RowsAlreadyTrimmed,
                "Requested history events have already been trimmed")
                << TErrorAttribute("last_trim_time", std::get<TInstant>(lastTrimTime))
                << TErrorAttribute("index_used", useIndex);
        }
        if (HasValue(adjustedLastTrimTime) && !HasValue(Options_.TimeInterval.Begin)) {
            Options_.TimeInterval.Begin = adjustedLastTrimTime;
        }

        bool needToQueryFirstEventWithinInterval = useIndex && HasValue(Options_.TimeInterval.Begin);
        Result_.IndexUsed = useIndex;

        TBatchSizeBackoff batchSize(Logger, Config_, /*initialBatchSize*/ Options_.Limit);
        batchSize.RestrictLimit(Config_->OutputEventLimit - 1);
        if (Options_.Limit && DistinctBySelector_) {
            batchSize.Next();

            // When using index, there is an upper bound to the number of events that need to be read:
            // An update event may be duplicated for each of the requested selectors.
            if (useIndex && !AttributeSelector_.Paths.empty()) {
                batchSize.RestrictLimit(static_cast<i64>(Options_.Limit * AttributeSelector_.Paths.size() + 1));
            }
        }

        // When reading with index and time interval, first event may be missed.
        //
        // Index contains only the first event within each group of equal `object_updated` events.
        // If time interval does not intersect with the first event, the group will be totally missed.
        // Therefore, query the first event intersecting the time interval from history table directly to handle the issue.
        //
        // E.g. requested attribute changed at time 5 and events at time 6 and 8 later did not change this attribute.
        // If we request events with `Distinct` and TimeInterval.Begin = 6,
        // we will get [] by using index, or [{time: 6, ...}] without it,
        // because index contains only event `object_updated` with time = 5,
        // which does not intersect with the given time interval.
        // This option queries first event without index, so that we get complete list of events.
        //
        // NB: Applied both to ascending(first event processed) and descending(last event processed) time orders.
        auto tryQueryFirstEventWithinInterval = [this, historyIndexTable] (EHistoryReadPhase phase) {
            YT_VERIFY(phase == EHistoryReadPhase::IndexAscendingBorder || phase == EHistoryReadPhase::IndexDescendingBorder);
            auto firstEventOptions_ = Options_;
            firstEventOptions_.DescendingTimeOrder = false;
            auto [query, customTokenSchema] = BuildQuery(
                phase,
                firstEventOptions_,
                historyIndexTable,
                /*limit*/ 1,
                /*useIndex*/ false);
            YT_LOG_DEBUG(
                "Selecting first event in object history within time interval ("
                "Query: %v, "
                "TimeInterval: %v, "
                "DescendingTimeOrder: %v)",
                query,
                firstEventOptions_.TimeInterval,
                Options_.DescendingTimeOrder);
            auto rowset = RunSelect(query);
            YT_VERIFY(rowset->GetRows().Size() <= 1);
            CollectRows(
                customTokenSchema,
                /*forwardContinuationToken*/ false,
                phase,
                std::move(rowset),
                /*fromIndex*/ false);
        };

        if (auto currentPhase = EHistoryReadPhase::IndexAscendingBorder; currentPhase > ContinuationToken_->ReadPhase)
        {
            if (needToQueryFirstEventWithinInterval &&
                !Options_.DescendingTimeOrder)
            {
                tryQueryFirstEventWithinInterval(currentPhase);
            }
        }

        if (auto currentPhase = EHistoryReadPhase::Main;
            EHistoryReadPhase::IndexDescendingBorder >= ContinuationToken_->ReadPhase)
        {
            while (!Collector_->IsEnough()) {
                auto [query, customTokenSchema] = BuildQuery(
                    currentPhase,
                    Options_,
                    historyIndexTable,
                    *batchSize,
                    useIndex);
                YT_LOG_DEBUG("Selecting object history (Query: %v)",
                    query);

                IUnversionedRowsetPtr rowset = nullptr;
                try {
                    rowset = RunSelect(std::move(query));
                } catch (const std::exception& ex) {

                    // Let the user adjust event count limit.
                    if (*batchSize <= Options_.Limit) {
                        THROW_ERROR_EXCEPTION(NClient::EErrorCode::LimitTooLarge,
                            "Error selecting object history, try to adjust event count limit")
                            << ex;
                    }

                    YT_LOG_DEBUG(ex, "Error selecting object history; decreasing batch size");
                    batchSize.Rollback();
                    continue;
                }

                auto collectedRowCount = CollectRows(
                    customTokenSchema,
                    /*forwardContinuationToken*/ true,
                    currentPhase,
                    rowset,
                    useIndex);
                if (collectedRowCount < *batchSize) {
                    break;
                }

                batchSize.Next();
            }
        }

        if (auto currentPhase = EHistoryReadPhase::IndexDescendingBorder; currentPhase > ContinuationToken_->ReadPhase)
        {
            if (needToQueryFirstEventWithinInterval && Options_.DescendingTimeOrder && !Collector_->IsEnough()) {
                // NB: Continuation token may be updated here if querying first event succeeds.
                tryQueryFirstEventWithinInterval(currentPhase);
            }
        }

        Collector_->Flush();

        if (static_cast<i64>(Result_.Events.size()) > Config_->OutputEventLimit) {
            THROW_ERROR_EXCEPTION("Too many events in response, expected no more than %v",
                Config_->OutputEventLimit);
        }

        YT_VERIFY(ContinuationToken_);
        Result_.ContinuationToken = std::move(*ContinuationToken_);

        return Result_;
    }

private:
    const IHistoryManagerPtr HistoryManager_;
    const TObjectTypeValue ObjectType_;
    const TObjectKey& ObjectKey_;
    const TAttributeSelector AttributeSelector_;
    const std::optional<TAttributeSelector>& DistinctBySelector_;
    const TSelectObjectHistoryConfigPtr Config_;
    const std::optional<TSelectObjectHistoryContinuationToken> NullToken_ = std::nullopt;
    std::optional<TSelectObjectHistoryContinuationToken> ContinuationToken_;
    TSelectObjectHistoryOptions Options_;

    TSelectObjectHistoryResult Result_;
    THistoryEventCollectorPtr Collector_;

    std::pair<TString, TSelectObjectHistoryContinuationTokenSchema> BuildQuery(
        EHistoryReadPhase phase,
        const TSelectObjectHistoryOptions& options,
        const THistoryIndexTable* historyIndexTable,
        int limit,
        bool useIndex)
    {
        return BuildHistoryQuery(
            Bootstrap_,
            historyIndexTable,
            ObjectType_,
            ObjectKey_,
            DistinctBySelector_,
            (ContinuationToken_->ReadPhase >= phase) ? ContinuationToken_ : NullToken_,
            limit,
            options,
            useIndex,
            Config_->RestrictHistoryFilter);
    }

    THistoryEvent ParseRow(
        const TSelectObjectHistoryContinuationTokenSchema& tokenSchema,
        TUnversionedRow row,
        bool fromIndex) const
    {
        // Index has no column for history enabled attributes: they are contained in `etc`.
        bool historyEnabledAttributePathsField = !fromIndex;
        auto event = ParseHistoryEvent(
            row,
            /*startingIndex*/ tokenSchema.Entries.size(),
            AttributeSelector_,
            DistinctBySelector_,
            Bootstrap_->GetObjectManager()->AreHistoryEnabledAttributePathsEnabled(),
            historyEnabledAttributePathsField,
            tokenSchema.ReadSource->TimeMode,
            /*invertTime*/ !tokenSchema.ReadSource->OptimizeForAscendingTime,
            /*positiveEventTypes*/ tokenSchema.ReadSource->UsePositiveEventTypes);

        if (Options_.FetchRootObject &&
            (AttributeSelector_.Paths.empty() || !AttributeSelector_.Paths[0].empty()))
        {
            NAttributes::ValidateSortedPaths(
                AttributeSelector_.Paths,
                /*pathProj*/ std::identity{},
                /*isEtcProj*/ NMpl::TConstantProjection<bool, false>{});

            NAttributes::TYsonStringBuilder builder;
            NAttributes::TMergeAttributesHelper mergeHelper(builder.GetConsumer());
            for (const auto& [path, value] : Zip(AttributeSelector_.Paths, event.Attributes.Values)) {
                mergeHelper.ToNextPath(path, /*isEtc*/ false);
                builder->OnRaw(value);
            }
            mergeHelper.Finalize();

            event.Attributes.Values = {builder.Flush()};
        }

        return event;
    }

    void ParseKey(
        const std::vector<TSelectContinuationTokenEntry>& continuationTokenEntries,
        TUnversionedRow row,
        std::vector<TYsonString>& key) const
    {
        key.resize(continuationTokenEntries.size());
        for (int index = 0; index < std::ssize(continuationTokenEntries); ++index) {
            YT_VERIFY(index < static_cast<int>(row.GetCount()));
            key[index] = UnversionedValueToYson(row[index]);
        }
    }

    int CollectRows(
        const TSelectObjectHistoryContinuationTokenSchema& tokenSchema,
        bool forwardContinuationToken,
        EHistoryReadPhase phase,
        IUnversionedRowsetPtr rowset,
        bool fromIndex)
    {
        auto rows = rowset->GetRows();
        std::vector<NYson::TYsonString> keyBuffer;

        int collectedRowCount = 0;
        for (collectedRowCount = 0; collectedRowCount < std::ssize(rows); ++collectedRowCount) {
            auto event = ParseRow(tokenSchema, rows[collectedRowCount], fromIndex);
            Collector_->OnEvent(std::move(event));
            // If distinct and limit are enabled, and enough events are read,
            // than latest event would not be in resulting events, so continuation is not updated here.
            if (!Collector_->IsEnough() || ContinuationToken_->EvaluatedEntries.empty() || !DistinctBySelector_) {
                if (forwardContinuationToken) {
                    ParseKey(tokenSchema.Entries, rows[collectedRowCount], keyBuffer);
                    FillContinuationToken(&*ContinuationToken_, tokenSchema, phase, keyBuffer);
                } else {
                    ContinuationToken_->ReadPhase = phase;
                }
            }
            if (Collector_->IsEnough()) {
                break;
            }
        }

        return collectedRowCount;
    }

    bool CanQueryHistoryIndex() const
    {
        THROW_ERROR_EXCEPTION_IF(!DistinctBySelector_ && Options_.IndexMode == ESelectObjectHistoryIndexMode::Enabled,
            "Cannot use history index without `distinct` option");
        if (!DistinctBySelector_ || Options_.IndexMode == ESelectObjectHistoryIndexMode::Disabled) {
            return false;
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        const auto& indexedAttributes = objectManager->GetTypeHandlerOrThrow(ObjectType_)->GetHistoryIndexedAttributes();
        // NB: returns true if no attributes are present.
        bool canUse = std::all_of(DistinctBySelector_->Paths.begin(), DistinctBySelector_->Paths.end(),
            [&] (const NYPath::TYPath& path) {
                return indexedAttributes.contains(path) && (Options_.IndexMode == ESelectObjectHistoryIndexMode::Enabled
                    ? objectManager->IsHistoryIndexAttributeStoreEnabled(ObjectType_, path)
                    : objectManager->IsHistoryIndexAttributeQueryEnabled(ObjectType_, path));
            });
        THROW_ERROR_EXCEPTION_IF(Options_.IndexMode == ESelectObjectHistoryIndexMode::Enabled && !canUse,
            "Cannot use history index for object %Qv for the given distinct-by selector %v",
            NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(ObjectType_),
            DistinctBySelector_->Paths);
        return canUse;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<ISelectObjectHistoryExecutor> MakeSelectObjectHistoryExecutor(
    TTransactionPtr transaction,
    TObjectTypeValue objectType,
    const TObjectKey& objectKey,
    const TAttributeSelector& attributeSelector,
    const std::optional<TAttributeSelector>& distinctBySelector,
    std::optional<TSelectObjectHistoryContinuationToken> continuationToken,
    TSelectObjectHistoryOptions options)
{
    return std::make_unique<TSelectObjectHistoryExecutor>(
        transaction,
        objectType,
        objectKey,
        attributeSelector,
        distinctBySelector,
        std::move(continuationToken),
        std::move(options));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
