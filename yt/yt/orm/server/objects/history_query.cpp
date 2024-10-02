#include "history_query.h"

#include "helpers.h"
#include "object_manager.h"

#include <yt/yt/orm/server/master/bootstrap.h>
#include <yt/yt/orm/server/master/yt_connector.h>

#include <yt/yt/orm/library/query/query_rewriter.h>

#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/library/query/base/ast.h>
#include <yt/yt/library/query/base/helpers.h>
#include <yt/yt/library/query/base/query_preparer.h>

namespace NYT::NOrm::NServer::NObjects {

using namespace NClient::NObjects;

using namespace NYT::NQueryClient::NAst;

using NYT::NQueryClient::TSourceLocation;
using NYT::NQueryClient::EBinaryOp;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

TSelectObjectHistoryContinuationTokenSchema GetObjectHistorySelectContinuationTokenEntries(
    bool uuidProvided,
    bool descendingTimeOrder,
    const THistoryTableBase* sourceTable)
{
    const auto* historyEventsTable = sourceTable->PrimaryTable;

    TSelectObjectHistoryContinuationTokenSchema tokenSchema;
    auto& entries = tokenSchema.Entries;
    bool timeOrdersMismatch = descendingTimeOrder == sourceTable->OptimizeForAscendingTime;
    // Workaround while YT-14961 is not ready, the description is below.
    if (!uuidProvided) {
        TSelectContinuationTokenEntry entry;
        entry.DBExpression = historyEventsTable->Fields.Time.Name;
        entry.DBTableName = historyEventsTable->GetName();
        entry.Descending = timeOrdersMismatch;
        entries.push_back(std::move(entry));
    }
    // Table key provides effectiveness (order by primary index) and the order stability.
    // Therefore evaluated fields must also be provided for effectiveness.
    for (const auto* keyColumn : historyEventsTable->GetKeyFields(/*filterEvaluatedFields*/ false)) {
        TSelectContinuationTokenEntry entry;
        entry.DBExpression = keyColumn->Name;
        entry.DBTableName = historyEventsTable->GetName();
        // Workaround while YT-14961 is not ready:
        // When uuid is provided, order by time may be added in the main loop
        // instead of the beginning of continuation token.
        entry.Descending = uuidProvided &&
            keyColumn == &historyEventsTable->Fields.Time &&
            timeOrdersMismatch;
        entries.push_back(std::move(entry));
    }
    if (!historyEventsTable->UseUuidInKey) {
        YT_VERIFY(entries.back().DBExpression == historyEventsTable->Fields.EventType.Name);
        // There is only one case when 2 events can happen in transaction for one object:
        //   1) Object gets removed;
        //   2) Object gets created with other uuid.
        // In order to get the correct order in result, event codes for these events must
        // come in descending order: CreatedEventTypeValue = 1, RemovedEventTypeValue = 2.
        entries.back().Descending = descendingTimeOrder != sourceTable->UsePositiveEventTypes;
    }
    tokenSchema.ReadSource = sourceTable;
    return tokenSchema;
}

////////////////////////////////////////////////////////////////////////////////

class THistoryQueryBuilder
{
public:
    THistoryQueryBuilder(
        NMaster::IBootstrap* bootstrap,
        const THistoryIndexTable* historyIndexTable,
        TObjectTypeValue objectType,
        const TObjectKey& objectKey,
        const std::optional<TAttributeSelector>& distinctBySelector,
        const std::optional<TSelectObjectHistoryContinuationToken>& continuationToken,
        int limit,
        const TSelectObjectHistoryOptions& options,
        bool useIndex,
        bool restrictHistoryFilter)
        : Bootstrap_(bootstrap)
        , HistoryEventsTable_(historyIndexTable->PrimaryTable)
        , HistoryIndexTable_(historyIndexTable)
        , ObjectType_(objectType)
        , ObjectKey_(objectKey)
        , DistinctBySelector_(distinctBySelector)
        , ContinuationToken_(continuationToken)
        , Limit_(limit)
        , Options_(options)
        , UseIndex_(useIndex)
        , RestrictHistoryFilter_(restrictHistoryFilter)
        , ContinuationTokenSchema_(GetObjectHistorySelectContinuationTokenEntries(
            /*uuidProvided*/ !Options_.Uuid.Empty(),
            Options_.DescendingTimeOrder,
            /*sourceTable*/ useIndex ? static_cast<const THistoryTableBase*>(HistoryIndexTable_) : HistoryEventsTable_))
        , PrimaryTableName_(UseIndex_
            ? HistoryIndexTable_->GetName()
            : HistoryEventsTable_->GetName())
    {
        if (Options_.Filter) {
            ParsedFilter_ = NQueryClient::ParseSource(Options_.Filter, NQueryClient::EParseMode::Expression);
        }

        InitializeColumnNameMapping();
    }

    std::pair<TString, TSelectObjectHistoryContinuationTokenSchema> Build()
    {
        PrepareSelect();
        PrepareFrom();
        PrepareWherePredicate();
        PrepareOrderBy();
        PrepareLimit();
        return {FormatQuery(Query_), std::move(ContinuationTokenSchema_)};
    }

private:
    NMaster::IBootstrap* const Bootstrap_;

    const THistoryEventsTable* HistoryEventsTable_;
    const THistoryIndexTable* HistoryIndexTable_;
    const TObjectTypeValue ObjectType_;
    const TObjectKey& ObjectKey_;
    const std::optional<TAttributeSelector>& DistinctBySelector_;
    const std::optional<TSelectObjectHistoryContinuationToken>& ContinuationToken_;
    const int Limit_;
    const TSelectObjectHistoryOptions& Options_;
    const bool UseIndex_;
    const bool RestrictHistoryFilter_;
    const TSelectObjectHistoryContinuationTokenSchema ContinuationTokenSchema_;
    const TString& PrimaryTableName_;

    THashMap<std::pair<TString, TString>, TString> ReferenceAndRequestedTableToColumnName_;

    TObjectsHolder Holder_;
    TQuery Query_;
    std::unique_ptr<NQueryClient::TParsedSource> ParsedFilter_;

    void InitializeColumnNameMapping()
    {
        auto addMapping = [&] (
            const TString& sourceTable,
            const TString& sourceColumnName,
            const TString& destinationTable,
            const TString& destinationColumnName)
        {
            ReferenceAndRequestedTableToColumnName_.emplace(
                std::pair(destinationTable, sourceColumnName),
                destinationColumnName);
            ReferenceAndRequestedTableToColumnName_.emplace(
                std::pair(sourceTable, destinationColumnName),
                sourceColumnName);
        };
        addMapping(
            HistoryEventsTable_->GetName(),
            HistoryEventsTable_->Fields.EventType.Name,
            HistoryIndexTable_->GetName(),
            HistoryIndexTable_->Fields.HistoryEventType.Name);
    }

    TExpression* TryRewriteSystemAttribute(const TString& referenceName)
    {
        if (referenceName.Empty()) {
            return nullptr;
        }

        NYPath::TTokenizer tokenizer(referenceName);
        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::Slash);
        if (tokenizer.Advance() != NYPath::ETokenType::Literal || tokenizer.GetLiteralValue() != "event") {
            return nullptr;
        }
        tokenizer.Advance();
        auto suffixPath = tokenizer.GetInput();

        tokenizer.Expect(NYPath::ETokenType::Slash);
        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::Literal);

        auto attribute = tokenizer.GetLiteralValue();
        tokenizer.Advance();

        const static THashSet<TString> AllowedSystemAttributes = {"user", "transaction_id"};
        if (AllowedSystemAttributes.find(attribute) != AllowedSystemAttributes.end()) {
            THROW_ERROR_EXCEPTION_UNLESS(
                tokenizer.GetInput().Empty(),
                "History event attribute %Qv does not allow subscription",
                attribute);
            return CreateReferenceExpression(attribute);
        }

        const static THashSet<TString> AllowedSubscriptableAttributes = {"transaction_context"};
        THROW_ERROR_EXCEPTION_IF(AllowedSubscriptableAttributes.find(attribute) == AllowedSubscriptableAttributes.end(),
            "History filter contains unsupported attribute %Qv",
            attribute);

        return CreateReferenceExpression(HistoryEventsTable_->Fields.Etc.Name, TString{suffixPath});
    }

    bool CanTransferContinuationToken()
    {
        YT_VERIFY(ContinuationToken_);

        if (ContinuationToken_->ReadSource == ContinuationTokenSchema_.ReadSource->GetName()) {
            return true;
        }

        // Switches to index table are allowed for compatible tables.
        if (ContinuationToken_->ReadSource == HistoryEventsTable_->GetName() &&
            ContinuationTokenSchema_.ReadSource == HistoryIndexTable_)
        {
            return true;
        }

        return false;
    }

    TExpressionPtr RewriteReferenceExpression(const TReference& reference)
    {
        THROW_ERROR_EXCEPTION_IF(reference.TableName,
            "Table references for history query are not supported");

        if (auto expression = TryRewriteSystemAttribute(reference.ColumnName)) {
            return expression;
        }

        if (RestrictHistoryFilter_) {
            auto* typeHandler = Bootstrap_->GetObjectManager()->GetTypeHandlerOrThrow(ObjectType_);
            THROW_ERROR_EXCEPTION_UNLESS(typeHandler->IsPathAllowedForHistoryFilter(reference.ColumnName),
                "Attribute %v is not allowed to be used in history filter",
                reference.ColumnName);
        }

        return CreateReferenceExpression(HistoryEventsTable_->Fields.Value.Name, /*suffixPath*/ reference.ColumnName);
    }

    const TString& ConvertReferenceToCorrespondingColumnName(const TString& reference)
    {
        std::pair<TStringBuf, TStringBuf> key{PrimaryTableName_, reference};
        if (auto it = ReferenceAndRequestedTableToColumnName_.find(key); it != ReferenceAndRequestedTableToColumnName_.end()) {
            return it->second;
        }
        return reference;
    }

    TFunctionExpression* CreateReferenceExpression(const TString& reference, const TString& suffixPath)
    {
        return CreateFunctionExpression(
            "try_get_any",
            TExpressionList{
                CreateReferenceExpression(reference),
                CreateLiteralExpression(suffixPath),
            });
    }

    TReferenceExpression* CreateReferenceExpression(
        const TString& reference,
        bool omitTable = false)
    {
        TString columnName = ConvertReferenceToCorrespondingColumnName(reference);
        if (omitTable) {
            return Holder_.New<TReferenceExpression>(TSourceLocation(), columnName);
        } else {
            return Holder_.New<TReferenceExpression>(TSourceLocation(), columnName, PrimaryTableName_);
        }
    }

    TLiteralExpression* CreateLiteralExpression(const TLiteralValue& value)
    {
        return Holder_.New<TLiteralExpression>(
            TSourceLocation(),
            value);
    }

    TBinaryOpExpression* CreateOperationExpression(EBinaryOp operation, const TString& columnName, TLiteralValue rhs)
    {
        return Holder_.New<TBinaryOpExpression>(
            TSourceLocation(),
            operation,
            TExpressionList{
                CreateReferenceExpression(columnName),
            },
            TExpressionList{
                CreateLiteralExpression(std::move(rhs))
            });
    }

    TReferenceExpression* CreateContinuationTokenEntryExpression(
        const TSelectContinuationTokenEntry& entry,
        bool omitTable = false)
    {
        entry.ValidateExactlyOneExpression();
        THROW_ERROR_EXCEPTION_IF(entry.DBExpression.empty(),
            "History select continuation token supports only DB expression for now");
        return CreateReferenceExpression(entry.DBExpression, omitTable);
    }

    TFunctionExpression* CreateFunctionExpression(const TString& functionName, TExpressionList expressions)
    {
        return Holder_.New<TFunctionExpression>(
            TSourceLocation(),
            functionName,
            std::move(expressions));
    }

    TFunctionExpression* CreateFunctionExpression(const TString& functionName, TExpression* expression)
    {
        return CreateFunctionExpression(functionName, TExpressionList{expression});
    }

    void PushSelector(TReferenceExpression* expression)
    {
        Query_.SelectExprs->push_back(expression);
    }

    void PushSelector(const TString& selector)
    {
        PushSelector(CreateReferenceExpression(selector));
    }

    void PushSelector(const TSelectContinuationTokenEntry& entry)
    {
        PushSelector(CreateContinuationTokenEntryExpression(entry));
    }

    void PrepareSelect()
    {
        Query_.SelectExprs.emplace();

        for (const auto& entry : ContinuationTokenSchema_.Entries) {
            PushSelector(entry);
        }

        auto pushCommonSelectors = [&] (const auto& table, const auto& eventTypeField) {
            PushSelector(table.Fields.Uuid.Name);
            PushSelector(table.Fields.Time.Name);
            PushSelector(eventTypeField.Name);
            PushSelector(table.Fields.User.Name);
            PushSelector(table.Fields.Value.Name);
            PushSelector(table.Fields.Etc.Name);
        };

        if (UseIndex_) {
            pushCommonSelectors(*HistoryIndexTable_, HistoryIndexTable_->Fields.HistoryEventType);
        } else {
            pushCommonSelectors(*HistoryEventsTable_, HistoryEventsTable_->Fields.EventType);
        }

        // Index has no column for history enabled attributes: they are contained in `etc`.
        if (!UseIndex_ && Bootstrap_->GetObjectManager()->AreHistoryEnabledAttributePathsEnabled()) {
            PushSelector(HistoryEventsTable_->Fields.HistoryEnabledAttributes.Name);
        }
    }

    void PrepareFrom()
    {
        const TDBTable* table{nullptr};
        if (UseIndex_) {
            table = HistoryIndexTable_;
        } else {
            table = HistoryEventsTable_;
        }
        Query_.Table = TTableDescriptor(
            Bootstrap_->GetYTConnector()->GetTablePath(table),
            table->GetName());
    }

    void PrepareOptionalFilter(TExpressionPtr& filterExpression)
    {
        YT_VERIFY(filterExpression);

        if (ParsedFilter_) {
            auto filter = std::get<TExpressionPtr>(ParsedFilter_->AstHead.Ast);

            NQuery::TQueryRewriter rewriter(&Holder_, [this] (const TReference& reference) {
                return RewriteReferenceExpression(reference);
            });
            filter = rewriter.Run(filter);

            filterExpression = NQueryClient::BuildAndExpression(&Holder_, filterExpression, filter);
        }
        if (Options_.Uuid) {
            filterExpression = NQueryClient::BuildAndExpression(
                &Holder_,
                filterExpression,
                CreateOperationExpression(
                    EBinaryOp::Equal,
                    HistoryEventsTable_->Fields.Uuid.Name,
                    Options_.Uuid));
        }
        if (HasValue(Options_.TimeInterval.Begin)) {
            filterExpression = NQueryClient::BuildAndExpression(
                &Holder_,
                filterExpression,
                CreateOperationExpression(
                    HistoryEventsTable_->OptimizeForAscendingTime ?  EBinaryOp::GreaterOrEqual : EBinaryOp::LessOrEqual,
                    HistoryEventsTable_->Fields.Time.Name,
                    GetRawHistoryTime(Options_.TimeInterval.Begin, HistoryEventsTable_->OptimizeForAscendingTime)));
        }
        if (HasValue(Options_.TimeInterval.End)) {
            filterExpression = NQueryClient::BuildAndExpression(
                &Holder_,
                filterExpression,
                CreateOperationExpression(
                    HistoryEventsTable_->OptimizeForAscendingTime ? EBinaryOp::Less : EBinaryOp::Greater,
                    HistoryEventsTable_->Fields.Time.Name,
                    GetRawHistoryTime(Options_.TimeInterval.End, HistoryEventsTable_->OptimizeForAscendingTime)));
        }

        if (ContinuationToken_) {
            THROW_ERROR_EXCEPTION_UNLESS(CanTransferContinuationToken(),
                NClient::EErrorCode::InvalidContinuationToken,
                "Read source mismatch: expected: %v, but token contains incompatible source %v",
                ContinuationTokenSchema_.ReadSource,
                ContinuationToken_->ReadSource)
            ValidateSelectContinuationTokenReceivedEntries(
                ContinuationToken_->EvaluatedEntries,
                ContinuationTokenSchema_.Entries);

            TExpressionList entryExpressions;
            entryExpressions.reserve(ContinuationToken_->EvaluatedEntries.size());
            for (const auto& entry : ContinuationToken_->EvaluatedEntries) {
                entryExpressions.push_back(CreateContinuationTokenEntryExpression(entry));
            }

            auto continuationTokenFilterExpression = CreateContinuationTokenFilterExpression(
                ContinuationToken_->EvaluatedEntries,
                entryExpressions,
                &Holder_);

            if (continuationTokenFilterExpression) {
                filterExpression = NQueryClient::BuildAndExpression(
                    &Holder_,
                    filterExpression,
                    continuationTokenFilterExpression);
            }
        }
    }

    void PrepareIndexFilter(TExpressionPtr& filterExpression)
    {
        YT_VERIFY(filterExpression);
        YT_VERIFY(UseIndex_);
        YT_VERIFY(DistinctBySelector_);

        bool negativeEventType = !HistoryEventsTable_->UsePositiveEventTypes;
        auto buildIndexFilterExpression = [&] (TEventTypeValue eventType, const NYPath::TYPath& indexType) {
            return NQueryClient::BuildAndExpression(
                &Holder_,
                CreateOperationExpression(
                    EBinaryOp::Equal,
                    HistoryIndexTable_->Fields.HistoryEventType.Name,
                    negativeEventType ? -eventType : eventType),
                CreateOperationExpression(
                    EBinaryOp::Equal,
                    HistoryIndexTable_->Fields.IndexEventType.Name,
                    indexType));
        };
        auto indexSelectionExpression = NQueryClient::BuildOrExpression(
            &Holder_,
            buildIndexFilterExpression(ObjectCreatedEventTypeValue, OtherHistoryIndexEventType),
            buildIndexFilterExpression(ObjectRemovedEventTypeValue, OtherHistoryIndexEventType));
        for (const auto& path : DistinctBySelector_->Paths) {
            indexSelectionExpression = NQueryClient::BuildOrExpression(
                &Holder_,
                indexSelectionExpression,
                buildIndexFilterExpression(ObjectUpdatedEventTypeValue, path));
        }
        filterExpression = NQueryClient::BuildAndExpression(
            &Holder_,
            filterExpression,
            std::move(indexSelectionExpression));
    }

    void PrepareWherePredicate()
    {
        NQueryClient::NAst::TExpressionPtr filterExpression;

        filterExpression = NQueryClient::BuildAndExpression(
            &Holder_,
            CreateOperationExpression(
                EBinaryOp::Equal,
                HistoryEventsTable_->Fields.ObjectType.Name,
                static_cast<i64>(ObjectType_)),
            CreateOperationExpression(
                EBinaryOp::Equal,
                HistoryEventsTable_->Fields.ObjectId.Name,
                ObjectKey_.ToString()));

        PrepareOptionalFilter(filterExpression);
        if (UseIndex_) {
            PrepareIndexFilter(filterExpression);
        }

        Query_.WherePredicate = TExpressionList{std::move(filterExpression)};
    }

    void PrepareOrderBy()
    {
        for (const auto& entry : ContinuationTokenSchema_.Entries) {
            Query_.OrderExpressions.push_back({
                {CreateContinuationTokenEntryExpression(entry)},
                entry.Descending
            });
        }
    }

    void PrepareLimit()
    {
        Query_.Limit = Limit_;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

void EnsureTimeMode(TTimeInterval* interval, bool allowTimeModeConversion, EHistoryTimeMode dbTimeMode)
{
    if (!HasValue(interval->Begin) && !HasValue(interval->End)) {
        return;
    }

    EnsureTimeMode(&interval->Begin, allowTimeModeConversion, dbTimeMode, /*ceil*/ false);
    EnsureTimeMode(&interval->End, allowTimeModeConversion, dbTimeMode, /*ceil*/ true);
}

void EnsureTimeMode(THistoryTime* time, bool allowTimeModeConversion, EHistoryTimeMode dbTimeMode, bool ceil)
{
    if (!HasValue(*time)) {
        return;
    }

    auto checkConversionAllowed = [allowTimeModeConversion] {
        THROW_ERROR_EXCEPTION_UNLESS(allowTimeModeConversion,
            "Cannot convert time to database time mode");
    };

    switch (dbTimeMode) {
        case EHistoryTimeMode::Logical:
            if (auto* physicalTime = std::get_if<TInstant>(time)) {
                checkConversionAllowed();
                auto [start, end] = NTransactionClient::InstantToTimestamp(*physicalTime);
                time->emplace<TTimestamp>(ceil ? end : start);
            }
            break;
        case EHistoryTimeMode::Physical:
            if (auto* logicalTime = std::get_if<TTimestamp>(time)) {
                checkConversionAllowed();
                auto [start, end] = NTransactionClient::TimestampToInstant(*logicalTime);
                time->emplace<TInstant>(ceil ? end : start);
            }
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

std::pair<TString, TSelectObjectHistoryContinuationTokenSchema> BuildHistoryQuery(
    NMaster::IBootstrap* bootstrap,
    const THistoryIndexTable* historyIndexTable,
    TObjectTypeValue objectType,
    const TObjectKey& objectKey,
    // TODO(grigminakov): Provide for index queries only
    const std::optional<TAttributeSelector>& distinctBySelector,
    const std::optional<TSelectObjectHistoryContinuationToken>& continuationToken,
    int limit,
    const TSelectObjectHistoryOptions& options,
    bool useIndex,
    bool restrictHistoryFilter)
{
    return THistoryQueryBuilder(
        bootstrap,
        historyIndexTable,
        objectType,
        objectKey,
        distinctBySelector,
        continuationToken,
        limit,
        options,
        useIndex,
        restrictHistoryFilter)
        .Build();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
