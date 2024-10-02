#include "object_table_reader.h"

#include "config.h"
#include "db_schema.h"
#include "key_util.h"
#include "private.h"
#include "session.h"
#include "transaction.h"

#include <yt/yt/orm/server/master/yt_connector.h>
#include <yt/yt/orm/server/objects/transaction_manager.h>

#include <yt/yt/orm/library/query/misc.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/rowset.h>

#include <library/cpp/yt/string/string_builder.h>

#include <util/string/vector.h>

namespace NYT::NOrm::NServer::NObjects {
namespace {

using namespace NApi;
using namespace NConcurrency;
using namespace NTableClient;
using namespace NYTree;
using namespace NYson;
using namespace NQueryClient;

using namespace NMaster;

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf PivotKeysAttributeName = "pivot_keys";

////////////////////////////////////////////////////////////////////////////////

class TObjectTableReader
    : public IObjectTableReader
{
public:
    TObjectTableReader(
        IBootstrap* bootstrap,
        TObjectTableReaderConfigPtr config,
        const TDBTable* dbTable,
        std::vector<TString> fieldNames,
        TString removalTimeFieldName)
        : Config_(std::move(config))
        , TablePath_(bootstrap->GetYTConnector()->GetTablePath(dbTable))
        , TableKeyFields_(dbTable->GetKeyFields(/*filterEvaluatedFields*/ false))
        , TableKeyFieldNames_(std::invoke([&] {
            std::vector<TString> result;
            result.reserve(TableKeyFields_.size());
            for (const auto* field : TableKeyFields_) {
                result.push_back(field->Name);
            }
            return result;
        }))
        , PivotKeys_(std::invoke([&] {
            std::vector<TObjectKey> keys;
            if (Config_->ReadByTablets) {
                const auto& ytConnector = bootstrap->GetYTConnector();
                auto ytClient = ytConnector->GetClient(ytConnector->FormatUserTag());
                auto future = ytClient->GetNode(TablePath_ + "/@" + PivotKeysAttributeName);
                auto rsp = ConvertTo<IListNodePtr>(WaitFor(future)
                    .ValueOrThrow());
                keys.reserve(rsp->GetChildCount());
                for (const auto& serializedKey : rsp->GetChildren()) {
                    keys.push_back(ParseObjectKey(
                        serializedKey->AsList(),
                        TableKeyFields_,
                        /*allowPrefix*/ true));
                }
            }

            if (keys.empty()) {
                keys.push_back({});
            }
            return keys;
        }))
        , KeyFieldPositionsInSelector_(std::invoke([&] {
            if (!Config_->BatchSize.has_value()) {
                return std::vector<size_t>{};
            }

            THashMap<TString, size_t> fieldNameToIndex;
            for (size_t index = 0; index < fieldNames.size(); ++index) {
                EmplaceOrCrash(fieldNameToIndex, fieldNames[index], index);
            }

            std::vector<size_t> result;
            result.reserve(TableKeyFieldNames_.size());
            for (const auto& keyFieldName : TableKeyFieldNames_) {
                if (auto it = fieldNameToIndex.find(keyFieldName)) {
                    result.push_back(it->second);
                } else {
                    // NB! If batching is enabled, all key fields must be present
                    // in selector to generate boundary condition for the next batch.
                    // To ensure that that is the case, we simply append all missing
                    // key fields to the selector.
                    result.push_back(fieldNames.size());
                    fieldNames.push_back(keyFieldName);
                }
            }
            return result;
        }))
        , Selector_(std::invoke([&] {
            TStringBuilder builder;
            TDelimitedStringBuilderWrapper wrapper(&builder, ", ");
            for (const auto& fieldName : fieldNames) {
                wrapper->AppendString(NAst::FormatId(fieldName));
            }
            return builder.Flush();
        }))
        , RemovalTimeFilter_(removalTimeFieldName.Empty()
            ? TString{}
            : Format("is_null(%v)", NAst::FormatId(removalTimeFieldName)))
    { }

    bool IsFinished() const override
    {
        return PivotKeys_.size() <= NextPivot_;
    }

    std::vector<TFuture<TStreamResult>> StreamAll(
        NObjects::TTimestamp timestamp,
        NObjects::TTransactionManagerPtr transactionManager,
        IInvokerPtr invoker) override
    {
        if (Config_->BatchSize) {
            THROW_ERROR_EXCEPTION("Cannot stream when batching enabled");
        }

        if (IsFinished()) {
            THROW_ERROR_EXCEPTION("The reader has already finished reading the table");
        }

        if (timestamp == NullTimestamp) {
            THROW_ERROR_EXCEPTION("Streaming timestamp cannot be null");
        }

        auto doStream = [timestamp, transactionManager] (TString query) {
            auto beginWaiting = GetCpuInstant();

            NObjects::TStartReadOnlyTransactionOptions options;
            options.StartTimestamp = timestamp;
            options.ReadingTransactionOptions.AllowFullScan = true;
            auto transaction = WaitFor(transactionManager->StartReadOnlyTransaction(std::move(options)))
                .ValueOrThrow();

            auto rowset = ReadImpl(transaction->GetSession(), query);
            return TStreamResult{
                .Rowset = std::move(rowset),
                .ResponseDelay = CpuDurationToDuration(GetCpuInstant() - beginWaiting),
            };
        };

        std::vector<TFuture<TStreamResult>> futures;
        futures.reserve(PivotKeys_.size() - NextPivot_);
        for (; NextPivot_ < PivotKeys_.size(); ++NextPivot_) {
            futures.push_back(BIND(doStream, GenerateQuery())
                .AsyncVia(invoker)
                .Run());
        }

        return futures;
    }

    IUnversionedRowsetPtr Read(
        NObjects::ISession* session,
        std::source_location location) override
    {
        if (IsFinished()) {
            THROW_ERROR_EXCEPTION("The reader has already finished reading the table");
        }

        const auto result = ReadImpl(session, GenerateQuery(), location);
        const auto range = result->GetRows();

        if (Config_->BatchSize) {
            size_t batchSize = *Config_->BatchSize;
            YT_VERIFY(range.Size() <= batchSize);
            if (range.Size() == batchSize) {
                CaptureLastKey(range.Back());
            } else {
                // Start from the beginning of the next shard.
                ++NextPivot_;
                LastKey_ = {};
            }
        } else {
            ++NextPivot_;
        }

        return result;
    }

    void Reset() override
    {
        ResetCustomObjectFilter();

        NextPivot_ = 0;
        LastKey_ = {};
    }

    void SetCustomObjectFilter(TString filter) override
    {
        CustomFilter_ = std::move(filter);
    }

    void ResetCustomObjectFilter() override
    {
        CustomFilter_.clear();
    }

private:
    const TObjectTableReaderConfigPtr Config_;
    const TString TablePath_;
    const std::vector<const TDBField*> TableKeyFields_;
    const std::vector<TString> TableKeyFieldNames_;
    const std::vector<TObjectKey> PivotKeys_;
    const std::vector<size_t> KeyFieldPositionsInSelector_;
    const TString Selector_;
    const TString RemovalTimeFilter_;

    size_t NextPivot_ = 0;
    TObjectKey LastKey_;
    TString CustomFilter_;

    TString GenerateQuery() const
    {
        TString query = Format("%v FROM %v", Selector_, NAst::FormatId(TablePath_));

        if (auto filter = GenerateFilter()) {
            query += Format(" WHERE %v", std::move(filter));
        }
        if (Config_->BatchSize) {
            query += Format(" limit %v", *Config_->BatchSize);
        }

        YT_LOG_DEBUG("Generated query string (Query: %v)", query);

        return query;
    }

    TString GenerateFilter() const
    {
        return NQuery::JoinFilters({
            GeneratePivotFilter(),
            RemovalTimeFilter_,
            CustomFilter_,
        });
    }

    TString GeneratePivotFilter() const
    {
        std::vector<TString> clauses;
        if (LastKey_) {
            clauses.push_back(NQuery::GenerateLexicographicalFilter(
                TableKeyFieldNames_,
                LastKey_.AsLiteralValueTuple(),
                NQuery::EOrderRelation::Greater));
        } else if (NextPivot_ < PivotKeys_.size() && PivotKeys_[NextPivot_]) {
            clauses.push_back(GenerateLexicographicalFilter(
                TableKeyFieldNames_,
                PivotKeys_[NextPivot_].AsLiteralValueTuple(),
                NQuery::EOrderRelation::GreaterOrEqual));
        }

        if (NextPivot_ + 1 < PivotKeys_.size() && PivotKeys_[NextPivot_ + 1]) {
            clauses.push_back(GenerateLexicographicalFilter(
                TableKeyFieldNames_,
                PivotKeys_[NextPivot_ + 1].AsLiteralValueTuple(),
                NQuery::EOrderRelation::Less));
        }

        return NQuery::JoinFilters(clauses);
    }

    void CaptureLastKey(TUnversionedRow row)
    {
        YT_VERIFY(!KeyFieldPositionsInSelector_.empty());

        TObjectKey::TKeyFields fields;
        fields.reserve(KeyFieldPositionsInSelector_.size());
        for (auto pos : KeyFieldPositionsInSelector_) {
            fields.push_back(UnversionedValueToKeyField(row[pos]));
        }
        LastKey_ = TObjectKey(std::move(fields));
    }

    static IUnversionedRowsetPtr ReadImpl(
        NObjects::ISession* session,
        const TString& query,
        std::source_location location = std::source_location::current())
    {
        IUnversionedRowsetPtr result;

        session->ScheduleLoad(
            [&] (NObjects::ILoadContext* context) {
                context->ScheduleSelect(
                    query,
                    [&] (const IUnversionedRowsetPtr& rowset) {
                        result = rowset;
                    });
            });
        session->FlushLoads(location);

        return result;
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

IObjectTableReaderPtr CreateObjectTableReader(
    IBootstrap* bootstrap,
    TObjectTableReaderConfigPtr config,
    const NObjects::TDBTable* dbTable,
    std::vector<TString> fieldNames,
    TString removalTimeFieldName)
{
    return New<TObjectTableReader>(
        bootstrap,
        std::move(config),
        dbTable,
        std::move(fieldNames),
        std::move(removalTimeFieldName));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
