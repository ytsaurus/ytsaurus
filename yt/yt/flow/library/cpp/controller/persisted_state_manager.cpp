#include "private.h"

#include "config.h"
#include "persisted_state_manager.h"
#include "yt_connector.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/persisted_state_control.h>

#include <yt/yt/flow/library/cpp/native_client/public.h>

#include <yt/yt/core/ypath/helpers.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/client/tablet_client/watermark_runtime_data.h>

namespace NYT::NFlow::NController {

////////////////////////////////////////////////////////////////////////////////

using namespace NApi;
using namespace NYTree;
using namespace NYson;
using namespace NYPath;
using namespace NConcurrency;
using namespace NTableClient;
using namespace NTabletClient;

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ControllerLogger;

////////////////////////////////////////////////////////////////////////////////

static const std::string ImportantVersionsKey = "important_versions"; // FlowView + Spec + FlowCoreTarget versions.
static const std::string FlowStateKey = "flow_view";                  // backward compatibility
static const std::string SpecKey = "spec";
static const std::string DynamicSpecKey = "dynamic_spec";
static const std::string FlowCoreTargetKey = "flow_core_target";
static const std::string CustomRuntimeDataAttribute = "custom_runtime_data";
static const std::string SystemTimestampColumnName = "system_timestamp";

////////////////////////////////////////////////////////////////////////////////

struct TCommitContext
    : public TPersistedStateCommitContext
{
    using TCallback = std::function<void(NApi::ITransactionPtr&)>;

    TCommitContext()
    {
        // Reserve 2 rows for updating flow_view_obsolete and important versions.
        StatementCount = 2;
    }

    TFlowStatePtr FlowState;
    TCallback Callback;
};

////////////////////////////////////////////////////////////////////////////////

class TPersistedStateStorageHandler
    : public TPersistedStateStorageHandlerBase<std::string>
{
public:
    TPersistedStateStorageHandler(IYTConnectorPtr connector, TPersistedStateManagerConfigPtr config)
        : Connector_{std::move(connector)}
        , Config_{std::move(config)}
    {
        MaxSelectSize_ = Config_->MaxReadsPerTransaction;
        MaxExecuteSize_ = Config_->MaxWritesPerTransaction;
        YT_TLOG_INFO("Persisted state storage handler created")
            .With("ReadLimit", Config_->MaxReadsPerTransaction)
            .With("WriteLimit", Config_->MaxWritesPerTransaction);
    }

    template <typename TBody>
    void RunInTransaction(const std::string& stepName, TBody body)
    {
        TTransactionStartOptions options;
        options.Timeout = Config_->Timeout;
        auto transaction = WaitFor(Connector_->StartTransaction(NYT::NTransactionClient::ETransactionType::Tablet, options))
            .ValueOrThrow();

        try {
            body(transaction);
        } catch (const NYT::TErrorException& exception) {
            auto error = TError("Failed to commit transaction during %v", stepName);
            error <<= exception.Error();
            auto abortError = WaitFor(transaction->Abort());
            if (abortError.IsOK()) {
                THROW_ERROR(error);
            }
            THROW_ERROR(error <<= abortError);
        }

        WaitFor(transaction->Commit())
            .ThrowOnError();
    }

    template <typename T>
    void WriteObsoleteObject(const NApi::ITransactionPtr& transaction, const std::string& name, const TIntrusivePtr<T>& object)
    {
        auto nameTable = New<TNameTable>();
        i32 keyField = nameTable->GetIdOrRegisterName("key");
        i32 valueField = nameTable->GetIdOrRegisterName("value");
        TYPath path = YPathJoin(Connector_->GetPipelinePath().GetPath(), FlowStateObsoleteTableName);

        const auto& value = ConvertToYsonString(object);
        const auto& valueBuf = value.AsStringBuf();
        if (valueBuf.size() > 50_MB) {
            YT_TLOG_WARNING("Too big object")
                .With("Size", valueBuf.size())
                .With("Name", name);
        }

        auto rowBuffer = New<TRowBuffer>();
        std::vector<NApi::TRowModification> rows;
        auto row = rowBuffer->AllocateUnversioned(2);
        row[0] = rowBuffer->CaptureValue(MakeUnversionedStringValue(name, keyField));
        row[1] = rowBuffer->CaptureValue(MakeUnversionedAnyValue(valueBuf, valueField));
        rows.push_back(NRowModifications::TWriteRow(row));

        transaction->ModifyRows(path, nameTable, MakeSharedRange(std::move(rows), std::move(rowBuffer)));
        YT_TLOG_INFO("Write obsolete object completed")
            .With("Type", TypeName<T>())
            .With("Table", FlowStateObsoleteTableName)
            .With("Key", name);
    }

private:
    template <typename T>
    TIntrusivePtr<T> ReadObsoleteObjectCommon(const NApi::IClientBasePtr client, const std::string& name)
    {
        auto nameTable = New<TNameTable>();
        i32 keyField = nameTable->GetIdOrRegisterName("key");
        i32 valueField = nameTable->GetIdOrRegisterName("value");
        TYPath path = YPathJoin(Connector_->GetPipelinePath().GetPath(), FlowStateObsoleteTableName);

        auto rowBuffer = New<TRowBuffer>();
        std::vector<TLegacyKey> keysToLookup;
        auto keyToLookup = rowBuffer->AllocateUnversioned(1);
        keyToLookup[0] = rowBuffer->CaptureValue(MakeUnversionedStringValue(name, keyField));
        keysToLookup.push_back(keyToLookup);
        auto range = MakeSharedRange(std::move(keysToLookup), std::move(rowBuffer));

        NApi::TLookupRowsOptions lookupRowsOptions;
        lookupRowsOptions.ColumnFilter = TColumnFilter({valueField});
        lookupRowsOptions.KeepMissingRows = true;
        lookupRowsOptions.Timeout = Config_->Timeout;
        lookupRowsOptions.Timestamp = NTransactionClient::SyncLastCommittedTimestamp;

        auto future = client->LookupRows(path, nameTable, range, lookupRowsOptions);
        auto result = WaitFor(std::move(future)).ValueOrThrow();
        const auto& rowset = result.Rowset;
        const auto& rows = rowset->GetRows();
        if (rows.Empty() || !rows[0]) {
            return New<T>();
        }
        const auto& row = rows[0];
        if (!row) {
            return New<T>();
        }
        auto schema = rowset->GetSchema();
        valueField = schema->GetColumnIndexOrThrow("value");
        auto str = FromUnversionedValue<std::optional<TYsonString>>(row[valueField]);
        if (!str) {
            return New<T>();
        }
        auto res = ConvertTo<TIntrusivePtr<T>>(*str);
        YT_TLOG_INFO("Read obsolete object completed")
            .With("Type", TypeName<T>())
            .With("Table", FlowStateObsoleteTableName)
            .With("Key", name);
        return res;
    }

public:
    template <typename T>
    TIntrusivePtr<T> ReadObsoleteObject(const NApi::ITransactionPtr& transaction, const std::string& name)
    {
        return ReadObsoleteObjectCommon<T>(transaction, name);
    }

    template <typename T>
    TIntrusivePtr<T> ReadObsoleteObject(const std::string& name)
    {
        return ReadObsoleteObjectCommon<T>(Connector_->GetClient(), name);
    }

    //! Get some rows with SequenceId > lastSequenceId, ordered by SequenceId.
    void Select(TSequenceId lastSequenceId, std::vector<TStorageRow>& result) override
    {
        TYPath path = YPathJoin(Connector_->GetPipelinePath().GetPath(), FlowStateTableName);
        auto query = Format("* FROM [%v] WHERE sequence_id > %v ORDER BY sequence_id LIMIT %v", path, lastSequenceId, MaxSelectSize_);
        auto future = Connector_->GetClient()->SelectRows(query);
        auto selected = WaitFor(std::move(future)).ValueOrThrow();
        const auto& rowset = selected.Rowset;
        const auto& rows = rowset->GetRows();
        if (result.empty()) {
            result.reserve(rows.size());
        }
        auto schema = rowset->GetSchema();
        i32 sequenceIdField = schema->GetColumnIndexOrThrow("sequence_id");
        i32 flagsField = schema->GetColumnIndexOrThrow("flags");
        i32 nameField = schema->GetColumnIndexOrThrow("state_name");
        i32 keyLeftField = schema->GetColumnIndexOrThrow("key_left");
        i32 keyRightField = schema->GetColumnIndexOrThrow("key_right");
        i32 valueField = schema->GetColumnIndexOrThrow("value");
        for (const auto& dbRow : rows) {
            TStorageRow row;
            row.SequenceId = TSequenceId{FromUnversionedValue<TSequenceId::TUnderlying>(dbRow[sequenceIdField])};
            if (dbRow[flagsField].Type != NTableClient::EValueType::Null) {
                row.Flags = static_cast<EStorageRowFlags>(FromUnversionedValue<ui64>(dbRow[flagsField]));
            }
            row.Name = FromUnversionedValue<TPersistedStateName>(dbRow[nameField]);
            row.KeyLeft = FromUnversionedValue<std::string>(dbRow[keyLeftField]);
            if (dbRow[keyRightField].Type != NTableClient::EValueType::Null) {
                row.KeyRight = FromUnversionedValue<std::string>(dbRow[keyRightField]);
            }
            if (dbRow[valueField].Type != NTableClient::EValueType::Null) {
                row.Value = FromUnversionedValue<TYsonString>(dbRow[valueField]).AsStringBuf();
            }
            result.push_back(std::move(row));
        }

        YT_TLOG_INFO("FlowState rows selected from YT")
            .With("RowCount", result.size());
    }

    //! Execute given statements in one transactions.
    void Execute(
        std::vector<TStorageRow>&& insertRows,
        const std::vector<TSequenceId>& deleteRows,
        bool isFinal,
        const std::vector<TPersistedStateCommitContext*>& commitContextsBase) override
    {
        YT_TLOG_INFO("FlowState persist started")
            .With("RowCount", insertRows.size() + deleteRows.size());

        bool hasStatements = !insertRows.empty() || !deleteRows.empty();
        if (isFinal) {
            for (auto* commitContextBase : commitContextsBase) {
                TCommitContext& context = dynamic_cast<TCommitContext&>(*commitContextBase);
                if (context.Callback) {
                    hasStatements = true;
                }
                if (context.FlowState) {
                    hasStatements = true;
                }
            }
        }
        if (!hasStatements) {
            // There's no reason to execute empty transaction.
            return;
        }

        auto nameTable = New<TNameTable>();
        i32 sequenceIdField = nameTable->GetIdOrRegisterName("sequence_id");
        i32 flagsField = nameTable->GetIdOrRegisterName("flags");
        i32 nameField = nameTable->GetIdOrRegisterName("state_name");
        i32 keyLeftField = nameTable->GetIdOrRegisterName("key_left");
        i32 keyRightField = nameTable->GetIdOrRegisterName("key_right");
        i32 valueField = nameTable->GetIdOrRegisterName("value");
        TYPath path = YPathJoin(Connector_->GetPipelinePath().GetPath(), FlowStateTableName);

        RunInTransaction("Persisting FlowState", [&] (NApi::ITransactionPtr& transaction) {
            if (isFinal) {
                for (auto* commitContextBase : commitContextsBase) {
                    TCommitContext& context = dynamic_cast<TCommitContext&>(*commitContextBase);
                    if (context.Callback) {
                        context.Callback(transaction);
                    }
                    context.Callback = nullptr;
                    if (context.FlowState) {
                        WriteObsoleteObject(transaction, FlowStateKey, context.FlowState);
                    }
                    context.FlowState = nullptr;
                }
            }

            auto rowBuffer = New<TRowBuffer>();
            std::vector<NApi::TRowModification> rows;

            for (auto sequenceId : deleteRows) {
                auto dbRow = rowBuffer->AllocateUnversioned(1);
                dbRow[0] = rowBuffer->CaptureValue(MakeUnversionedInt64Value(sequenceId.Underlying(), sequenceIdField));
                rows.push_back(NRowModifications::TDeleteRow(dbRow));
            }
            for (auto& origRow : insertRows) {
                auto dbRow = rowBuffer->AllocateUnversioned(6);
                dbRow[0] = rowBuffer->CaptureValue(MakeUnversionedInt64Value(origRow.SequenceId.Underlying(), sequenceIdField));
                if (origRow.Flags != EStorageRowFlags::None) {
                    dbRow[1] = rowBuffer->CaptureValue(MakeUnversionedUint64Value(static_cast<ui64>(origRow.Flags), flagsField));
                } else {
                    dbRow[1] = rowBuffer->CaptureValue(MakeUnversionedNullValue(flagsField));
                }
                dbRow[2] = rowBuffer->CaptureValue(MakeUnversionedStringValue(origRow.Name, nameField));
                dbRow[3] = rowBuffer->CaptureValue(MakeUnversionedStringValue(origRow.KeyLeft, keyLeftField));
                if (!origRow.KeyRight.empty()) {
                    dbRow[4] = rowBuffer->CaptureValue(MakeUnversionedStringValue(origRow.KeyRight, keyRightField));
                } else {
                    dbRow[4] = rowBuffer->CaptureValue(MakeUnversionedNullValue(keyRightField));
                }
                if (!origRow.Value.empty()) {
                    dbRow[5] = rowBuffer->CaptureValue(MakeUnversionedAnyValue(origRow.Value, valueField));
                } else {
                    dbRow[5] = rowBuffer->CaptureValue(MakeUnversionedNullValue(valueField));
                }
                rows.push_back(NRowModifications::TWriteRow(dbRow));
            }
            auto rowCount = rows.size();

            transaction->ModifyRows(path, nameTable, MakeSharedRange(std::move(rows), std::move(rowBuffer)));

            YT_TLOG_INFO("FlowState row written to YT")
                .With("RowCount", rowCount);
        });
    }

private:
    const IYTConnectorPtr Connector_;
    const TPersistedStateManagerConfigPtr Config_;
};

////////////////////////////////////////////////////////////////////////////////

class TPersistedStateManager
    : public IPersistedStateManager
{
public:
    TPersistedStateManager(
        IYTConnectorPtr connector,
        TPersistedStateManagerConfigPtr config)
        : Connector_(std::move(connector))
        , Config_(std::move(config))
        , PersistedStateStorageHandler_(New<TPersistedStateStorageHandler>(Connector_, Config_))
    { }

    // Used by PersistFlowState, PersistSpecs, and PersistFlowCoreTarget to enforce
    // optimistic concurrency on ImportantVersionsKey. Validates |expectedVersions|
    // against the currently persisted versions, then writes |newVersions| back
    // when at least one version was bumped. The write anchors a row-level lock so
    // that exactly one transactions wins.
    void HandleImportantVersions(
        const TPipelineImportantVersionsPtr& expectedVersions,
        const TPipelineImportantVersionsPtr& newVersions,
        const NApi::ITransactionPtr& transaction)
    {
        if (AreNodesEqual(ConvertToNode(newVersions), ConvertToNode(expectedVersions))) {
            return;
        }

        auto actualVersions = PersistedStateStorageHandler_->ReadObsoleteObject<TPipelineImportantVersions>(transaction, ImportantVersionsKey);
        expectedVersions->EnsureEqual(*actualVersions);

        PersistedStateStorageHandler_->WriteObsoleteObject(transaction, ImportantVersionsKey, newVersions);
    }

    void CheckDynamicSpecVersion(TVersion expectedVersion, const NApi::ITransactionPtr& transaction)
    {
        auto spec = PersistedStateStorageHandler_->ReadObsoleteObject<TVersionedDynamicPipelineSpec>(transaction, DynamicSpecKey);
        auto actualVersion = spec->GetVersion();
        THROW_ERROR_EXCEPTION_UNLESS(
            actualVersion == expectedVersion,
            NFlow::EErrorCode::SpecVersionMismatch,
            "Spec version mismatch: expected %v, actual %v",
            expectedVersion,
            actualVersion);
    }

    void PersistFlowState(const TFlowStatePtr& flowState, const TPipelineImportantVersionsPtr& expectedVersions) override
    {
        YT_TLOG_INFO("Flow state persist started");
        auto newVersions = CloneYsonStruct(expectedVersions);
        newVersions->PipelineStateVersion = flowState->ExecutionSpec->PipelineState->GetVersion();
        THROW_ERROR_EXCEPTION_UNLESS(
            expectedVersions->PipelineStateVersion <= newVersions->PipelineStateVersion,
            "Can not decrease pipeline state version");
        auto path = YPathJoin(Connector_->GetPipelinePath().GetPath(), FlowStateKey);
        auto stateManager = MakeStrong(this);
        TCommitContext context;
        context.FlowState = flowState;
        context.Callback = [weakThis = MakeWeak(this), &newVersions, expectedVersions] (NApi::ITransactionPtr& transaction) {
            if (auto strongThis = weakThis.Lock()) {
                strongThis->HandleImportantVersions(expectedVersions, newVersions, transaction);
            } else {
                THROW_ERROR_EXCEPTION("Controller state manager is dead");
            }
        };
        flowState->CommitMutation(&context);
        flowState->Epoch = flowState->ExecutionSpec->GetEpoch();
        YT_TLOG_INFO("Flow state persist completed");
    }

    TFlowStatePtr RecoverFlowState() override
    {
        YT_TLOG_INFO("Flow state recover started");
        auto flowState = PersistedStateStorageHandler_->ReadObsoleteObject<TFlowState>(FlowStateKey);
        auto control = New<TPersistedStateControl<std::string>>(PersistedStateStorageHandler_);
        flowState->AttachToControl(control);
        control->Recover();
        YT_TLOG_INFO("Flow state recover completed");
        return flowState;
    }

    void PersistSpecs(
        const std::optional<TVersionedPipelineSpecPtr>& spec,
        const std::optional<TPipelineImportantVersionsPtr>& expectedVersions,
        const std::optional<TVersionedDynamicPipelineSpecPtr>& dynamicSpec,
        const std::optional<TVersion>& expectedDynamicSpecVersion) override
    {
        YT_TLOG_INFO("PersistSpecs started")
            .With("HasSpec", spec.has_value())
            .With("HasDynamicSpec", dynamicSpec.has_value());
        THROW_ERROR_EXCEPTION_IF(!spec.has_value() && !dynamicSpec.has_value(), "At least one spec must be provided");

        PersistedStateStorageHandler_->RunInTransaction("Persisting specs", [&] (const NApi::ITransactionPtr& transaction) {
            if (spec.has_value()) {
                THROW_ERROR_EXCEPTION_IF(!expectedVersions.has_value(), "Expected versions must be provided for spec");
                auto newVersions = CloneYsonStruct(expectedVersions.value());
                newVersions->PipelineSpecVersion = (*spec)->GetVersion();
                THROW_ERROR_EXCEPTION_UNLESS(
                    expectedVersions.value()->PipelineSpecVersion < newVersions->PipelineSpecVersion,
                    "New pipeline spec version %v must be greater than expected version %v",
                    newVersions->PipelineSpecVersion,
                    expectedVersions.value()->PipelineSpecVersion);
                HandleImportantVersions(expectedVersions.value(), newVersions, transaction);
                PersistedStateStorageHandler_->WriteObsoleteObject(transaction, SpecKey, *spec);
            }

            if (dynamicSpec.has_value()) {
                THROW_ERROR_EXCEPTION_IF(!expectedDynamicSpecVersion.has_value(), "Expected dynamic spec version must be provided");
                THROW_ERROR_EXCEPTION_UNLESS(
                    expectedDynamicSpecVersion->Underlying() < (*dynamicSpec)->GetVersion().Underlying(),
                    "New pipeline dynamic spec version %v must be greater than expected version %v",
                    (*dynamicSpec)->GetVersion().Underlying(),
                    expectedDynamicSpecVersion->Underlying());
                CheckDynamicSpecVersion(*expectedDynamicSpecVersion, transaction);
                PersistedStateStorageHandler_->WriteObsoleteObject(transaction, DynamicSpecKey, *dynamicSpec);
            }
        });

        YT_TLOG_INFO("PersistSpecs finished")
            .With("SpecPersistedVersion", spec.has_value() ? std::make_optional(spec.value()->GetVersion()) : std::nullopt)
            .With("DynamicSpecPersistedVersion", dynamicSpec.has_value() ? std::make_optional(dynamicSpec.value()->GetVersion()) : std::nullopt);
    }

    TVersionedPipelineSpecPtr RecoverSpec() override
    {
        YT_TLOG_INFO("RecoverSpec started");
        auto result = PersistedStateStorageHandler_->ReadObsoleteObject<TVersionedPipelineSpec>(SpecKey);
        YT_TLOG_INFO("RecoverSpec finished")
            .With("Version", result->GetVersion());
        return result;
    }

    TVersionedDynamicPipelineSpecPtr RecoverDynamicSpec() override
    {
        YT_TLOG_INFO("RecoverDynamicSpec started");
        auto result = PersistedStateStorageHandler_->ReadObsoleteObject<TVersionedDynamicPipelineSpec>(DynamicSpecKey);
        YT_TLOG_INFO("RecoverDynamicSpec finished")
            .With("Version", result->GetVersion());
        return result;
    }

    void AdvanceInputMessagesWatermark(TSystemTimestamp watermark) override
    {
        TWatermarkRuntimeDataConfig watermarkRuntimeData;
        watermarkRuntimeData.Watermark = ConvertTo<ui64>(watermark);
        watermarkRuntimeData.ColumnName = SystemTimestampColumnName;

        TSetNodeOptions setOptions;
        setOptions.Timeout = Config_->Timeout;

        // clang-format off
        auto runtimeData = BuildYsonStringFluently()
            .BeginMap()
                .Item(CustomRuntimeDataWatermarkKey).Value(watermarkRuntimeData)
            .EndMap();
        // clang-format on

        auto advanceWatermark = [&] (TStringBuf tableName) {
            auto tablePath = YPathJoin(Connector_->GetPipelinePath().GetPath(), tableName);
            WaitFor(
                Connector_->GetClient()->SetNode(
                    Format("%v/@%v", tablePath, CustomRuntimeDataAttribute),
                    runtimeData,
                    setOptions))
                .ThrowOnError();
        };

        advanceWatermark(InputMessagesTableName);

        // Also advance compact_input_messages watermark if any computation uses it.
        auto compactTablePath = YPathJoin(Connector_->GetPipelinePath().GetPath(), CompactInputMessagesTableName);
        auto existsResult = WaitFor(Connector_->GetClient()->NodeExists(compactTablePath, {}));
        if (existsResult.IsOK() && existsResult.Value()) {
            advanceWatermark(CompactInputMessagesTableName);
        }
    }

    void PersistFlowCoreTarget(
        const TVersionedFlowCoreTargetPtr& flowCoreTarget,
        const TPipelineImportantVersionsPtr& expectedVersions) override
    {
        if (flowCoreTarget->GetVersion() <= expectedVersions->FlowCoreTargetVersion) {
            THROW_ERROR_EXCEPTION(
                    NFlow::EErrorCode::FlowCoreTargetVersionMismatch,
                    "FlowCoreTarget version must strictly increase")
                << TErrorAttribute("expected_version", expectedVersions->FlowCoreTargetVersion)
                << TErrorAttribute("new_version", flowCoreTarget->GetVersion());
        }

        auto newVersions = CloneYsonStruct(expectedVersions);
        newVersions->FlowCoreTargetVersion = flowCoreTarget->GetVersion();

        PersistedStateStorageHandler_->RunInTransaction("Persisting flow core target",
            [&] (const NApi::ITransactionPtr& transaction) {
                HandleImportantVersions(expectedVersions, newVersions, transaction);
                PersistedStateStorageHandler_->WriteObsoleteObject(transaction, FlowCoreTargetKey, flowCoreTarget);
            });

        YT_TLOG_DEBUG("FlowCoreTarget persisted")
            .With("FlowCoreTarget", flowCoreTarget->GetValue())
            .With("Version", flowCoreTarget->GetVersion());
    }

    TVersionedFlowCoreTargetPtr RecoverFlowCoreTarget() override
    {
        YT_TLOG_DEBUG("RecoverFlowCoreTarget started");
        auto result = PersistedStateStorageHandler_->ReadObsoleteObject<TVersionedFlowCoreTarget>(FlowCoreTargetKey);
        YT_TLOG_DEBUG("RecoverFlowCoreTarget finished")
            .With("FlowCoreTarget", result->GetValue())
            .With("Version", result->GetVersion());
        return result;
    }

private:
    const IYTConnectorPtr Connector_;
    const TPersistedStateManagerConfigPtr Config_;
    const TIntrusivePtr<TPersistedStateStorageHandler> PersistedStateStorageHandler_;
};

////////////////////////////////////////////////////////////////////////////////

IPersistedStateManagerPtr CreatePersistedStateManager(
    IYTConnectorPtr connector,
    TPersistedStateManagerConfigPtr config)
{
    return New<TPersistedStateManager>(std::move(connector), std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

TPipelineImportantVersionsPtr MakePipelineImportantVersions(const TFlowStatePtr& flowState, const TVersionedPipelineSpecPtr& spec)
{
    auto versions = New<TPipelineImportantVersions>();
    versions->PipelineSpecVersion = spec->GetVersion();
    versions->PipelineStateVersion = flowState->ExecutionSpec->PipelineState->GetVersion();
    versions->FlowCoreTargetVersion = flowState->ExecutionSpec->FlowCoreTarget->GetVersion();
    return versions;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
