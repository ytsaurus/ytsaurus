#include "tables.h"

#include "config.h"
#include "private.h"

#include <yt/client/table_client/name_table.h>
#include <yt/client/table_client/row_buffer.h>

#include <yt/client/chunk_client/read_limit.h>

#include <yt/client/api/client.h>
#include <yt/client/api/transaction.h>
#include <yt/client/api/rowset.h>

#include <yt/client/ypath/rich.h>

#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/serialize.h>

#include <yt/core/misc/range.h>

namespace NYT {
namespace NSkynetManager {

using namespace NApi;
using namespace NYson;
using namespace NYTree;
using namespace NYPath;
using namespace NTableClient;
using namespace NChunkClient;
using namespace NTransactionClient;
using namespace NConcurrency;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TRequestKey& key, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("table_path").Value(key.TablePath)
            .Item("revision").Value(key.TableRevision)
            .Item("key_columns").Value(key.KeyColumns)
        .EndMap();
}

TString ToString(const TRequestKey& key)
{
    return ConvertToYsonString(key, EYsonFormat::Text).GetData();
}

////////////////////////////////////////////////////////////////////////////////

TRequestKey TRequestKey::FromRow(
    const TNameTablePtr& nameTable,
    const TUnversionedRow& row)
{
    YCHECK(row.GetCount() == 2);
    TRequestKey key;

    YCHECK(row[0].Id == nameTable->GetId("table_path"));
    YCHECK(row[0].Type == EValueType::String);
    key.TablePath = TString(row[0].Data.String, row[0].Length);

    YCHECK(row[1].Id == nameTable->GetId("options"));
    YCHECK(row[1].Type == EValueType::String);
    TYsonString optionsYson(row[1].Data.String, row[1].Length);
    auto optionsNode = ConvertToNode(optionsYson)->AsMap();
    key.TableRevision = ConvertTo<ui64>(optionsNode->FindChild("revision"));
    key.KeyColumns = ConvertTo<std::vector<TString>>(optionsNode->FindChild("key_columns"));

    return key;
}

void TRequestKey::FillRow(
    TMutableUnversionedRow* row,
    const NTableClient::TNameTablePtr& nameTable,
    const NTableClient::TRowBufferPtr& rowBuffer) const
{
    YCHECK(row->GetCount() >= 2);

    auto tablePathId = nameTable->GetIdOrRegisterName("table_path");
    (*row)[0] = rowBuffer->Capture(MakeUnversionedStringValue(TablePath, tablePathId));

    auto optionsId = nameTable->GetIdOrRegisterName("options");
    auto optionsString = BuildYsonStringFluently(EYsonFormat::Text)
        .BeginMap()
            .Item("revision").Value(TableRevision)
            .Item("key_columns").Value(KeyColumns)
        .EndMap().GetData();
    (*row)[1] = rowBuffer->Capture(MakeUnversionedStringValue(optionsString, optionsId));
}

TKey TRequestKey::ToRow(
    const TNameTablePtr& nameTable,
    const TRowBufferPtr& rowBuffer) const
{
    auto row = rowBuffer->AllocateUnversioned(2);
    FillRow(&row, nameTable, rowBuffer);
    return row;
}

////////////////////////////////////////////////////////////////////////////////

bool TRequestState::IsExpired(TDuration ttl) const
{
    return State != ERequestState::Active && UpdateTime + ttl < TInstant::Now();
}

void TRequestState::ValidateOwnerId(TGuid processId) const
{
    if (OwnerId != processId) {
        THROW_ERROR_EXCEPTION("Failed to update request status")
            << TErrorAttribute("owner_id", OwnerId)
            << TErrorAttribute("process_id", processId);
    }
}

TRequestState TRequestState::FromRow(
    const TNameTablePtr& nameTable,
    const TUnversionedRow& row)
{
    TRequestState state;

    YCHECK(row[3].Id == nameTable->GetId("state"));
    YCHECK(row[3].Type == EValueType::String);
    state.State = TEnumTraits<ERequestState>::FromString(TStringBuf(row[3].Data.String, row[3].Length));

    YCHECK(row[4].Id == nameTable->GetId("update_time"));
    YCHECK(row[4].Type == EValueType::Uint64);
    state.UpdateTime = TInstant::MicroSeconds(row[4].Data.Uint64);

    YCHECK(row[5].Id == nameTable->GetId("owner_id"));
    YCHECK(row[5].Type == EValueType::String);
    state.OwnerId = TGuid::FromString(TStringBuf(row[5].Data.String, row[5].Length));

    YCHECK(row[6].Id == nameTable->GetId("error"));
    if (row[6].Type == EValueType::Any) {
        TYsonString errorYson(row[6].Data.String, row[6].Length);
        state.Error = ConvertTo<TError>(errorYson);
    }

    YCHECK(row[7].Id == nameTable->GetId("progress"));
    if (row[7].Type == EValueType::Any) {
        TYsonString progressYson(row[7].Data.String, row[7].Length);
        state.Progress = progressYson;
    }

    YCHECK(row[8].Id == nameTable->GetId("resources"));
    if (row[8].Type == EValueType::Any) {
        TYsonString resourcesYson(row[8].Data.String, row[8].Length);
        state.Resources = ConvertTo<std::vector<TResourceLinkPtr>>(resourcesYson);
    }

    if (state.State == ERequestState::Failed && !state.Error) {
        THROW_ERROR_EXCEPTION("Inconsistent error state");
    }
    if (state.State == ERequestState::Creating && !state.Progress) {
        THROW_ERROR_EXCEPTION("Inconsistent starting state");
    }
    if (state.State == ERequestState::Active && !state.Resources) {
        THROW_ERROR_EXCEPTION("Invonsistent active state");
    }

    return state;
}

TUnversionedRow ToRow(
    const TRequestKey& key,
    const TRequestState& state,
    const TNameTablePtr& nameTable,
    const TRowBufferPtr& rowBuffer)
{
    auto row = rowBuffer->AllocateUnversioned(8);
    key.FillRow(&row, nameTable, rowBuffer);

    auto stateId = nameTable->GetIdOrRegisterName("state");
    row[2] = rowBuffer->Capture(MakeUnversionedStringValue(ToString(state.State), stateId));

    auto updateTimeId = nameTable->GetIdOrRegisterName("update_time");
    row[3] = MakeUnversionedUint64Value(state.UpdateTime.MicroSeconds(), updateTimeId);

    auto ownerId = nameTable->GetIdOrRegisterName("owner_id");
    row[4] = rowBuffer->Capture(MakeUnversionedStringValue(ToString(state.OwnerId), ownerId));

    auto errorId = nameTable->GetIdOrRegisterName("error");
    if (state.Error) {
        auto errorYson = ConvertToYsonString(state.Error);
        row[5] = rowBuffer->Capture(MakeUnversionedAnyValue(errorYson.GetData(), errorId));
    } else {
        row[5] = MakeUnversionedNullValue(errorId);
    }

    auto progressId = nameTable->GetIdOrRegisterName("progress");
    if (state.Progress) {
        row[6] = rowBuffer->Capture(MakeUnversionedAnyValue(state.Progress->GetData(), progressId));
    } else {
        row[6] = MakeUnversionedNullValue(progressId);
    }

    auto resourcesId = nameTable->GetIdOrRegisterName("resources");
    if (state.Resources) {
        auto resourcesYson = ConvertToYsonString(state.Resources);
        row[7] = rowBuffer->Capture(MakeUnversionedAnyValue(resourcesYson.GetData(), resourcesId));
    } else {
        row[7] = MakeUnversionedNullValue(resourcesId);
    }

    return row;
}

TUnversionedRow ToRow(
    const NProto::TResource& resource,
    TGuid duplicateId,
    const TResourceId& resourceId,
    const NYPath::TYPath& tablePath,
    const TNameTablePtr& nameTable,
    const TRowBufferPtr& rowBuffer,
    std::vector<TUnversionedRow>* filesRows)
{
    auto row = rowBuffer->AllocateUnversioned(4);
    
    auto resourceRowId = nameTable->GetIdOrRegisterName("resource_id");
    row[0] = rowBuffer->Capture(MakeUnversionedStringValue(resourceId, resourceRowId));

    auto duplicateRowId = nameTable->GetIdOrRegisterName("duplicate_id");
    row[1] = rowBuffer->Capture(MakeUnversionedStringValue(ToString(duplicateId), duplicateRowId));

    auto tableRangeId = nameTable->GetIdOrRegisterName("table_range");
    row[2] = rowBuffer->Capture(MakeUnversionedStringValue(tablePath, tableRangeId));

    auto metaId = nameTable->GetIdOrRegisterName("meta");
    auto proto = SerializeProtoToString(resource);
    row[3] = rowBuffer->Capture(MakeUnversionedStringValue(proto, metaId));

    return row;
}

////////////////////////////////////////////////////////////////////////////////

TTables::TTables(
    const IClientPtr& client,
    const TClusterConnectionConfigPtr& config)
    : Config_(config)
    , Client_(client)
    , ProcessId_(TGuid::Create())
    , Logger(TLogger(SkynetManagerLogger).AddTag("Cluster: %v", config->ClusterName))
    , RequestsTable_(Config_->Root + "/requests")
    , ResourcesTable_(Config_->Root + "/resources")
    , FilesTable_(Config_->Root + "/files")
{ }

THashSet<TResourceId> TTables::ListResources()
{
    auto result = WaitFor(Client_->SelectRows(Format("resource_id FROM [%s] GROUP BY resource_id", ResourcesTable_)))
        .ValueOrThrow();

    THashSet<TResourceId> resources;
    for (auto&& row : result.Rowset->GetRows()) {
        YCHECK(row.GetCount() == 1);
        YCHECK(row[0].Type == EValueType::String);
        resources.emplace(row[0].Data.String, row[0].Length);
    }
    return resources;
}

std::vector<TRequestKey> TTables::ListActiveRequests()
{
    std::vector<TRequestKey> requests;
    auto result = WaitFor(Client_->SelectRows(Format("table_path, options FROM [%s]", RequestsTable_)))
        .ValueOrThrow();

    auto nameTable = TNameTable::FromSchema(result.Rowset->Schema());
    for (auto&& row : result.Rowset->GetRows()) {
        requests.push_back(TRequestKey::FromRow(nameTable, row));
    }

    return requests;
}

bool TTables::StartRequest(const TRequestKey& key, TRequestState* state)
{
    LOG_INFO("Starting request (RequestKey: %v)", key);

    auto nameTable = New<TNameTable>();
    auto rowBuffer = New<TRowBuffer>();

    auto tx = WaitFor(Client_->StartTransaction(ETransactionType::Tablet))
        .ValueOrThrow();

    TRequestState oldState;
    bool ok = LookupRequest(tx, nameTable, rowBuffer, key, &oldState);
    if (ok) {
        if (oldState.State == ERequestState::Failed) {
            LOG_INFO("Found failed request (RequestKey: %v)", key);
            DeleteRequest(tx, nameTable, rowBuffer, key);
            WaitFor(tx->Commit())
                .ThrowOnError();
            *state = oldState;
            return false;
        }

        // TODO(prime@): move to config
        if (!oldState.IsExpired(TDuration::Minutes(5))) {
            LOG_INFO("Found existing active request (RequestKey: %v)", key);
            WaitFor(tx->Abort())
                .ThrowOnError();
            *state = oldState;
            return false;
        }

        LOG_INFO("Found existing expired request (RequestKey: %v)", key);
    } else {
        LOG_INFO("Existing request not found (RequestKey: %v)", key);
    }

    TRequestState newState;
    newState.State = ERequestState::Creating;
    newState.OwnerId = ProcessId_;
    newState.UpdateTime = TInstant::Now();
    newState.Progress = BuildYsonStringFluently()
        .BeginMap()
            .Item("stage").Value("starting")
        .EndMap();

    WriteRequest(tx, nameTable, rowBuffer, key, newState);

    WaitFor(tx->Commit())
        .ThrowOnError();

    *state = newState;
    return true;
}

void TTables::UpdateStatus(
    const TRequestKey& key,
    std::optional<TYsonString> progress,
    std::optional<TError> error)
{
    auto nameTable = New<TNameTable>();

    auto rowBuffer = New<TRowBuffer>();
    auto rowKey = key.ToRow(nameTable, rowBuffer);
    auto keyRange = MakeSharedRange(std::vector<TKey>{rowKey}, rowBuffer);

    auto tx = WaitFor(Client_->StartTransaction(ETransactionType::Tablet))
        .ValueOrThrow();

    TRequestState state;
    bool ok = LookupRequest(tx, nameTable, rowBuffer, key, &state);
    if (!ok) {
        THROW_ERROR_EXCEPTION("Failed to update request status; Request not found")
            << TErrorAttribute("request_key", key);
    }

    state.ValidateOwnerId(ProcessId_);
    state.UpdateTime = TInstant::Now();

    if (error) {
        state.State = ERequestState::Failed;
        state.Error = *error;
    }

    if (progress) {
        state.Progress = *progress;
    }

    WriteRequest(tx, nameTable, rowBuffer, key, state);

    WaitFor(tx->Commit())
        .ValueOrThrow();
}

std::vector<TResourceId> TTables::FinishRequest(
    const TRequestKey& key,
    const std::vector<TTableShard>& shards)
{
    auto nameTable = New<TNameTable>();

    auto rowBuffer = New<TRowBuffer>();
    auto rowKey = key.ToRow(nameTable, rowBuffer);
    auto keyRange = MakeSharedRange(std::vector<TKey>{rowKey}, rowBuffer);

    auto tx = WaitFor(Client_->StartTransaction(ETransactionType::Tablet))
        .ValueOrThrow();

    TRequestState state;
    bool ok = LookupRequest(tx, nameTable, rowBuffer, key, &state);
    if (!ok) {
        THROW_ERROR_EXCEPTION("Failed to finish request; Key not found")
            << TErrorAttribute("request_key", key);
    }

    state.ValidateOwnerId(ProcessId_);
    state.State = ERequestState::Active;
    state.Progress.reset();

    auto duplicateId = TGuid::Create();
    std::vector<TResourceId> resourceIds;
    std::vector<TResourceLinkPtr> links;
    std::vector<TUnversionedRow> resourcesRows;
    std::vector<TUnversionedRow> filesRows;

    for (const auto& shard : shards) {
        auto description = ConvertResource(shard.Resource, true, false);

        YCHECK(!shard.Resource.files().empty());
        auto startRow = shard.Resource.files()[0].start_row();
        const auto& lastFile = shard.Resource.files()[shard.Resource.files().size() - 1];
        auto endRow = lastFile.start_row() + lastFile.row_count();

        TYPath tableRange = TRichYPath::Parse(key.TablePath).GetPath();
        tableRange += Format("[#%lld:#%lld]", startRow, endRow);

        resourcesRows.emplace_back(ToRow(
            shard.Resource,
            duplicateId,
            description.ResourceId,
            tableRange,
            nameTable,
            rowBuffer,
            &filesRows));

        resourceIds.push_back(description.ResourceId);

        auto link = New<TResourceLink>();
        link->ResourceId = description.ResourceId;
        link->DuplicateId = duplicateId;
        link->Key = shard.Key;

        links.emplace_back(link);
    }
    state.Resources = links;

    WriteRequest(tx, nameTable, rowBuffer, key, state);
    tx->WriteRows(ResourcesTable_, nameTable, MakeSharedRange(std::move(resourcesRows), rowBuffer));
    if (!filesRows.empty()) {
        tx->WriteRows(FilesTable_, nameTable, MakeSharedRange(std::move(filesRows), rowBuffer));
    }

    WaitFor(tx->Commit())
        .ValueOrThrow();

    return resourceIds;
}

void TTables::EraseRequest(const TRequestKey& key)
{
    auto nameTable = New<TNameTable>();
    auto rowBuffer = New<TRowBuffer>();

    auto tx = WaitFor(Client_->StartTransaction(ETransactionType::Tablet))
        .ValueOrThrow();

    TRequestState state;
    bool ok = LookupRequest(tx, nameTable, rowBuffer, key, &state);
    if (!ok) {
        tx->Abort();
        return;
    }

    DeleteRequest(tx, nameTable, rowBuffer, key);

    std::vector<TKey> linkedResources;
    auto resourceRowId = nameTable->GetIdOrRegisterName("resource_id");
    auto duplicateRowId = nameTable->GetIdOrRegisterName("duplicate_id");

    if (state.Resources) {
        for (auto&& resource : *state.Resources) {
            auto resourceKey = rowBuffer->AllocateUnversioned(2);
            resourceKey[0] = rowBuffer->Capture(MakeUnversionedStringValue(
                resource->ResourceId,
                resourceRowId));
            resourceKey[1] = rowBuffer->Capture(MakeUnversionedStringValue(
                ToString(resource->DuplicateId),
                duplicateRowId));
            linkedResources.push_back(resourceKey);
        }

        auto keyRange = MakeSharedRange(linkedResources, rowBuffer);
        tx->DeleteRows(ResourcesTable_, nameTable, keyRange);
    }

    WaitFor(tx->Commit())
        .ValueOrThrow();    
}

void TTables::GetResource(
    const TResourceId& resourceId,
    TYPath* tableRange,
    NProto::TResource* resource)
{
    auto query = Format("duplicate_id, table_range, meta FROM [%s] WHERE resource_id = \"%s\" LIMIT 1",
        ResourcesTable_,
        resourceId);

    auto result = WaitFor(Client_->SelectRows(query))
        .ValueOrThrow();

    if (result.Rowset->GetRows().Size() == 0) {
        THROW_ERROR_EXCEPTION("Resource not found")
            << TErrorAttribute("resource_id", resourceId);
    }

    const auto& row = result.Rowset->GetRows()[0];
    YCHECK(row.GetCount() == 3);
    YCHECK(row[0].Type == EValueType::String);
    auto duplicateId = TGuid::FromString(TString(row[0].Data.String, row[0].Length));
    YCHECK(row[1].Type == EValueType::String);
    *tableRange = TString(row[1].Data.String, row[1].Length);
    YCHECK(row[2].Type == EValueType::String);
    DeserializeProto(resource, TRef(row[2].Data.String, row[2].Length));

    // TODO(prime): load sha1 from /files table
    (void) duplicateId;
}

bool TTables::LookupRequest(
    const IClientBasePtr& client,
    const TNameTablePtr& nameTable,
    const TRowBufferPtr& rowBuffer,
    const TRequestKey& key,
    TRequestState* state)
{
    auto rowKey = key.ToRow(nameTable, rowBuffer);
    auto keyRange = MakeSharedRange(std::vector<TKey>{rowKey}, rowBuffer);
    auto result = WaitFor(client->LookupRows(RequestsTable_, nameTable, keyRange))
        .ValueOrThrow();

    if (result->GetRows().Size() == 0) {
        return false;
    }

    *state = TRequestState::FromRow(TNameTable::FromSchema(result->Schema()), result->GetRows()[0]);
    return true;
}

void TTables::WriteRequest(
    const NApi::ITransactionPtr& tx,
    const NTableClient::TNameTablePtr& nameTable,
    const NTableClient::TRowBufferPtr& rowBuffer,
    const TRequestKey& key,
    const TRequestState& state)
{
    auto row = ToRow(key, state, nameTable, rowBuffer);
    auto rowRange = MakeSharedRange(std::vector<TUnversionedRow>{row}, rowBuffer);

    tx->WriteRows(RequestsTable_, nameTable, rowRange);
}

void TTables::DeleteRequest(
    const ITransactionPtr& tx,
    const TNameTablePtr& nameTable,
    const TRowBufferPtr& rowBuffer,
    const TRequestKey& key)
{
    auto rowKey = key.ToRow(nameTable, rowBuffer);
    auto keyRange = MakeSharedRange(std::vector<TKey>{rowKey}, rowBuffer);

    tx->DeleteRows(RequestsTable_, nameTable, keyRange);
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NSkynetManager
} // namespace NYT
