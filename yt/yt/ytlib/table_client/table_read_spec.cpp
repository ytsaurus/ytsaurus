#include "table_read_spec.h"

#include "helpers.h"
#include "chunk_meta_extensions.h"

#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/object_client/helpers.h>
#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/client/table_client/private.h>
#include <yt/yt/client/table_client/timestamped_schema_helpers.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_spec.pb.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NTableClient {

using namespace NChunkClient;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NYTree;
using namespace NApi;
using namespace NConcurrency;
using namespace NYson;
using namespace NLogging;

using NChunkClient::NProto::TChunkSpec;

////////////////////////////////////////////////////////////////////////////////

TTableReadSpec FetchRegularTableReadSpec(
    const NYPath::TRichYPath& path,
    const NNative::IClientPtr& client,
    const TFetchSingleTableReadSpecOptions& options,
    const TUserObject* userObject,
    const TLogger& logger)
{
    const auto& Logger = logger;
    YT_LOG_INFO("Fetching regular table");

    auto tablePath = path.GetPath();
    auto suppressAccessTracking = options.GetUserObjectBasicAttributesOptions.SuppressAccessTracking;
    auto suppressExpirationTimeoutRenewal = options.GetUserObjectBasicAttributesOptions.SuppressExpirationTimeoutRenewal;

    int chunkCount;
    bool dynamic;
    TTableSchemaPtr schema;
    bool fetchFromTablets;
    std::string account;
    {
        YT_LOG_INFO("Requesting extended table attributes");

        auto proxy = CreateObjectServiceReadProxy(
            client,
            EMasterChannelKind::Follower,
            userObject->ExternalCellTag);

        // NB: objectId is null for virtual tables.
        auto req = TYPathProxy::Get(userObject->GetObjectIdPathIfAvailable() + "/@");
        if (userObject->ObjectId) {
            AddCellTagToSyncWith(req, userObject->ObjectId);
        }
        SetTransactionId(req, userObject->ExternalTransactionId);
        SetSuppressAccessTracking(req, suppressAccessTracking);
        SetSuppressExpirationTimeoutRenewal(req, suppressExpirationTimeoutRenewal);
        // TODO(danilalexeev): Figure out why request ignores the Sequoia resolve.
        SetAllowResolveFromSequoiaObject(req, true);
        NYT::ToProto(req->mutable_attributes()->mutable_keys(), std::vector<std::string>{
            "chunk_count",
            "dynamic",
            "retained_timestamp",
            "schema",
            "unflushed_timestamp",
            "enable_dynamic_store_read",
            "fetch_from_tablets",
            "account",
        });

        auto rspOrError = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error requesting extended attributes of table %v",
            tablePath);

        const auto& rsp = rspOrError.Value();
        auto attributes = ConvertToAttributes(TYsonString(rsp->value()));

        chunkCount = attributes->Get<int>("chunk_count");
        dynamic = attributes->Get<bool>("dynamic");
        schema = attributes->Get<TTableSchemaPtr>("schema");
        // NB: Works only for read_table, map-reduce operations do not pass rich path options.
        if (path.GetVersionedReadOptions().ReadMode == EVersionedIOMode::LatestTimestamp) {
            schema = ToLatestTimestampSchema(schema);
        }
        fetchFromTablets = attributes->Get<bool>("fetch_from_tablets", false);
        account = attributes->Get<std::string>("account", "");

        ValidateDynamicTableTimestamp(path, dynamic, *schema, *attributes);
    }

    std::vector<TChunkSpec> chunkSpecs;

    {
        if (fetchFromTablets) {
            YT_LOG_INFO("Fetching table chunks from tablets");
            chunkSpecs = FetchTabletStores(
                client,
                *userObject,
                path.GetNewRanges(schema->ToComparator(), schema->GetKeyColumnTypes()),
                logger);
        } else {
            YT_LOG_INFO("Fetching table chunks (ChunkCount: %v)",
                chunkCount);

            chunkSpecs = FetchChunkSpecs(
                client,
                client->GetNativeConnection()->GetNodeDirectory(),
                *userObject,
                path.GetNewRanges(schema->ToComparator(), schema->GetKeyColumnTypes()),
                // XXX(babenko): YT-11825
                dynamic && !schema->IsSorted() ? -1 : chunkCount,
                options.FetchChunkSpecConfig->MaxChunksPerFetch,
                options.FetchChunkSpecConfig->MaxChunksPerLocateRequest,
                [&] (const TChunkOwnerYPathProxy::TReqFetchPtr& req) {
                    req->set_fetch_all_meta_extensions(false);
                    req->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
                    req->add_extension_tags(TProtoExtensionTag<NTableClient::NProto::TBoundaryKeysExt>::Value);
                    req->set_fetch_parity_replicas(options.FetchParityReplicas);
                    AddCellTagToSyncWith(req, userObject->ObjectId);
                    SetTransactionId(req, userObject->ExternalTransactionId);
                    SetSuppressAccessTracking(req, suppressAccessTracking);
                    SetSuppressExpirationTimeoutRenewal(req, suppressExpirationTimeoutRenewal);
                },
                Logger,
                /*skipUnavailableChunks*/ options.UnavailableChunkStrategy == EUnavailableChunkStrategy::Skip);

            CheckUnavailableChunks(
                options.UnavailableChunkStrategy,
                options.ChunkAvailabilityPolicy,
                &chunkSpecs);
        }
    }

    TDataSourcePtr dataSource;
    std::vector<TDataSliceDescriptor> dataSliceDescriptors;
    if (dynamic && schema->IsSorted()) {
        dataSource = MakeVersionedDataSource(
            tablePath,
            schema,
            path.GetColumns(),
            userObject->OmittedInaccessibleColumns,
            path.GetTimestamp().value_or(AsyncLastCommittedTimestamp),
            path.GetRetentionTimestamp().value_or(NullTimestamp),
            /*columnRenameDescriptors*/ {});
        dataSource->SetObjectId(userObject->ObjectId);
        dataSource->SetAccount(account);
        dataSliceDescriptors.emplace_back(std::move(chunkSpecs));
    } else {
        dataSource = MakeUnversionedDataSource(
            tablePath,
            schema,
            path.GetColumns(),
            userObject->OmittedInaccessibleColumns,
            /*columnRenameDescriptors*/ {});
        dataSource->SetObjectId(userObject->ObjectId);
        dataSource->SetAccount(account);
        for (auto& chunkSpec : chunkSpecs) {
            dataSliceDescriptors.emplace_back(std::move(chunkSpec));
        }
    }
    if (userObject->RowLevelAcl) {
        dataSource->SetRlsReadSpec(
            TRlsReadSpec::BuildFromRowLevelAclAndTableSchema(
                schema,
                *userObject->RowLevelAcl,
                Logger));
    }

    auto dataSourceDirectory = New<TDataSourceDirectory>();
    dataSourceDirectory->DataSources().emplace_back(std::move(dataSource));

    return TTableReadSpec{
        .DataSourceDirectory = std::move(dataSourceDirectory),
        .DataSliceDescriptors = std::move(dataSliceDescriptors),
    };
}

TTableReadSpec FetchSingleTableReadSpec(
    const NYPath::TRichYPath& path,
    const NNative::IClientPtr& client,
    const TFetchSingleTableReadSpecOptions& options)
{
    auto tablePath = path.GetPath();

    auto Logger = TableClientLogger().WithTag("Path: %v, TransactionId: %v, ReadSessionId: %v",
        tablePath,
        options.TransactionId,
        options.ReadSessionId);

    YT_LOG_INFO("Fetching table read spec");

    auto userObject = std::make_unique<TUserObject>(path);

    GetUserObjectBasicAttributes(
        client,
        {userObject.get()},
        options.TransactionId,
        Logger,
        EPermission::Read,
        options.GetUserObjectBasicAttributesOptions);

    EObjectType type;

    if (userObject->ObjectId) {
        type = userObject->Type;
    } else {
        YT_LOG_INFO("Table is virtual");
        // Just assume this is indeed a table.
        type = EObjectType::Table;
    }

    switch (type) {
        case EObjectType::Table:
            return FetchRegularTableReadSpec(path, client, options, userObject.get(), Logger);
        default:
            THROW_ERROR_EXCEPTION("Invalid type of %v: expected %Qlv, actual %Qlv",
                tablePath,
                EObjectType::Table,
                userObject->Type);
    }
}

////////////////////////////////////////////////////////////////////////////////

TTableReadSpec JoinTableReadSpecs(std::vector<TTableReadSpec>& tableReadSpecs)
{
    if (tableReadSpecs.empty()) {
        return {};
    }

    // Calculate total counts and use them for reserve().
    size_t totalDataSliceCount = 0;
    size_t totalDataSourceCount = 0;
    for (const auto& tableReadSpec : tableReadSpecs) {
        totalDataSliceCount += tableReadSpec.DataSliceDescriptors.size();
        totalDataSourceCount += tableReadSpec.DataSourceDirectory->DataSources().size();
    }

    // Use first table read spec as the resulting one. In particular, when
    // joining single table read spec, method always costs nothing.
    TTableReadSpec result = std::move(tableReadSpecs.front());
    result.DataSliceDescriptors.reserve(totalDataSliceCount);
    result.DataSourceDirectory->DataSources().reserve(totalDataSourceCount);

    size_t dataSourceIndexOffset = result.DataSourceDirectory->DataSources().size();

    for (size_t index = 1; index < tableReadSpecs.size(); ++index) {
        auto& tableReadSpec = tableReadSpecs[index];
        for (auto& dataSliceDescriptor : tableReadSpec.DataSliceDescriptors) {
            for (auto& chunkSpec : dataSliceDescriptor.ChunkSpecs) {
                chunkSpec.set_table_index(chunkSpec.table_index() + dataSourceIndexOffset);
            }
            result.DataSliceDescriptors.emplace_back(std::move(dataSliceDescriptor));
        }
        dataSourceIndexOffset += tableReadSpec.DataSourceDirectory->DataSources().size();
        for (auto& dataSource : tableReadSpec.DataSourceDirectory->DataSources()) {
            result.DataSourceDirectory->DataSources().emplace_back(std::move(dataSource));
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
