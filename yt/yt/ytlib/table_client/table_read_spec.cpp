#include "table_read_spec.h"

#include "private.h"
#include "helpers.h"
#include "chunk_meta_extensions.h"

#include <yt/ytlib/chunk_client/helpers.h>
#include <yt/ytlib/chunk_client/data_source.h>
#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/connection.h>

#include <yt/ytlib/object_client/helpers.h>
#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/client/chunk_client/proto/chunk_spec.pb.h>

#include <yt/core/misc/protobuf_helpers.h>

namespace NYT::NTableClient {

using namespace NChunkClient;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NYTree;
using namespace NApi;
using namespace NConcurrency;
using namespace NYson;

using NChunkClient::NProto::TChunkSpec;

////////////////////////////////////////////////////////////////////////////////

TTableReadSpec FetchSingleTableReadSpec(const TFetchSingleTableReadSpecOptions& options)
{
    const auto& path = options.RichPath.GetPath();
    auto suppressAccessTracking = options.GetUserObjectBasicAttributesOptions.SuppressAccessTracking;
    auto suppressExpirationTimeoutRenewal = options.GetUserObjectBasicAttributesOptions.SuppressExpirationTimeoutRenewal;

    auto Logger = NLogging::TLogger(TableClientLogger)
        .AddTag("Path: %v, TransactionId: %v, ReadSessionId: %v",
            path,
            options.TransactionId,
            options.ReadSessionId);

    YT_LOG_INFO("Opening table reader");

    auto userObject = std::make_unique<TUserObject>(options.RichPath);

    GetUserObjectBasicAttributes(
        options.Client,
        {userObject.get()},
        options.TransactionId,
        Logger,
        EPermission::Read,
        options.GetUserObjectBasicAttributesOptions);

    if (userObject->ObjectId) {
        if (userObject->Type != EObjectType::Table) {
            THROW_ERROR_EXCEPTION("Invalid type of %v: expected %Qlv, actual %Qlv",
                path,
                EObjectType::Table,
                userObject->Type);
        }
    } else {
        YT_LOG_INFO("Table is virtual");
    }

    int chunkCount;
    bool dynamic;
    TTableSchemaPtr schema;
    {
        YT_LOG_INFO("Requesting extended table attributes");

        auto channel = options.Client->GetMasterChannelOrThrow(
            EMasterChannelKind::Follower,
            userObject->ExternalCellTag);

        TObjectServiceProxy proxy(channel);

        // NB: objectId is null for virtual tables.
        auto req = TYPathProxy::Get(userObject->GetObjectIdPathIfAvailable() + "/@");
        if (userObject->ObjectId) {
            AddCellTagToSyncWith(req, userObject->ObjectId);
        }
        SetTransactionId(req, userObject->ExternalTransactionId);
        SetSuppressAccessTracking(req, suppressAccessTracking);
        SetSuppressExpirationTimeoutRenewal(req, suppressExpirationTimeoutRenewal);
        NYT::ToProto(req->mutable_attributes()->mutable_keys(), std::vector<TString>{
            "chunk_count",
            "dynamic",
            "retained_timestamp",
            "schema",
            "unflushed_timestamp",
            "enable_dynamic_store_read",
        });

        auto rspOrError = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error requesting extended attributes of table %v",
            path);

        const auto& rsp = rspOrError.Value();
        auto attributes = ConvertToAttributes(TYsonString(rsp->value()));

        chunkCount = attributes->Get<int>("chunk_count");
        dynamic = attributes->Get<bool>("dynamic");
        schema = attributes->Get<TTableSchemaPtr>("schema");

        ValidateDynamicTableTimestamp(options.RichPath, dynamic, *schema, *attributes);
    }

    std::vector<TChunkSpec> chunkSpecs;

    {
        YT_LOG_INFO("Fetching table chunks (ChunkCount: %v)",
            chunkCount);

        chunkSpecs = FetchChunkSpecs(
            options.Client,
            options.Client->GetNativeConnection()->GetNodeDirectory(),
            *userObject,
            options.RichPath.GetRanges(),
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
            /* skipUnavailableChunks */ options.UnavailableChunkStrategy == EUnavailableChunkStrategy::Skip);

        CheckUnavailableChunks(options.UnavailableChunkStrategy, &chunkSpecs);
    }

    TDataSource dataSource;
    if (dynamic && schema->IsSorted()) {
        dataSource = MakeVersionedDataSource(
            path,
            schema,
            options.RichPath.GetColumns(),
            userObject->OmittedInaccessibleColumns,
            options.RichPath.GetTimestamp().value_or(AsyncLastCommittedTimestamp),
            options.RichPath.GetRetentionTimestamp().value_or(NullTimestamp));
    } else {
        dataSource = MakeUnversionedDataSource(
            path,
            schema,
            options.RichPath.GetColumns(),
            userObject->OmittedInaccessibleColumns);
    }

    auto dataSourceDirectory = New<TDataSourceDirectory>();
    dataSourceDirectory->DataSources().emplace_back(std::move(dataSource));

    return TTableReadSpec{
        .DataSourceDirectory = std::move(dataSourceDirectory),
        .ChunkSpecs = std::move(chunkSpecs),
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
