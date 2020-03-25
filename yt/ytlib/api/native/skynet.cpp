#include "skynet.h"
#include "client.h"
#include "private.h"
#include "connection.h"
#include "rpc_helpers.h"

#include <yt/client/api/skynet.h>

#include <yt/client/object_client/helpers.h>

#include <yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/ytlib/chunk_client/private.h>
#include <yt/ytlib/chunk_client/helpers.h>
#include <yt/ytlib/chunk_client/dispatcher.h>
#include <yt/ytlib/chunk_client/chunk_owner_ypath_proxy.h>

#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/object_client/object_ypath_proxy.h>
#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/ytlib/table_client/table_ypath_proxy.h>

#include <yt/core/ytree/fluent.h>

#include <yt/core/ypath/public.h>

#include <yt/core/yson/consumer.h>

namespace NYT::NApi::NNative {

using namespace NConcurrency;
using namespace NYson;
using namespace NYTree;
using namespace NObjectClient;
using namespace NChunkClient;
using namespace NTableClient;
using namespace NTableClient::NProto;
using namespace NCypressClient;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;

////////////////////////////////////////////////////////////////////////////////

TSkynetSharePartsLocationsPtr DoLocateSkynetShare(
    const IClientPtr& client,
    const NYPath::TRichYPath& richPath,
    const TLocateSkynetShareOptions& options)
{
    auto Logger = NLogging::TLogger(ApiLogger)
        .AddTag("Path: %v", richPath.GetPath());

    TGetUserObjectBasicAttributesOptions getAttributesOptions;
    getAttributesOptions.ReadFrom = EMasterChannelKind::Cache;

    TUserObject userObject(richPath);

    GetUserObjectBasicAttributes(
        client,
        {&userObject},
        NullTransactionId,
        ChunkClientLogger,
        EPermission::Read,
        getAttributesOptions);

    if (userObject.Type != EObjectType::Table) {
        THROW_ERROR_EXCEPTION("Invalid type of %v: expected %Qlv, actual %Qlv",
            richPath,
            EObjectType::Table,
            userObject.Type);
    }

    int chunkCount;
    // XXX(babenko): YT-11825
    bool dynamic;
    bool sorted;
    {
        YT_LOG_INFO("Requesting chunk count");

        auto channel = client->GetMasterChannelOrThrow(EMasterChannelKind::Cache, userObject.ExternalCellTag);
        TObjectServiceProxy proxy(channel);

        auto masterReadOptions = TMasterReadOptions{
            .ReadFrom = EMasterChannelKind::Cache
        };

        auto batchReq = proxy.ExecuteBatch();
        SetBalancingHeader(batchReq, client->GetNativeConnection()->GetConfig(), masterReadOptions);

        auto req = TYPathProxy::Get(userObject.GetObjectIdPath() + "/@");
        SetCachingHeader(req, client->GetNativeConnection()->GetConfig(), masterReadOptions);
        SetSuppressAccessTracking(req, false);
        ToProto(req->mutable_attributes()->mutable_keys(), std::vector<TString>{
            "chunk_count",
            "dynamic",
            "sorted"
        });
        batchReq->AddRequest(req);

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error getting table chunk count %v",
            richPath);

        const auto& batchRsp = batchRspOrError.Value();
        auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>(0).Value();
        auto attributes = ConvertToAttributes(TYsonString(rsp->value()));

        chunkCount = attributes->Get<int>("chunk_count");
        dynamic = attributes->Get<bool>("dynamic");
        sorted = attributes->Get<bool>("sorted");
    }

    auto skynetShareLocations = New<TSkynetSharePartsLocations>();

    YT_LOG_INFO("Fetching table chunks");

    skynetShareLocations->ChunkSpecs = FetchChunkSpecs(
        client,
        skynetShareLocations->NodeDirectory,
        userObject,
        richPath.GetRanges(),
        // XXX(babenko): YT-11825
        dynamic && !sorted ? -1 : chunkCount,
        options.Config->MaxChunksPerFetch,
        options.Config->MaxChunksPerLocateRequest,
        [&] (TChunkOwnerYPathProxy::TReqFetchPtr req) {
            req->set_fetch_all_meta_extensions(false);
            SetSuppressAccessTracking(req, false);
        },
        Logger,
        false,
        EAddressType::SkynetHttp);

    return skynetShareLocations;
}

TFuture<TSkynetSharePartsLocationsPtr> LocateSkynetShare(
    const IClientPtr& client,
    const NYPath::TRichYPath& path,
    const TLocateSkynetShareOptions& options)
{
    return BIND(DoLocateSkynetShare, client, path, options)
        .AsyncVia(NChunkClient::TDispatcher::Get()->GetReaderInvoker())
        .Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
