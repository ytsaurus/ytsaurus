#include "helpers.h"

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/ytlib/chunk_client/config.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/object_client/helpers.h>
#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/permission.h>
#include <yt/yt/core/ytree/ypath_proxy.h>

namespace NYT::NFileClient {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NYson;
using namespace NYTree;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

TMultiChunkWriterOptionsPtr GetWriterOptions(
    const IAttributeDictionaryPtr& attributes,
    const TRichYPath& path,
    const IMemoryUsageTrackerPtr& tracker)
{
    auto writerOptions = New<TMultiChunkWriterOptions>();

    auto attributesCompressionCodec = attributes->Get<NCompression::ECodec>("compression_codec");
    auto attributesErasureCodec = attributes->Get<NErasure::ECodec>("erasure_codec");

    writerOptions->ReplicationFactor = attributes->Get<int>("replication_factor");
    writerOptions->MediumName = attributes->Get<std::string>("primary_medium");
    writerOptions->Account = attributes->Get<std::string>("account");
    writerOptions->CompressionCodec = path.GetCompressionCodec().value_or(attributesCompressionCodec);
    writerOptions->ErasureCodec = path.GetErasureCodec().value_or(attributesErasureCodec);

    writerOptions->EnableStripedErasure = attributes->Get<bool>("enable_striped_erasure", false);
    writerOptions->MemoryUsageTracker = tracker;

    return writerOptions;
}

////////////////////////////////////////////////////////////////////////////////

TFileObjectInfo FetchFileObjectInfo(
    const NNative::IClientPtr& client,
    const TYPath& path,
    NTransactionClient::TTransactionId transactionId,
    const TFetchFileObjectInfoOptions& options,
    const NLogging::TLogger& logger)
{
    TFileObjectInfo info;
    auto& userObject = info.UserObject;
    userObject = TUserObject(path);

    GetUserObjectBasicAttributes(
        client,
        {&userObject},
        transactionId,
        logger,
        EPermission::Read,
        TGetUserObjectBasicAttributesOptions{
            .SuppressAccessTracking = options.SuppressAccessTracking,
            .SuppressExpirationTimeoutRenewal = options.SuppressExpirationTimeoutRenewal,
        });

    if (userObject.Type != EObjectType::File) {
        THROW_ERROR_EXCEPTION("Invalid type of %v: expected %Qlv, actual %Qlv",
            path,
            EObjectType::File,
            userObject.Type);
    }

    {
        auto proxy = CreateObjectServiceReadProxy(
            client,
            EMasterChannelKind::Follower,
            userObject.ExternalCellTag);
        auto req = TYPathProxy::Get(userObject.GetObjectIdPath() + "/@");
        ToProto(req->mutable_attributes()->mutable_keys(), std::vector<std::string>{
            "account",
            "revision",
            "chunk_count",
        });
        AddCellTagToSyncWith(req, userObject.ObjectId);
        SetTransactionId(req, userObject.ExternalTransactionId);
        SetSuppressAccessTracking(req, options.SuppressAccessTracking);
        SetSuppressExpirationTimeoutRenewal(req, options.SuppressExpirationTimeoutRenewal);

        auto rspOrError = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error requesting extended attributes of file %v",
            path);
        const auto& rsp = rspOrError.Value();

        auto attributes = ConvertToAttributes(TYsonString(rsp->value()));
        info.Revision = attributes->Get<NHydra::TRevision>("revision", NHydra::NullRevision);
        info.ChunkCount = attributes->Get<int>("chunk_count");
        userObject.Account = attributes->Get<std::string>("account");
    }

    return info;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFileClient
