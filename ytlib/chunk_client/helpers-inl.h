#pragma once
#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
#endif

#include "private.h"

#include <yt/ytlib/api/native/client.h>

#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/object_client/public.h>
#include <yt/client/object_client/helpers.h>
#include <yt/ytlib/object_client/object_service_proxy.h>
#include <yt/ytlib/object_client/object_ypath_proxy.h>

#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/misc/checksum.h>

#include <yt/core/ytree/permission.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void GetUserObjectBasicAttributes(
    NApi::NNative::IClientPtr client,
    TMutableRange<T> objects,
    const NObjectClient::TTransactionId& defaultTransactionId,
    const NLogging::TLogger& logger,
    NYTree::EPermission permission,
    bool suppressAccessTracking)
{
    const auto& Logger = logger;

    LOG_INFO("Getting basic attributes of user objects");

    auto channel = client->GetMasterChannelOrThrow(NApi::EMasterChannelKind::Follower);
    NObjectClient::TObjectServiceProxy proxy(channel);

    auto batchReq = proxy.ExecuteBatch();

    for (auto iterator = objects.Begin(); iterator != objects.End(); ++iterator) {
        const auto& userObject = *iterator;
        auto req = NObjectClient::TObjectYPathProxy::GetBasicAttributes(userObject.GetPath());
        req->set_permissions(static_cast<ui32>(permission));
        auto transactionId = userObject.TransactionId.Get(defaultTransactionId);
        NCypressClient::SetTransactionId(req, transactionId);
        NCypressClient::SetSuppressAccessTracking(req, suppressAccessTracking);
        batchReq->AddRequest(req, "get_basic_attributes");
    }

    auto batchRspOrError = NConcurrency::WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(batchRspOrError, "Error getting basic attributes of user objects");
    const auto& batchRsp = batchRspOrError.Value();

    auto rspsOrError = batchRsp->GetResponses<NObjectClient::TObjectYPathProxy::TRspGetBasicAttributes>("get_basic_attributes");
    for (auto iterator = objects.Begin(); iterator != objects.End(); ++iterator) {
        auto& userObject = *iterator;
        const auto& path = userObject.GetPath();
        const auto& rspOrError = rspsOrError[iterator - objects.Begin()];
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error getting basic attributes of user object %v",
            path);
        const auto& rsp = rspOrError.Value();

        userObject.ObjectId = NYT::FromProto<NObjectClient::TObjectId>(rsp->object_id());
        userObject.CellTag = rsp->cell_tag();

        userObject.Type = NObjectClient::TypeFromId(userObject.ObjectId);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TRpcPtr>
std::vector<TBlock> GetRpcAttachedBlocks(const TRpcPtr& rpc, bool validateChecksums)
{
    if (rpc->block_checksums_size() != 0 && rpc->Attachments().size() != rpc->block_checksums_size()) {
        THROW_ERROR_EXCEPTION("Number of RPC attachments does not match the number of checksums")
            << TErrorAttribute("attachment_count", rpc->block_checksums_size())
            << TErrorAttribute("checksum_count", rpc->Attachments().size());
    }

    std::vector<TBlock> blocks;
    blocks.reserve(rpc->Attachments().size());
    for (int i = 0; i < rpc->Attachments().size(); ++i) {
        auto checksum = NullChecksum;
        if (rpc->block_checksums_size() != 0) {
            checksum = rpc->block_checksums(i);
        }

        blocks.emplace_back(rpc->Attachments()[i], checksum);

        if (validateChecksums && !blocks.back().IsChecksumValid()) {
            THROW_ERROR_EXCEPTION("Invalid block checksum")
                << TErrorAttribute("block_index", i);
        }
    }

    return blocks;
}

template <class TRpcPtr>
void SetRpcAttachedBlocks(const TRpcPtr& rpc, const std::vector<TBlock>& blocks)
{
    rpc->Attachments().reserve(blocks.size());
    for (const auto& block : blocks) {
        rpc->Attachments().push_back(block.Data);
        rpc->add_block_checksums(block.Checksum);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
