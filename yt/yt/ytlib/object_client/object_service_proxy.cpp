#include "object_service_proxy.h"

#include "config.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/rpc_helpers.h>

#include <yt/yt/ytlib/object_client/proto/object_ypath.pb.h>

#include <yt/yt/ytlib/cell_master_client/cell_directory.h>

#include <yt/yt/core/misc/checksum.h>

#include <yt/yt/core/rpc/message.h>
#include <yt/yt/core/rpc/helpers.h>

#include <utility>

namespace NYT::NObjectClient {

using namespace NYTree;
using namespace NObjectClient;
using namespace NRpc;
using namespace NApi::NNative;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ObjectClientLogger;

const auto ExecuteMethodDescriptor = TMethodDescriptor("Execute");

TReqExecuteBatchRetriesConfigPtr GetSequoiaRetriesConfig(const IConnectionPtr& connection)
{
    return connection->GetConfig()->SequoiaConnection->Retries->ToRetriesConfig();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TObjectServiceProxy::TObjectServiceProxy(
    IClientPtr client,
    NApi::EMasterChannelKind masterChannelKind,
    TCellTag cellTag,
    TStickyGroupSizeCachePtr stickyGroupSizeCache)
    : TProxyBase(
        client->GetCypressChannelOrThrow(masterChannelKind, cellTag),
        GetDescriptor())
    , StickyGroupSizeCache_(std::move(stickyGroupSizeCache))
    , SequoiaRetriesConfig_(GetSequoiaRetriesConfig(client->GetNativeConnection()))
{ }

TObjectServiceProxy::TObjectServiceProxy(
    IConnectionPtr connection,
    NApi::EMasterChannelKind masterChannelKind,
    TCellTag cellTag,
    TStickyGroupSizeCachePtr stickyGroupSizeCache)
    : TProxyBase(
        connection->GetCypressChannelOrThrow(masterChannelKind, cellTag),
        GetDescriptor())
    , StickyGroupSizeCache_(std::move(stickyGroupSizeCache))
    , SequoiaRetriesConfig_(GetSequoiaRetriesConfig(connection))
{ }

TObjectServiceProxy::TObjectServiceProxy(
    IConnectionPtr connection,
    NApi::EMasterChannelKind masterChannelKind,
    TCellTag cellTag,
    TStickyGroupSizeCachePtr stickyGroupSizeCache,
    TAuthenticationIdentity identity)
    : TProxyBase(
        CreateAuthenticatedChannel(
            connection->GetCypressChannelOrThrow(masterChannelKind, cellTag),
            identity),
        GetDescriptor())
    , StickyGroupSizeCache_(std::move(stickyGroupSizeCache))
    , SequoiaRetriesConfig_(GetSequoiaRetriesConfig(connection))
{ }

TObjectServiceProxy TObjectServiceProxy::FromDirectMasterChannel(IChannelPtr channel)
{
    return TObjectServiceProxy(std::move(channel));
}

TStickyGroupSizeCache::TKey TObjectServiceProxy::TReqExecuteSubbatch::TInnerRequestDescriptor::GetKey() const
{
    return {Key, Message};
}

TObjectServiceProxy::TReqExecuteSubbatch::TReqExecuteSubbatch(
    IChannelPtr channel,
    int subbatchSize,
    TStickyGroupSizeCachePtr stickyGroupSizeCache)
    : TClientRequest(
        std::move(channel),
        TObjectServiceProxy::GetDescriptor(),
        ExecuteMethodDescriptor)
    , StickyGroupSizeCache_(std::move(stickyGroupSizeCache))
    , SubbatchSize_(subbatchSize)
{
    YT_VERIFY(SubbatchSize_ > 0);
    SetResponseHeavy(true);
}

TObjectServiceProxy::TReqExecuteSubbatch::TReqExecuteSubbatch(
    const TReqExecuteSubbatch& other,
    std::vector<TInnerRequestDescriptor>&& innerRequestDescriptors)
    : TClientRequest(other)
    , StickyGroupSizeCache_(other.StickyGroupSizeCache_)
    , SubbatchSize_(other.SubbatchSize_)
    , InnerRequestDescriptors_(std::move(innerRequestDescriptors))
    , OriginalRequestId_(other.OriginalRequestId_)
    , SuppressUpstreamSync_(other.SuppressUpstreamSync_)
    , SuppressTransactionCoordinatorSync_(other.SuppressTransactionCoordinatorSync_)
{
    // Undo some work done by the base class's copy ctor and make some tweaks.
    // XXX(babenko): refactor this, maybe avoid TClientRequest copying.
    Attachments().clear();
}

int TObjectServiceProxy::TReqExecuteSubbatch::GetSize() const
{
    return std::ssize(InnerRequestDescriptors_);
}

TFuture<TObjectServiceProxy::TRspExecuteBatchPtr>
TObjectServiceProxy::TReqExecuteSubbatch::DoInvoke()
{
    // Prepare attachments.
    bool mutating = false;
    for (const auto& descriptor : InnerRequestDescriptors_) {
        mutating = mutating || descriptor.Mutating;
        if (const auto& message = descriptor.Message) {
            Attachments_.insert(Attachments_.end(), message.Begin(), message.End());
        }
    }

    auto& header = Header();
    ToProto(header.mutable_request_id(), TRequestId::Create());
    header.set_logical_request_weight(GetSize());

    auto* ypathExt = header.MutableExtension(NYTree::NProto::TYPathHeaderExt::ypath_header_ext);
    ypathExt->set_mutating(mutating);

    auto batchRsp = New<TRspExecuteBatch>(CreateClientContext(), InnerRequestDescriptors_, StickyGroupSizeCache_);
    auto promise = batchRsp->GetPromise();
    if (GetSize() == 0) {
        batchRsp->SetEmpty();
    } else {
        auto requestControl = Send(batchRsp);
        promise.OnCanceled(BIND([=] (const TError& /*error*/) {
            requestControl->Cancel();
        }));
    }
    return promise;
}

TSharedRefArray TObjectServiceProxy::TReqExecuteSubbatch::SerializeHeaderless() const
{
    NProto::TReqExecute req;
    if (OriginalRequestId_) {
        ToProto(req.mutable_original_request_id(), OriginalRequestId_);
    }

    // COMPAT(shakurov): remove in favor of proto ext (see SetMulticellSyncHeader).
    req.set_suppress_upstream_sync(SuppressUpstreamSync_);
    req.set_suppress_transaction_coordinator_sync(SuppressTransactionCoordinatorSync_);

    req.set_allow_backoff(true);
    req.set_supports_portals(true);

    if (Header().HasExtension(NRpc::NProto::TBalancingExt::balancing_ext)) {
        auto currentStickyGroupSize = Header().GetExtension(NRpc::NProto::TBalancingExt::balancing_ext).sticky_group_size();
        req.set_current_sticky_group_size(currentStickyGroupSize);
    }

    for (const auto& descriptor : InnerRequestDescriptors_) {
        req.add_part_counts(descriptor.Message.Size());
    }

    // COMPAT(danilalexeev): legacy RPC codecs.
    auto body = EnableLegacyRpcCodecs_
        ? SerializeProtoToRefWithEnvelope(req, RequestCodec_)
        : SerializeProtoToRefWithCompression(req, RequestCodec_);

    std::vector<TSharedRef> data;
    data.reserve(Attachments_.size() + 1);
    data.push_back(std::move(body));
    data.insert(data.end(), Attachments_.begin(), Attachments_.end());

    return TSharedRefArray(std::move(data), TSharedRefArray::TMoveParts{});
}

size_t TObjectServiceProxy::TReqExecuteSubbatch::ComputeHash() const
{
    size_t hash = 0;
    HashCombine(hash, SuppressUpstreamSync_);
    HashCombine(hash, SuppressTransactionCoordinatorSync_);
    for (const auto& descriptor : InnerRequestDescriptors_) {
        if (descriptor.Hash) {
            HashCombine(hash, descriptor.Hash);
        } else {
            for (auto* it = descriptor.Message.Begin(); it != descriptor.Message.End(); ++it) {
                HashCombine(hash, GetChecksum(*it));
            }
        }
    }
    return hash;
}

////////////////////////////////////////////////////////////////////////////////

TObjectServiceProxy::TReqExecuteBatchBase::TReqExecuteBatchBase(
    IChannelPtr channel,
    int subbatchSize,
    TStickyGroupSizeCachePtr stickyGroupSizeCache)
    : TReqExecuteSubbatch(
        std::move(channel),
        subbatchSize,
        std::move(stickyGroupSizeCache))
{ }

TObjectServiceProxy::TReqExecuteBatchBase::TReqExecuteBatchBase(
    const TReqExecuteBatchBase& other,
    std::vector<TInnerRequestDescriptor>&& innerRequestDescriptors)
    : TReqExecuteSubbatch(other, std::move(innerRequestDescriptors))
{ }

TFuture<TObjectServiceProxy::TRspExecuteBatchPtr> TObjectServiceProxy::TReqExecuteBatchBase::Invoke()
{
    PushDownPrerequisites();
    return DoInvoke();
}

void TObjectServiceProxy::TReqExecuteBatchBase::SetOriginalRequestId(TRequestId originalRequestId)
{
    OriginalRequestId_ = originalRequestId;
}

void TObjectServiceProxy::TReqExecuteBatchBase::SetSuppressUpstreamSync(bool value)
{
    SuppressUpstreamSync_ = value;
}

void TObjectServiceProxy::TReqExecuteBatchBase::SetSuppressTransactionCoordinatorSync(bool value)
{
    SuppressTransactionCoordinatorSync_ = value;
}

void TObjectServiceProxy::TReqExecuteBatchBase::AddRequest(
    const TYPathRequestPtr& innerRequest,
    std::optional<std::string> key,
    std::optional<size_t> hash)
{
    const auto& ypathExt = innerRequest->Header().GetExtension(NYTree::NProto::TYPathHeaderExt::ypath_header_ext);

    InnerRequestDescriptors_.push_back({
        std::move(key),
        innerRequest->Tag(),
        innerRequest->Serialize(),
        hash,
        ypathExt.mutating(),
    });
}

void TObjectServiceProxy::TReqExecuteBatchBase::AddRequestMessage(
    TSharedRefArray innerRequestMessage,
    std::optional<std::string> key,
    std::any tag,
    std::optional<size_t> hash)
{
    NRpc::NProto::TRequestHeader header;
    YT_VERIFY(TryParseRequestHeader(innerRequestMessage, &header));

    const auto& ypathExt = header.GetExtension(NYTree::NProto::TYPathHeaderExt::ypath_header_ext);

    InnerRequestDescriptors_.push_back({
        std::move(key),
        std::move(tag),
        std::move(innerRequestMessage),
        hash,
        ypathExt.mutating()
    });
}

void TObjectServiceProxy::TReqExecuteBatchBase::PushDownPrerequisites()
{
    // Push TPrerequisitesExt down to individual requests.
    if (Header().HasExtension(NProto::TPrerequisitesExt::prerequisites_ext)) {
        const auto& batchPrerequisitesExt = Header().GetExtension(NProto::TPrerequisitesExt::prerequisites_ext);
        for (auto& descriptor : InnerRequestDescriptors_) {
            NRpc::NProto::TRequestHeader requestHeader;
            YT_VERIFY(TryParseRequestHeader(descriptor.Message, &requestHeader));

            auto* prerequisitesExt = requestHeader.MutableExtension(NProto::TPrerequisitesExt::prerequisites_ext);
            prerequisitesExt->mutable_transactions()->MergeFrom(batchPrerequisitesExt.transactions());
            prerequisitesExt->mutable_revisions()->MergeFrom(batchPrerequisitesExt.revisions());
            descriptor.Message = SetRequestHeader(descriptor.Message, requestHeader);
        }
        Header().ClearExtension(NProto::TPrerequisitesExt::prerequisites_ext);
    }
}

////////////////////////////////////////////////////////////////////////////////

TSharedRefArray TObjectServiceProxy::TReqExecuteBatchNoSequoiaRetries::PatchForRetry(const TSharedRefArray& message)
{
    NRpc::NProto::TRequestHeader header;
    YT_VERIFY(TryParseRequestHeader(message, &header));
    if (header.retry()) {
        // Already patched.
        return message;
    } else {
        header.set_retry(true);
        return SetRequestHeader(message, header);
    }
}

TFuture<TObjectServiceProxy::TRspExecuteBatchPtr> TObjectServiceProxy::TReqExecuteBatchNoSequoiaRetries::Invoke()
{
    InFlightSubbatchIndexToGlobalIndex_.reserve(SubbatchSize_);
    SetBalancingHeader();
    SetMulticellSyncHeader();
    FullResponsePromise_ = NewPromise<TRspExecuteBatchPtr>();
    PushDownPrerequisites();
    InvokeNextBatch();
    return FullResponsePromise_;
}

void TObjectServiceProxy::TReqExecuteBatchNoSequoiaRetries::SetStickyGroupSize(int value)
{
    StickyGroupSize_ = value;
}

void TObjectServiceProxy::TReqExecuteBatchNoSequoiaRetries::SetEnableClientStickiness(bool value)
{
    EnableClientStickiness_ = value;
}

TObjectServiceProxy::TObjectServiceProxy(IChannelPtr channel)
    : TProxyBase(std::move(channel), GetDescriptor())
    , SequoiaRetriesConfig_(New<TReqExecuteBatchRetriesConfig>())
{
    SequoiaRetriesConfig_->RetryCount = 0;
}

TObjectServiceProxy::TReqExecuteBatchNoSequoiaRetries::TReqExecuteBatchNoSequoiaRetries(
    const TReqExecuteBatchBase& other,
    std::vector<TInnerRequestDescriptor>&& innerRequestDescriptors)
    : TReqExecuteBatchBase(other, std::move(innerRequestDescriptors))
{ }

TObjectServiceProxy::TReqExecuteBatchNoSequoiaRetries::TReqExecuteBatchNoSequoiaRetries(
    IChannelPtr channel,
    int subbatchSize,
    TStickyGroupSizeCachePtr stickyGroupSizeCache)
    : TReqExecuteBatchBase(
        std::move(channel),
        subbatchSize,
        std::move(stickyGroupSizeCache))
{ }

TObjectServiceProxy::TReqExecuteSubbatchPtr
TObjectServiceProxy::TReqExecuteBatchNoSequoiaRetries::FormNextBatch()
{
    std::vector<TInnerRequestDescriptor> innerRequestDescriptors;
    innerRequestDescriptors.reserve(SubbatchSize_);

    YT_VERIFY(InFlightSubbatchIndexToGlobalIndex_.empty());

    std::optional<bool> mutating;

    for (auto i = GetFirstUnreceivedSubresponseIndex(); i < GetTotalSubrequestCount(); ++i) {
        if (IsSubresponseReceived(i)) {
            continue;
        }

        auto& descriptor = InnerRequestDescriptors_[i];

        if (!mutating.has_value()) {
            mutating = descriptor.Mutating;
        }

        if (mutating != descriptor.Mutating) {
            continue;
        }

        if (IsSubresponseUncertain(i)) {
            descriptor.Message = PatchForRetry(descriptor.Message);
        }

        InFlightSubbatchIndexToGlobalIndex_.push_back(i);
        innerRequestDescriptors.push_back(descriptor);

        if (std::ssize(innerRequestDescriptors) == SubbatchSize_) {
            break;
        }
    }

    return New<TReqExecuteSubbatch>(*this, std::move(innerRequestDescriptors));
}

void TObjectServiceProxy::TReqExecuteBatchNoSequoiaRetries::InvokeNextBatch()
{
    auto subbatchReq = FormNextBatch();
    CurrentReqFuture_ = subbatchReq->DoInvoke();
    YT_LOG_DEBUG("Subbatch request invoked (BatchRequestId: %v, SubbatchRequestId: %v, SubbatchSize: %v)",
        GetRequestId(),
        subbatchReq->GetRequestId(),
        subbatchReq->GetSize());

    CurrentReqFuture_.Subscribe(
        BIND(&TObjectServiceProxy::TReqExecuteBatchNoSequoiaRetries::OnSubbatchResponse, MakeStrong(this)));
}

TObjectServiceProxy::TRspExecuteBatchPtr TObjectServiceProxy::TReqExecuteBatchNoSequoiaRetries::GetFullResponse()
{
    if (!FullResponse_) {
        // Make sure the full response uses the promise we've returned to the caller.
        FullResponse_ = New<TRspExecuteBatch>(
            CreateClientContext(),
            InnerRequestDescriptors_,
            StickyGroupSizeCache_,
            FullResponsePromise_);
    }
    return FullResponse_;
}

void TObjectServiceProxy::TReqExecuteBatchNoSequoiaRetries::OnSubbatchResponse(const TErrorOr<TRspExecuteBatchPtr>& rspOrErr)
{
    auto isFirstBatch = IsFirstBatch_;
    IsFirstBatch_ = false;

    if (!rspOrErr.IsOK()) {
        FullResponsePromise_.Set(rspOrErr);
        return;
    }

    const auto& rsp = rspOrErr.Value();

    // Optimization for the typical case of a small batch.
    if (isFirstBatch && rsp->GetResponseCount() == GetTotalSubrequestCount()) {
        FullResponsePromise_.Set(rspOrErr);
        return;
    }

    YT_LOG_DEBUG("Subbatch response received (BatchRequestId: %v, SubbatchRequestId: %v, SubbatchSubresponseCount: %v)",
        GetRequestId(),
        rsp->GetRequestId(),
        rsp->GetResponseCount());

    // The remote side shouldn't backoff until there's at least one subresponse.
    YT_VERIFY(rsp->GetResponseCount() > 0 || GetTotalSubrequestCount() == 0);

    auto fullResponse = GetFullResponse();
    YT_VERIFY(rsp->GetSize() <= std::ssize(InFlightSubbatchIndexToGlobalIndex_));
    for (auto i = 0; i < rsp->GetSize(); ++i) {
        auto globalIndex = InFlightSubbatchIndexToGlobalIndex_[i];
        YT_ASSERT(!fullResponse->IsResponseReceived(globalIndex));

        if (rsp->IsResponseReceived(i)) {
            auto revision = rsp->GetRevision(i);
            auto attachmentRange = rsp->GetResponseAttachmentRange(i);
            fullResponse->SetResponseReceived(globalIndex, revision, attachmentRange);
        } else if (rsp->IsResponseUncertain(i)) {
            fullResponse->SetResponseUncertain(globalIndex);
        } else {
            // NB: Currently, every subresponse can be marked by master as
            // completed, not completed or uncertain. There are buggy
            // situations that lead to some completed subrequests being reported
            // as not completed. This, in turn, leads to "Duplicate request is
            // not marked as retry" error. It's safer to mark such subrequests
            // as uncertain to avoid said error.

            // TODO(kvk1920): rethink "uncertain subresponse" and probably stop
            // report them explicitly.
            fullResponse->SetResponseUncertain(globalIndex);
        }
    }
    InFlightSubbatchIndexToGlobalIndex_.clear();

    if (GetFirstUnreceivedSubresponseIndex() == GetTotalSubrequestCount()) {
        GetFullResponse()->SetPromise({});
        return;
    }

    InvokeNextBatch();
}

int TObjectServiceProxy::TReqExecuteBatchNoSequoiaRetries::GetTotalSubrequestCount() const
{
    return GetSize();
}

int TObjectServiceProxy::TReqExecuteBatchNoSequoiaRetries::GetFirstUnreceivedSubresponseIndex() const
{
    return FullResponse_ ? FullResponse_->GetFirstUnreceivedResponseIndex() : 0;
}

bool TObjectServiceProxy::TReqExecuteBatchNoSequoiaRetries::IsSubresponseUncertain(int index) const
{
    return FullResponse_ ? FullResponse_->IsResponseUncertain(index) : false;
}

bool TObjectServiceProxy::TReqExecuteBatchNoSequoiaRetries::IsSubresponseReceived(int index) const
{
    return FullResponse_ ? FullResponse_->IsResponseReceived(index) : false;
}

void TObjectServiceProxy::TReqExecuteBatchNoSequoiaRetries::SetBalancingHeader()
{
    if (!StickyGroupSize_) {
        return;
    }

    auto* balancingHeaderExt = Header().MutableExtension(NRpc::NProto::TBalancingExt::balancing_ext);
    balancingHeaderExt->set_enable_stickiness(true);
    balancingHeaderExt->set_enable_client_stickiness(EnableClientStickiness_);

    auto stickyGroupSize = *StickyGroupSize_;
    if (auto advisedStickyGroupSize = GetAdvisedStickyGroupSize()) {
        stickyGroupSize = std::max(stickyGroupSize, *advisedStickyGroupSize);
        YT_LOG_DEBUG("Using advised sticky group size (RequestId: %v, ProvidedSize: %v, AdvisedSize: %v)",
            GetRequestId(),
            stickyGroupSize,
            *advisedStickyGroupSize);
    } else {
        YT_LOG_DEBUG("Not using advised sticky group size (RequestId: %v, ProvidedSize: %v)",
            GetRequestId(),
            stickyGroupSize);
    }
    balancingHeaderExt->set_sticky_group_size(stickyGroupSize);
}

void TObjectServiceProxy::TReqExecuteBatchNoSequoiaRetries::SetMulticellSyncHeader()
{
    NObjectClient::SetSuppressUpstreamSync(&Header(), SuppressUpstreamSync_);
    NObjectClient::SetSuppressTransactionCoordinatorSync(&Header(), SuppressTransactionCoordinatorSync_);
}

std::optional<int> TObjectServiceProxy::TReqExecuteBatchNoSequoiaRetries::GetAdvisedStickyGroupSize() const
{
    if (!StickyGroupSizeCache_) {
        return std::nullopt;
    }

    std::optional<int> advisedStickyGroupSize;

    for (const auto& descriptor : InnerRequestDescriptors_) {
        auto key = descriptor.GetKey();
        auto subrequestAdvisedStickyGroupSize = StickyGroupSizeCache_->GetAdvisedStickyGroupSize(key);
        if (!advisedStickyGroupSize) {
            advisedStickyGroupSize = subrequestAdvisedStickyGroupSize;
        } else if (subrequestAdvisedStickyGroupSize) {
            advisedStickyGroupSize = std::max(subrequestAdvisedStickyGroupSize, advisedStickyGroupSize);
        }
    }

    return advisedStickyGroupSize;
}

////////////////////////////////////////////////////////////////////////////////

TObjectServiceProxy::TReqExecuteBatch::TReqExecuteBatch(
    IChannelPtr channel,
    TReqExecuteBatchRetriesConfigPtr config,
    TStickyGroupSizeCachePtr stickyGroupSizeCache,
    TCallback<bool(int, const TError&)> needRetry,
    int subbatchSize,
    bool regenerateMutationIdForRetries)
    : TReqExecuteBatchBase(
        std::move(channel),
        subbatchSize,
        std::move(stickyGroupSizeCache))
    , Config_(std::move(config))
    , NeedRetry_(std::move(needRetry))
    , RegenerateMutationIdForRetries_(regenerateMutationIdForRetries)
{
    YT_VERIFY(Config_);
    YT_VERIFY(NeedRetry_);
}

void TObjectServiceProxy::TReqExecuteBatch::SetStickyGroupSize(int value)
{
    StickyGroupSize_ = value;
}

void TObjectServiceProxy::TReqExecuteBatch::SetEnableClientStickiness(bool value)
{
    EnableClientStickiness_ = value;
}

TFuture<TObjectServiceProxy::TRspExecuteBatchPtr> TObjectServiceProxy::TReqExecuteBatch::Invoke()
{
    Initialize();
    InvokeNextBatch();
    return FullResponsePromise_;
}

void TObjectServiceProxy::TReqExecuteBatch::Initialize()
{
    FullResponsePromise_ = NewPromise<TRspExecuteBatchPtr>();
    FullResponse_ = New<TRspExecuteBatch>(
        CreateClientContext(),
        InnerRequestDescriptors_,
        StickyGroupSizeCache_,
        FullResponsePromise_);

    // First batch contains all requests so fill in all the indexes.
    PendingIndexes_.resize(InnerRequestDescriptors_.size());
    std::iota(PendingIndexes_.begin(), PendingIndexes_.end(), 0);

    PushDownPrerequisites();
}

void TObjectServiceProxy::TReqExecuteBatch::InvokeNextBatch()
{
    std::vector<TInnerRequestDescriptor> innerRequestDescriptors;
    innerRequestDescriptors.reserve(PendingIndexes_.size());

    for (int index : PendingIndexes_) {
        auto& descriptor = InnerRequestDescriptors_[index];
        descriptor.Message = PatchMutationId(
            descriptor.Message,
            CurrentRetry_ > 0,
            RegenerateMutationIdForRetries_);
        innerRequestDescriptors.push_back(descriptor);
    }

    auto batchRequest = New<TReqExecuteBatchNoSequoiaRetries>(*this, std::move(innerRequestDescriptors));
    batchRequest->SetEnableClientStickiness(EnableClientStickiness_);
    if (StickyGroupSize_) {
        batchRequest->SetStickyGroupSize(*StickyGroupSize_);
    }
    CurrentReqFuture_ = batchRequest->Invoke();
    YT_LOG_DEBUG("Batch attempt invoked (BatchRequestId: %v, AttemptRequestId: %v, RequestCount: %v, CurrentRetry: %v)",
        GetRequestId(),
        batchRequest->GetRequestId(),
        batchRequest->GetSize(),
        CurrentRetry_);
    YT_UNUSED_FUTURE(CurrentReqFuture_.Apply(
        BIND(&TObjectServiceProxy::TReqExecuteBatch::OnBatchResponse, MakeStrong(this))));
}

void TObjectServiceProxy::TReqExecuteBatch::OnBatchResponse(const TErrorOr<TRspExecuteBatchPtr>& batchRspOrErr)
{
    if (!batchRspOrErr.IsOK()) {
        FullResponsePromise_.Set(batchRspOrErr);
        return;
    }

    const auto batchRsp = batchRspOrErr.Value();
    YT_VERIFY(batchRsp->GetResponseCount() == std::ssize(PendingIndexes_));
    YT_VERIFY(batchRsp->GetResponseCount() == batchRsp->GetSize());

    int retryCount = 0;
    for (int i = 0; i < batchRsp->GetSize(); ++i) {
        const auto& rspOrErr = batchRsp->GetResponse(i);
        if (CurrentRetry_ < Config_->RetryCount && NeedRetry_(CurrentRetry_, rspOrErr)) {
            // Building new indexes vector in-place to avoid new allocations.
            PendingIndexes_[retryCount++] = PendingIndexes_[i];
        } else {
            auto revision = batchRsp->GetRevision(i);
            auto attachmentRange = batchRsp->GetResponseAttachmentRange(i);
            FullResponse_->SetResponseReceived(PendingIndexes_[i], revision, attachmentRange);
        }
    }

    if (retryCount == 0) {
        FullResponse_->SetPromise({});
        return;
    }

    PendingIndexes_.resize(retryCount);

    NConcurrency::TDelayedExecutor::Submit(
        BIND(&TObjectServiceProxy::TReqExecuteBatch::OnRetryDelayFinished, MakeStrong(this)),
        GetCurrentDelay());
}

void TObjectServiceProxy::TReqExecuteBatch::OnRetryDelayFinished()
{
    ++CurrentRetry_;
    InvokeNextBatch();
}

TSharedRefArray TObjectServiceProxy::TReqExecuteBatch::PatchMutationId(
    const TSharedRefArray& message,
    bool retry,
    bool regenerateMutationIdForRetries)
{
    NRpc::NProto::TRequestHeader header;
    YT_VERIFY(TryParseRequestHeader(message, &header));

    // TReqExecuteBatch has 2 modes:
    //   1. low-level retries: every error is considered as "transient error"
    //      (i.e. is not kept by master response keeper). In this mode mutation
    //      ID is not generated for every retry and |retry| flag is properly
    //      set. Examples: Unavailable, Timeout, Sequoia errors.
    //   2. retries of any errors: every request retry has its own mutation ID to
    //      be able retry persistent errors occurred inside master mutations.
    //      |retry| flag is always false in this mode.

    if (retry) {
        YT_VERIFY(header.has_mutation_id());
        if (regenerateMutationIdForRetries) {
            NRpc::SetMutationId(&header, GenerateMutationId(), /*retry*/ false);
        } else if (header.retry()) {
            // Fast path: request header isn't changed.
            return message;
        } else {
            header.set_retry(true);
        }
    } else {
        if (header.has_mutation_id()) {
            // NB: we don't check if |retry| flag is already set because caller
            // may want to retry request with old mutation ID. In such cases
            // mutation ID isn't patched and it's a caller's responsibility to
            // properly set |retry| flag.

            // Fast path: request header isn't changed.
            return message;
        } else {
            NRpc::SetMutationId(&header, GenerateMutationId(), /*retry*/ false);
        }
    }

    return SetRequestHeader(message, header);
}

TDuration TObjectServiceProxy::TReqExecuteBatch::GetCurrentDelay()
{
    YT_VERIFY(CurrentRetry_ < Config_->RetryCount);

    double backoffMultiplier = std::pow(Config_->BackoffMultiplier, CurrentRetry_);
    return std::min(Config_->StartBackoff * backoffMultiplier, Config_->MaxBackoff);
}

////////////////////////////////////////////////////////////////////////////////

TObjectServiceProxy::TRspExecuteBatch::TRspExecuteBatch(
    TClientContextPtr clientContext,
    const std::vector<TReqExecuteSubbatch::TInnerRequestDescriptor>& innerRequestDescriptors,
    TStickyGroupSizeCachePtr stickyGroupSizeCache,
    TPromise<TRspExecuteBatchPtr> promise)
    : TClientResponse(std::move(clientContext))
    , StickyGroupSizeCache_(std::move(stickyGroupSizeCache))
    , InnerResponseDescriptors_(innerRequestDescriptors.size())
    , InnerRequestDescriptors_(std::move(innerRequestDescriptors))
    , Promise_(promise ? std::move(promise) : NewPromise<TRspExecuteBatchPtr>())
{ }

TPromise<TObjectServiceProxy::TRspExecuteBatchPtr> TObjectServiceProxy::TRspExecuteBatch::GetPromise()
{
    return Promise_;
}

void TObjectServiceProxy::TRspExecuteBatch::SetEmpty()
{
    NProto::TRspExecute body;
    auto message = CreateResponseMessage(body);
    static_cast<IClientResponseHandler*>(this)->HandleResponse(
        std::move(message),
        /*address*/ std::string());
}

void TObjectServiceProxy::TRspExecuteBatch::SetPromise(const TError& error)
{
    if (error.IsOK()) {
        Promise_.Set(this);
    } else {
        Promise_.Set(error);
    }
    Promise_.Reset();
}

bool TObjectServiceProxy::TRspExecuteBatch::TryDeserializeBody(
    TRef data,
    std::optional<NCompression::ECodec> codecId)
{
    NProto::TRspExecute body;
    auto deserializeResult = codecId
        ? TryDeserializeProtoWithCompression(&body, data, *codecId)
        : TryDeserializeProtoWithEnvelope(&body, data);
    if (!deserializeResult) {
        return false;
    }

    if (body.subresponses_size() != 0) { // new format
        YT_VERIFY(std::ssize(InnerResponseDescriptors_) >= body.subresponses_size());

        auto partIndex = 0;
        for (const auto& subresponse : body.subresponses()) {
            auto subrequestIndex = subresponse.index();
            auto partCount = subresponse.part_count();
            auto revision = FromProto<NHydra::TRevision>(subresponse.revision());
            if (subresponse.has_advised_sticky_group_size() && StickyGroupSizeCache_) {
                auto advisedStickyGroupSize = subresponse.advised_sticky_group_size();
                auto key = InnerRequestDescriptors_[subrequestIndex].GetKey();
                auto oldAdvisedStickyGroupSize = StickyGroupSizeCache_->UpdateAdvisedStickyGroupSize(key, advisedStickyGroupSize);
                YT_LOG_DEBUG_IF(
                    advisedStickyGroupSize != oldAdvisedStickyGroupSize,
                    "Cached sticky group size updated (RequestId: %v, SubrequestIndex: %v, Size: %v -> %v)",
                    GetRequestId(),
                    subrequestIndex,
                    oldAdvisedStickyGroupSize,
                    advisedStickyGroupSize);
            }
            InnerResponseDescriptors_[subrequestIndex].Meta = {{partIndex, partIndex + partCount}, revision};
            partIndex += partCount;
        }
        ResponseCount_ = body.subresponses_size();

        for (const auto& uncertainSubrequestIndex : body.uncertain_subrequest_indexes()) {
            InnerResponseDescriptors_[uncertainSubrequestIndex].Uncertain = true;
        }
    } else { // old format
        // COMPAT(shakurov)

        YT_VERIFY(std::ssize(InnerResponseDescriptors_) >= body.part_counts_size());
        YT_VERIFY(body.revisions_size() == body.part_counts_size() || body.revisions_size() == 0);
        YT_VERIFY(body.advised_sticky_group_size_size() == body.part_counts_size() || body.advised_sticky_group_size_size() == 0);

        auto revisions = body.revisions_size() == 0
            ? std::vector<NHydra::TRevision>(body.part_counts_size())
            : FromProto<std::vector<NHydra::TRevision>>(body.revisions());

        auto partIndex = 0;
        for (auto subrequestIndex = 0; subrequestIndex < body.part_counts_size(); ++subrequestIndex) {
            auto partCount = body.part_counts(subrequestIndex);
            InnerResponseDescriptors_[subrequestIndex].Meta = {{partIndex, partIndex + partCount}, revisions[subrequestIndex]};
            partIndex += partCount;

            if (body.advised_sticky_group_size_size() > 0 && StickyGroupSizeCache_) {
                auto advisedStickyGroupSize = body.advised_sticky_group_size(subrequestIndex);
                auto key = InnerRequestDescriptors_[subrequestIndex].GetKey();
                auto oldAdvisedStickyGroupSize = StickyGroupSizeCache_->UpdateAdvisedStickyGroupSize(key, advisedStickyGroupSize);
                YT_LOG_DEBUG_IF(
                    advisedStickyGroupSize != oldAdvisedStickyGroupSize,
                    "Cached sticky group size updated (RequestId: %v, SubrequestIndex: %v, Size: %v -> %v)",
                    GetRequestId(),
                    subrequestIndex,
                    oldAdvisedStickyGroupSize,
                    advisedStickyGroupSize);
            }
        }

        ResponseCount_ = body.part_counts_size();
    }

    return true;
}

void TObjectServiceProxy::TRspExecuteBatch::SetResponseReceived(
    int index,
    NHydra::TRevision revision,
    TAttachmentRange attachments)
{
    YT_VERIFY(0 <= index && index <= std::ssize(InnerResponseDescriptors_));

    auto& descriptor = InnerResponseDescriptors_[index];
    YT_VERIFY(!descriptor.Meta);

    const auto attachmentCount = std::ssize(Attachments_);
    const auto rangeSize = std::distance(attachments.Begin, attachments.End);

    descriptor.Uncertain = false;
    descriptor.Meta = {{attachmentCount, attachmentCount + rangeSize}, revision};
    ++ResponseCount_;

    Attachments_.insert(Attachments_.end(), attachments.Begin, attachments.End);

    if (index == FirstUnreceivedResponseIndex_) {
        for (; FirstUnreceivedResponseIndex_ < std::ssize(InnerRequestDescriptors_); ++FirstUnreceivedResponseIndex_) {
            if (!IsResponseReceived(FirstUnreceivedResponseIndex_)) {
                break;
            }
        }
    }
}

void TObjectServiceProxy::TRspExecuteBatch::SetResponseUncertain(int index)
{
    YT_VERIFY(0 <= index && index <= std::ssize(InnerResponseDescriptors_));
    YT_VERIFY(!InnerResponseDescriptors_[index].Meta);
    InnerResponseDescriptors_[index].Uncertain = true;
}

int TObjectServiceProxy::TRspExecuteBatch::GetSize() const
{
    return InnerResponseDescriptors_.size();
}

int TObjectServiceProxy::TRspExecuteBatch::GetResponseCount() const
{
    return ResponseCount_;
}

std::vector<int> TObjectServiceProxy::TRspExecuteBatch::GetUncertainRequestIndexes() const
{
    std::vector<int> result;
    result.reserve(InnerResponseDescriptors_.size());

    for (auto i = 0; i < std::ssize(InnerResponseDescriptors_); ++i) {
        if (IsResponseUncertain(i)) {
            result.push_back(i);
        }
    }

    return result;
}

TGuid TObjectServiceProxy::TRspExecuteBatch::GetRequestId() const
{
    return FromProto<TGuid>(Header().request_id());
}

TErrorOr<TYPathResponsePtr> TObjectServiceProxy::TRspExecuteBatch::GetResponse(int index) const
{
    return GetResponse<TYPathResponse>(index);
}

std::optional<TErrorOr<TYPathResponsePtr>> TObjectServiceProxy::TRspExecuteBatch::FindResponse(const std::string& key) const
{
    return FindResponse<TYPathResponse>(key);
}

TErrorOr<TYPathResponsePtr> TObjectServiceProxy::TRspExecuteBatch::GetResponse(const std::string& key) const
{
    return GetResponse<TYPathResponse>(key);
}

std::vector<TErrorOr<TYPathResponsePtr>> TObjectServiceProxy::TRspExecuteBatch::GetResponses(const std::optional<std::string>& key) const
{
    return GetResponses<TYPathResponse>(key);
}

TSharedRefArray TObjectServiceProxy::TRspExecuteBatch::GetResponseMessage(int index) const
{
    YT_VERIFY(index >= 0 && index < std::ssize(InnerRequestDescriptors_));

    const auto& responseMeta = InnerResponseDescriptors_[index].Meta;

    if (!responseMeta) {
        return TSharedRefArray();
    }

    auto [beginIndex, endIndex] = responseMeta->PartRange;
    if (beginIndex == endIndex) {
        // This is an empty response.
        return TSharedRefArray();
    }
    return TSharedRefArray(
        TRange(
            Attachments_.data() + beginIndex,
            Attachments_.data() + endIndex),
        TSharedRefArray::TCopyParts{});
}

bool TObjectServiceProxy::TRspExecuteBatch::IsResponseReceived(int index) const
{
    YT_VERIFY(index >= 0 && index < std::ssize(InnerRequestDescriptors_));
    return InnerResponseDescriptors_[index].Meta.has_value();
}

bool TObjectServiceProxy::TRspExecuteBatch::IsResponseUncertain(int index) const
{
    YT_VERIFY(index >= 0 && index < std::ssize(InnerRequestDescriptors_));
    return InnerResponseDescriptors_[index].Uncertain;
}

int TObjectServiceProxy::TRspExecuteBatch::GetFirstUnreceivedResponseIndex() const
{
    return FirstUnreceivedResponseIndex_;
}

TObjectServiceProxy::TRspExecuteBatch::TAttachmentRange TObjectServiceProxy::TRspExecuteBatch::GetResponseAttachmentRange(int index) const
{
    const auto& meta = InnerResponseDescriptors_[index].Meta;
    YT_VERIFY(meta);
    return {
        Attachments_.begin() + meta->PartRange.first,
        Attachments_.begin() + meta->PartRange.second
    };
}

NHydra::TRevision TObjectServiceProxy::TRspExecuteBatch::GetRevision(int index) const
{
    if (InnerResponseDescriptors_.empty()) {
        return NHydra::NullRevision;
    }

    YT_VERIFY(index >= 0 && index <= std::ssize(InnerRequestDescriptors_));

    const auto& meta = InnerResponseDescriptors_[index].Meta;
    YT_VERIFY(meta);
    return meta->Revision;
}

////////////////////////////////////////////////////////////////////////////////

TObjectServiceProxy::TReqExecuteBatchPtr
TObjectServiceProxy::ExecuteBatch(int subbatchSize)
{
    static const auto NeedRetry = BIND_NO_PROPAGATE([] (int, const TError& error) {
            return error.GetCode() == NSequoiaClient::EErrorCode::SequoiaRetriableError;
    });

    return ExecuteBatchWithRetries(
        SequoiaRetriesConfig_,
        NeedRetry,
        subbatchSize,
        /*regenerateMutationIdForRetries*/ false);
}

TObjectServiceProxy::TReqExecuteBatchBasePtr
TObjectServiceProxy::ExecuteBatchNoBackoffRetries(int subbatchSize)
{
    auto batchReq = New<TReqExecuteBatchBase>(
        Channel_,
        subbatchSize,
        StickyGroupSizeCache_);
    PrepareBatchRequest(batchReq);
    return batchReq;
}

TObjectServiceProxy::TReqExecuteBatchPtr
TObjectServiceProxy::ExecuteBatchWithRetries(
    TReqExecuteBatchRetriesConfigPtr config,
    TCallback<bool(int, const TError&)> needRetry,
    int subbatchSize,
    bool regenerateMutationIdForRetries)
{
    auto batchReq = New<TReqExecuteBatch>(
        Channel_,
        std::move(config),
        StickyGroupSizeCache_,
        std::move(needRetry),
        subbatchSize,
        regenerateMutationIdForRetries);
    PrepareBatchRequest(batchReq);
    return batchReq;
}

TObjectServiceProxy::TReqExecuteBatchInParallelPtr
TObjectServiceProxy::ExecuteBatchWithRetriesInParallel(
    TReqExecuteBatchRetriesConfigPtr config,
    TCallback<bool(int, const TError&)> needRetry,
    int subbatchSize,
    int maxParallelSubbatchCount)
{
    YT_VERIFY(maxParallelSubbatchCount > 0);
    std::vector<TReqExecuteBatchPtr> parallelReqs;
    for (int i = 0; i < maxParallelSubbatchCount; ++i) {
        auto batchReq = ExecuteBatchWithRetries(
            config,
            needRetry,
            subbatchSize);
        PrepareBatchRequest(batchReq);
        parallelReqs.push_back(std::move(batchReq));
    }
    return New<TReqExecuteBatchInParallel>(
        Channel_,
        subbatchSize,
        StickyGroupSizeCache_,
        std::move(parallelReqs));
}

TObjectServiceProxy::TReqExecuteBatchInParallel::TReqExecuteBatchInParallel(
    NRpc::IChannelPtr channel,
    int subbatchSize,
    TStickyGroupSizeCachePtr stickyGroupSizeCache,
    std::vector<TReqExecuteBatchPtr> parallelReqs)
    : TReqExecuteBatchBase(
        std::move(channel),
        subbatchSize,
        std::move(stickyGroupSizeCache))
    , ParallelReqs_(std::move(parallelReqs))
{ }

TFuture<TObjectServiceProxy::TRspExecuteBatchPtr>
TObjectServiceProxy::TReqExecuteBatchInParallel::Invoke()
{
    auto needsRoundUp = std::ssize(InnerRequestDescriptors_) % std::ssize(ParallelReqs_) != 0;
    auto reqsPerParallelReq = std::ssize(InnerRequestDescriptors_) / std::ssize(ParallelReqs_) + needsRoundUp;
    for (int i = 0; i < std::ssize(InnerRequestDescriptors_); ++i) {
        const auto& req = InnerRequestDescriptors_[i];
        auto parallelReqIndex = i / reqsPerParallelReq;
        YT_VERIFY(parallelReqIndex < std::ssize(ParallelReqs_));
        ParallelReqs_[parallelReqIndex]->AddRequestMessage(
            req.Message,
            req.Key,
            req.Tag,
            req.Hash);
    }

    std::vector<TFuture<TRspExecuteBatchPtr>> parallelReqFutures;
    for (auto& parallelReq : ParallelReqs_) {
        parallelReqFutures.push_back(parallelReq->Invoke());
    }

    return AllSucceeded(std::move(parallelReqFutures)).Apply(
        BIND(&TReqExecuteBatchInParallel::OnParallelResponses, MakeStrong(this)));
}

TObjectServiceProxy::TRspExecuteBatchPtr
TObjectServiceProxy::TReqExecuteBatchInParallel::OnParallelResponses(
    const TErrorOr<std::vector<TRspExecuteBatchPtr>>& parallelRspsOrError)
{
    auto fullResponse = New<TRspExecuteBatch>(
        CreateClientContext(),
        InnerRequestDescriptors_,
        StickyGroupSizeCache_);

    if (!parallelRspsOrError.IsOK()) {
        fullResponse->GetPromise().Set(parallelRspsOrError);
        return fullResponse;
    }

    const auto& parallelRsps = parallelRspsOrError.Value();

    auto globalIndex = 0;
    for (const auto& parallelRsp : parallelRsps) {
        for (auto i = 0; i < parallelRsp->GetSize(); ++i) {
            if (parallelRsp->IsResponseReceived(i)) {
                auto revision = parallelRsp->GetRevision(i);
                auto attachmentRange = parallelRsp->GetResponseAttachmentRange(i);
                fullResponse->SetResponseReceived(globalIndex, revision, attachmentRange);
            } else if (parallelRsp->IsResponseUncertain(i)) {
                fullResponse->SetResponseUncertain(globalIndex);
            } else {
                // See comment in OnSubbatchResponse().
                fullResponse->SetResponseUncertain(globalIndex);
            }
            ++globalIndex;
        }
    }

    fullResponse->SetPromise({});
    return fullResponse;
}

template <class TBatchReqPtr>
void TObjectServiceProxy::PrepareBatchRequest(const TBatchReqPtr& batchReq)
{
    batchReq->SetTimeout(DefaultTimeout_);
    batchReq->SetAcknowledgementTimeout(DefaultAcknowledgementTimeout_);
}

const TServiceDescriptor& TObjectServiceProxy::GetDescriptor()
{
    static auto serviceDescriptor = TServiceDescriptor("ObjectService")
        .SetProtocolVersion(12)
        .SetAcceptsBaggage(false)
        .SetFeaturesType<EMasterFeature>();
    return serviceDescriptor;
}

////////////////////////////////////////////////////////////////////////////////

TError GetCumulativeError(
    const TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError,
    const std::optional<std::string>& key)
{
    if (!batchRspOrError.IsOK()) {
        return batchRspOrError;
    }

    return GetCumulativeError(batchRspOrError.Value(), key);
}

TError GetCumulativeError(
    const TObjectServiceProxy::TRspExecuteBatchPtr& batchRsp,
    const std::optional<std::string>& key)
{
    TError cumulativeError("Error communicating with master");
    for (const auto& rspOrError : batchRsp->GetResponses(key)) {
        if (!rspOrError.IsOK()) {
            cumulativeError.MutableInnerErrors()->push_back(rspOrError);
        }
    }
    return cumulativeError.InnerErrors().empty() ? TError() : cumulativeError;
}

void ThrowCumulativeErrorIfFailed(const TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError) {
    auto cumulativeError = GetCumulativeError(batchRspOrError);
    THROW_ERROR_EXCEPTION_IF_FAILED(cumulativeError);
}

////////////////////////////////////////////////////////////////////////////////

TObjectServiceProxy CreateObjectServiceReadProxy(
    IClientPtr client,
    NApi::EMasterChannelKind readFrom,
    TCellTag cellTag,
    TStickyGroupSizeCachePtr stickyGroupSizeCache)
{
    return TObjectServiceProxy(
        std::move(client),
        readFrom,
        cellTag,
        std::move(stickyGroupSizeCache));
}

TObjectServiceProxy CreateObjectServiceReadProxy(
    IConnectionPtr connection,
    NApi::EMasterChannelKind readFrom,
    TCellTag cellTag,
    TStickyGroupSizeCachePtr stickyGroupSizeCache)
{
    return TObjectServiceProxy(
        std::move(connection),
        readFrom,
        cellTag,
        std::move(stickyGroupSizeCache));
}

TObjectServiceProxy CreateObjectServiceWriteProxy(
    NApi::NNative::IClientPtr client,
    TCellTag cellTag)
{
    return TObjectServiceProxy(
        std::move(client),
        NApi::EMasterChannelKind::Leader,
        cellTag,
        /*stickyGroupSizeCache*/ nullptr);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient
