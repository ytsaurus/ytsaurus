#ifndef DISTRIBUTED_WRITE_FACADE_INL_H
#error "Direct inclusion of this file is not allowed, include distributed_write_facade_base.h"
// For the sake of sane code completion.
#include "distributed_write_facade_base.h"
#endif

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/client/signature/generator.h>
#include <yt/yt/client/signature/signature.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

template <class TDerived, CDistributedWriteFacadeTraits TTraits>
TDistributedWriteStartFacadeBase<TDerived, TTraits>::TDistributedWriteStartFacadeBase(
    const IClientPtr& client,
    const NLogging::TLogger& logger)
    : Client_(client)
    , Logger(logger)
{ }

template <class TDerived, CDistributedWriteFacadeTraits TTraits>
TTraits::TSessionWithCookies TDistributedWriteStartFacadeBase<TDerived, TTraits>::StartSession(
    const NYPath::TRichYPath& richPath,
    const TTraits::TStartOptions& options)
{
    const auto& path = richPath.GetPath();

    auto masterTransaction = StartMasterTransaction(options);
    auto userObject = RequestUserObject(path, masterTransaction);

    auto objectId = userObject.ObjectId;
    auto nativeCellTag = NObjectClient::CellTagFromId(objectId);
    auto externalCellTag = userObject.ExternalCellTag;
    auto objectIdPath = NObjectClient::FromObjectId(objectId);

    NYTree::INodePtr nodeWithAttributes;

    {
        YT_LOG_DEBUG("Requesting extended object attributes (ObjectType: %Qlv)",
            TTraits::ObjectType);

        nodeWithAttributes = ToDerived()->RequestExtendedObjectAttributes(
            richPath,
            externalCellTag,
            objectIdPath,
            userObject);

        YT_LOG_DEBUG("Extended attributes received (Attributes: %v, ObjectType: %Qlv)",
            ConvertToYsonString(nodeWithAttributes->Attributes(), NYson::EYsonFormat::Text),
            TTraits::ObjectType);
    }

    auto [chunkSchemaId, uploadTransactionId] = ToDerived()->BeginUpload(
        richPath,
        nativeCellTag,
        objectIdPath,
        masterTransaction->GetId());

    auto rootChunkListId = ToDerived()->RequestUploadParameters(
        richPath,
        externalCellTag,
        objectIdPath,
        uploadTransactionId);

    auto timestamp = NConcurrency::WaitFor(
        Client_->GetNativeConnection()->GetTimestampProvider()->GenerateTimestamps())
        .ValueOrThrow();

    auto session = ToDerived()->CreateSession(
        masterTransaction->GetId(),
        uploadTransactionId,
        rootChunkListId,
        richPath,
        objectId,
        externalCellTag,
        chunkSchemaId,
        timestamp,
        nodeWithAttributes);

    std::vector<typename TTraits::TSignedCookiePtr> cookies;
    cookies.reserve(options.CookieCount);

    auto signatureGenerator = Client_->GetNativeConnection()->GetSignatureGenerator();

    for (int i = 0; i < options.CookieCount; ++i) {
        cookies.emplace_back(
            signatureGenerator->Sign(NYson::ConvertToYsonString(session.CookieFromThis()).ToString()));
    }

    typename TTraits::TSessionWithCookies result;
    result.Session = typename TTraits::TSignedSessionPtr(signatureGenerator->Sign(NYson::ConvertToYsonString(session).ToString()));
    result.Cookies = std::move(cookies);

    // NB(pavook): we pass the transaction to the user here, and expect it to be attached,
    // so it shouldn't be auto-aborted anymore.
    masterTransaction->Detach();

    return result;
}

template <class TDerived, CDistributedWriteFacadeTraits TTraits>
NChunkClient::TUserObject TDistributedWriteStartFacadeBase<TDerived, TTraits>::RequestUserObject(
    const NYPath::TYPath& path,
    NApi::ITransactionPtr transaction)
{
    NChunkClient::TUserObject userObject(path);

    NChunkClient::GetUserObjectBasicAttributes(
        Client_,
        {&userObject},
        transaction->GetId(),
        Logger,
        NYTree::EPermission::Write);

    if (userObject.Type != TTraits::ObjectType) {
        THROW_ERROR_EXCEPTION(
            "Invalid type of %v: expected %Qlv, actual %Qlv",
            path,
            TTraits::ObjectType,
            userObject.Type);
    }

    return userObject;
}

template <class TDerived, CDistributedWriteFacadeTraits TTraits>
NApi::ITransactionPtr TDistributedWriteStartFacadeBase<TDerived, TTraits>::StartMasterTransaction(
    const TTraits::TStartOptions& options)
{
    TTransactionStartOptions transactionStartOptions;
    transactionStartOptions.Timeout = options.SessionTimeout;
    if (options.TransactionId) {
        transactionStartOptions.ParentId = options.TransactionId;
        transactionStartOptions.Ping = true;
        transactionStartOptions.PingAncestors = options.PingAncestors;
    }

    return NConcurrency::WaitFor(
        Client_->StartTransaction(
            NTransactionClient::ETransactionType::Master,
            transactionStartOptions))
        .ValueOrThrow();
}

////////////////////////////////////////////////////////////////////////////////

template <class TDerived, CDistributedWriteFacadeTraits TTraits>
TDistributedWritePingFacadeBase<TDerived, TTraits>::TDistributedWritePingFacadeBase(
    const IClientPtr& client)
    : Client_(client)
{ }

template <class TDerived, CDistributedWriteFacadeTraits TTraits>
void TDistributedWritePingFacadeBase<TDerived, TTraits>::PingSession(
    TTraits::TSignedSessionPtr session,
    const TTraits::TPingOptions& /*options*/)
{
    YT_VERIFY(session);
    auto concreteSession = ConvertTo<typename TTraits::TSession>(
        NYson::TYsonStringBuf(session.Underlying()->Payload()));

    // NB(arkady-e1ppa): AutoAbort = false by default.
    auto masterTransactionId = ToDerived()->GetMasterTransaction(concreteSession);
    auto mainTransaction = Client_->AttachTransaction(masterTransactionId);
    NConcurrency::WaitFor(mainTransaction->Ping())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

template <class TDerived, CDistributedWriteFacadeTraits TTraits>
TDistributedWriteFinishFacadeBase<TDerived, TTraits>::TDistributedWriteFinishFacadeBase(
    const IClientPtr& client,
    const NLogging::TLogger& logger)
    : Client_(client)
    , Logger(logger)
{ }

template <class TDerived, CDistributedWriteFacadeTraits TTraits>
void TDistributedWriteFinishFacadeBase<TDerived, TTraits>::FinishSession(
    const TTraits::TSessionWithResults& sessionWithResults,
    const TTraits::TFinishOptions& /*options*/)
{
    YT_VERIFY(sessionWithResults.Session);

    auto session = ConvertTo<typename TTraits::TSession>(
        NYson::TYsonStringBuf(sessionWithResults.Session.Underlying()->Payload()));

    NYPath::TRichYPath path = ToDerived()->GetPath(session);

    TTransactionAttachOptions attachOptions;
    attachOptions.AutoAbort = true;
    auto transaction = Client_->AttachTransaction(ToDerived()->GetMasterTransaction(session), attachOptions);

    NChunkClient::NProto::TDataStatistics dataStatistics;

    auto channel = Client_->GetMasterChannelOrThrow(
        EMasterChannelKind::Leader,
        ToDerived()->GetExternalCellTag(session));
    NChunkClient::TChunkServiceProxy proxy(channel);

    // Split large outputs into separate requests.
    NChunkClient::NProto::TReqAttachChunkTrees* req = nullptr;
    NChunkClient::TChunkServiceProxy::TReqExecuteBatchPtr batchReq;

    auto flushRequest = [&] (bool requestStatistics) {
        if (!batchReq) {
            return;
        }

        if (req) {
            req->set_request_statistics(requestStatistics);
            req = nullptr;
        }

        auto batchRspOrError = NConcurrency::WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(
            NChunkClient::GetCumulativeError(batchRspOrError),
            "Error attaching output chunks (Path: %v)",
            path);

        const auto& batchRsp = batchRspOrError.Value();
        const auto& subresponses = batchRsp->attach_chunk_trees_subresponses();

        if (requestStatistics) {
            for (const auto& rsp : subresponses) {
                dataStatistics += rsp.statistics();
            }
        }

        batchReq.Reset();
    };

    int currentRequestSize = 0;
    THashSet<NChunkClient::TChunkTreeId> addedChunkTrees;

    const auto& config = Client_->GetNativeConnection()->GetConfig()->DistributedWriteDynamicConfig;
    auto addChunkTree = [&] (NChunkClient::TChunkTreeId chunkTreeId) {
        if (batchReq && currentRequestSize >= config->MaxChildrenPerAttachRequest) {
            flushRequest(/*requestStatistics*/ false);
            currentRequestSize = 0;
        }

        ++currentRequestSize;

        if (!req) {
            if (!batchReq) {
                batchReq = proxy.ExecuteBatch();
                GenerateMutationId(batchReq);
                NObjectClient::SetSuppressUpstreamSync(&batchReq->Header(), true);
                // COMPAT(shakurov): prefer proto ext (above).
                batchReq->set_suppress_upstream_sync(true);
            }
            req = batchReq->add_attach_chunk_trees_subrequests();
            ToProto(req->mutable_parent_id(), session.RootChunkListId);
        }

        ToProto(req->add_child_ids(), chunkTreeId);
    };

    std::vector<typename TTraits::TWriteFragmentResult> writeResults;
    writeResults.reserve(std::ssize(sessionWithResults.Results));
    for (const auto& signedResult : sessionWithResults.Results) {
        YT_VERIFY(signedResult);
        writeResults.push_back(ConvertTo<typename TTraits::TWriteFragmentResult>(
            NYson::TYsonStringBuf(signedResult.Underlying()->Payload())));
    }

    ToDerived()->SortResults(&writeResults, session);

    for (const auto& writeResult : writeResults) {
        if (!addedChunkTrees.insert(writeResult.ChunkListId).second) {
            THROW_ERROR_EXCEPTION("Duplicate chunk list ids")
                << TErrorAttribute("session_id", writeResult.SessionId)
                << TErrorAttribute("cookie_id", writeResult.CookieId)
                << TErrorAttribute("chunk_list_id", writeResult.ChunkListId);
        }
        addChunkTree(writeResult.ChunkListId);
    }
    flushRequest(/*requestStatistics*/ true);

    ToDerived()->EndUpload(session, dataStatistics);

    NConcurrency::WaitFor(transaction->Commit())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
