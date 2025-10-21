#include "client_impl.h"

#include <yt/yt/ytlib/file_client/file_fragment_writer.h>
#include <yt/yt/ytlib/file_client/file_ypath_proxy.h>

#include <yt/yt/ytlib/api/native/distributed_write_facade_base.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/yt/ytlib/chunk_client/config.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/client/api/distributed_file_session.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt/client/signature/generator.h>
#include <yt/yt/client/signature/signature.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>

namespace NYT {

using namespace NConcurrency;
using namespace NChunkClient;
using namespace NCypressClient;
using namespace NFileClient;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NYson;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

struct TDistributedFileSessionTraits
{
    using TStartOptions = TDistributedWriteFileSessionStartOptions;
    using TPingOptions = TDistributedWriteFileSessionPingOptions;
    using TFinishOptions = TDistributedWriteFileSessionFinishOptions;

    using TSession = TDistributedWriteFileSession;
    using TWriteFragmentResult = TWriteFileFragmentResult;

    using TSessionWithCookies = TDistributedWriteFileSessionWithCookies;
    using TSessionWithResults = TDistributedWriteFileSessionWithResults;

    using TSignedSessionPtr = TSignedDistributedWriteFileSessionPtr;
    using TSignedCookiePtr = TSignedWriteFileFragmentCookiePtr;

    inline static const EObjectType ObjectType = EObjectType::File;
};

static_assert(CDistributedWriteFacadeTraits<TDistributedFileSessionTraits>,
    "TDistributedFileSessionTraits must follow CDistributedWriteFacadeTraits concept");

////////////////////////////////////////////////////////////////////////////////

class TDistributedWriteFileStartFacade
    : public TDistributedWriteStartFacadeBase<TDistributedWriteFileStartFacade, TDistributedFileSessionTraits>
{
    using TBase = TDistributedWriteStartFacadeBase<TDistributedWriteFileStartFacade, TDistributedFileSessionTraits>;
    using TTraits = TDistributedFileSessionTraits;

    friend class TDistributedWriteStartFacadeBase<TDistributedWriteFileStartFacade, TDistributedFileSessionTraits>;

public:
    TDistributedWriteFileStartFacade(
        const IClientPtr& client,
        const NLogging::TLogger logger)
        : TBase(client, logger)
        , Client_(client)
    { }

protected:
    INodePtr RequestExtendedObjectAttributes(
        const NYPath::TRichYPath& path,
        TCellTag externalCellTag,
        const TYPath& objectIdPath,
        const TUserObject& userObject)
    {
        static const std::vector<std::string> FileAttributes{
            "account",
            "compression_codec",
            "erasure_codec",
            "primary_medium",
            "replication_factor",
            "enable_striped_erasure"};

        auto proxy = CreateObjectServiceReadProxy(
            Client_,
            EMasterChannelKind::Follower,
            externalCellTag);

        auto req = TCypressYPathProxy::Get(objectIdPath + "/@");
        AddCellTagToSyncWith(req, userObject.ObjectId);
        SetTransactionId(req, userObject.ExternalTransactionId);
        ToProto(req->mutable_attributes()->mutable_keys(), FileAttributes);

        auto rspOrError = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(
            rspOrError,
            "Error requesting extended attributes of file %v",
            path.GetPath());

        const auto& rsp = rspOrError.Value();
        return ConvertToNode(TYsonStringBuf(rsp->value()));
    }

    std::tuple<TMasterTableSchemaId, TTransactionId> BeginUpload(
        const TRichYPath& path,
        TCellTag nativeCellTag,
        const TYPath& objectIdPath,
        TTransactionId transactionId)
    {
        auto proxy = CreateObjectServiceWriteProxy(Client_, nativeCellTag);
        auto batchReq = proxy.ExecuteBatch();

        {
            bool isAppend = path.GetAppend();
            auto req = TFileYPathProxy::BeginUpload(objectIdPath);
            auto updateMode = isAppend ? EUpdateMode::Append : EUpdateMode::Overwrite;
            req->set_update_mode(ToProto(updateMode));
            auto lockMode = isAppend ? ELockMode::Shared : ELockMode::Exclusive;
            req->set_lock_mode(ToProto(lockMode));
            req->set_upload_transaction_title(Format("Distributed upload to %v", path));

            GenerateMutationId(req);
            SetTransactionId(req, transactionId);
            batchReq->AddRequest(req, "begin_upload");
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(
            GetCumulativeError(batchRspOrError),
            "Error starting upload to file %v",
            path);
        const auto& batchRsp = batchRspOrError.Value();

        auto rsp = batchRsp->GetResponse<TFileYPathProxy::TRspBeginUpload>("begin_upload").Value();
        auto uploadTransactionId = FromProto<TTransactionId>(rsp->upload_transaction_id());
        auto chunkSchemaId = FromProto<TMasterTableSchemaId>(rsp->upload_chunk_schema_id());

        return std::tuple(chunkSchemaId, uploadTransactionId);
    }

    TChunkListId RequestUploadParameters(
        const TRichYPath& path,
        TCellTag externalCellTag,
        const TYPath& objectIdPath,
        TTransactionId uploadTransactionId)
    {
        auto proxy = CreateObjectServiceReadProxy(
            Client_,
            EMasterChannelKind::Follower,
            externalCellTag);
        auto req = TFileYPathProxy::GetUploadParams(objectIdPath);

        SetTransactionId(req, uploadTransactionId);

        auto rspOrError = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(
            rspOrError,
            "Error requesting upload parameters for file %v",
            path);

        const auto& rsp = rspOrError.Value();
        return FromProto<TChunkListId>(rsp->chunk_list_id());
    }

    TTraits::TSession CreateSession(
        TTransactionId masterTransactionId,
        TTransactionId uploadTransactionId,
        TChunkListId rootChunkListId,
        const TRichYPath& path,
        TObjectId objectId,
        TCellTag externalCellTag,
        NTableClient::TMasterTableSchemaId /*chunkSchemaId*/,
        TTimestamp timestamp,
        INodePtr attributes)
    {
        return TDistributedWriteFileSession(
            masterTransactionId,
            uploadTransactionId,
            rootChunkListId,
            path,
            objectId,
            externalCellTag,
            timestamp,
            attributes);
    }

private:
    const IClientPtr Client_;
};

class TDistributedWriteFilePingFacade
    : public TDistributedWritePingFacadeBase<TDistributedWriteFilePingFacade, TDistributedFileSessionTraits>
{
    using TBase = TDistributedWritePingFacadeBase<TDistributedWriteFilePingFacade, TDistributedFileSessionTraits>;
    using TTraits = TDistributedFileSessionTraits;

    friend class TDistributedWritePingFacadeBase<TDistributedWriteFilePingFacade, TDistributedFileSessionTraits>;

public:
    explicit TDistributedWriteFilePingFacade(const IClientPtr& client)
        : TBase(client)
    { }

protected:
    TTransactionId GetMasterTransaction(const TTraits::TSession& session)
    {
        return session.HostData.MainTransactionId;
    }
};

class TDistributedWriteFileFinishFacade
    : public TDistributedWriteFinishFacadeBase<TDistributedWriteFileFinishFacade, TDistributedFileSessionTraits>
{
    using TBase = TDistributedWriteFinishFacadeBase<TDistributedWriteFileFinishFacade, TDistributedFileSessionTraits>;
    using TTraits = TDistributedFileSessionTraits;

    friend class TDistributedWriteFinishFacadeBase<TDistributedWriteFileFinishFacade, TDistributedFileSessionTraits>;

public:
    TDistributedWriteFileFinishFacade(
        const IClientPtr& client,
        const NLogging::TLogger logger)
        : TBase(client, logger)
        , Client_(client)
    { }

protected:
    TTransactionId GetMasterTransaction(const TTraits::TSession& session)
    {
        return session.HostData.MainTransactionId;
    }

    TObjectId GetObjectId(const TTraits::TSession& session)
    {
        return session.HostData.FileId;
    }

    NYPath::TRichYPath GetPath(const TTraits::TSession& session)
    {
        return session.HostData.RichPath;
    }

    NObjectClient::TCellTag GetExternalCellTag(const TTraits::TSession& session)
    {
        return session.HostData.ExternalCellTag;
    }

    void SortResults(
        TNonNullPtr<std::vector<typename TTraits::TWriteFragmentResult>> /*results*/,
        const TTraits::TSession& /*session*/)
    { }

    void EndUpload(
        const TTraits::TSession& session,
        const NChunkClient::NProto::TDataStatistics& dataStatistics)
    {
        const auto& hostData = session.HostData;
        auto nativeCellTag = CellTagFromId(hostData.FileId);
        auto objectIdPath = FromObjectId(hostData.FileId);

        auto proxy = CreateObjectServiceWriteProxy(Client_, nativeCellTag);
        auto batchReq = proxy.ExecuteBatch();

        {
            auto req = TFileYPathProxy::EndUpload(objectIdPath);
            *req->mutable_statistics() = std::move(dataStatistics);
            SetTransactionId(req, session.UploadTransactionId);
            GenerateMutationId(req);
            batchReq->AddRequest(req, "end_upload");
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(
            GetCumulativeError(batchRspOrError),
            "Error finishing upload to file %v",
            hostData.RichPath.GetPath());
    }

private:
    const IClientPtr Client_;
};

////////////////////////////////////////////////////////////////////////////////

TDistributedWriteFileSessionWithCookies TClient::DoStartDistributedWriteFileSession(
    const TRichYPath& richPath,
    const TDistributedWriteFileSessionStartOptions& options)
{
    TDistributedWriteFileStartFacade facade(
        MakeStrong(this),
        Logger);
    return facade.StartSession(richPath, options);
}

void TClient::DoPingDistributedWriteFileSession(
    const TSignedDistributedWriteFileSessionPtr& session,
    const TDistributedWriteFileSessionPingOptions& options)
{
    TDistributedWriteFilePingFacade facade(
        MakeStrong(this));
    facade.PingSession(session, options);
}

void TClient::DoFinishDistributedWriteFileSession(
    const TDistributedWriteFileSessionWithResults& sessionWithResults,
    const TDistributedWriteFileSessionFinishOptions& options)
{
    TDistributedWriteFileFinishFacade facade(
        MakeStrong(this),
        Logger);
    facade.FinishSession(sessionWithResults, options);
}

IFileFragmentWriterPtr TClient::CreateFileFragmentWriter(
    const TSignedWriteFileFragmentCookiePtr& signedCookie,
    const TFileFragmentWriterOptions& options)
{
    YT_VERIFY(signedCookie);

    if (options.ComputeMD5) {
        THROW_ERROR_EXCEPTION("MD5 is not yet supported in distributed file API");
    }

    auto cookie = ConvertTo<TWriteFileFragmentCookie>(TYsonStringBuf(signedCookie.Underlying()->Payload()));

    return NFileClient::CreateFileFragmentWriter(
        options.Config ? options.Config : New<TFileWriterConfig>(),
        std::move(cookie),
        MakeStrong(this),
        cookie.CookieData.MainTransactionId,
        /*writeBlocksOptions*/ {},
        HeavyRequestMemoryUsageTracker_
    );
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi::NNative

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
