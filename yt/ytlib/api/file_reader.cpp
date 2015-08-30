#include "stdafx.h"
#include "file_reader.h"
#include "connection.h"
#include "config.h"
#include "private.h"

#include <ytlib/object_client/object_service_proxy.h>
#include <ytlib/object_client/helpers.h>

#include <ytlib/cypress_client/rpc_helpers.h>

#include <ytlib/transaction_client/transaction_manager.h>
#include <ytlib/transaction_client/transaction_listener.h>
#include <ytlib/transaction_client/helpers.h>

#include <ytlib/chunk_client/read_limit.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/dispatcher.h>
#include <ytlib/chunk_client/helpers.h>

#include <ytlib/file_client/file_chunk_reader.h>
#include <ytlib/file_client/file_ypath_proxy.h>

#include <ytlib/node_tracker_client/node_directory.h>

namespace NYT {
namespace NApi {

using namespace NRpc;
using namespace NYTree;
using namespace NConcurrency;
using namespace NYPath;
using namespace NChunkClient;
using namespace NFileClient;
using namespace NTransactionClient;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NCypressClient;

////////////////////////////////////////////////////////////////////////////////

class TFileReader
    : public TTransactionListener
    , public IFileReader
{
public:
    TFileReader(
        IClientPtr client,
        const TYPath& path,
        const TFileReaderOptions& options)
        : Client_(client)
        , Path_(path)
        , Options_(options)
        , Config_(options.Config ? options.Config : New<TFileReaderConfig>())
        , Logger(ApiLogger)
    {
        if (Options_.TransactionId) {
            auto transactionManager = Client_->GetTransactionManager();
            Transaction_ = transactionManager->Attach(Options_.TransactionId);
        }

        Logger.AddTag("Path: %v, TransactionId: %v",
            Path_,
            Options_.TransactionId);
    }

    virtual TFuture<void> Open() override
    {
        return BIND(&TFileReader::DoOpen, MakeStrong(this))
            .AsyncVia(NChunkClient::TDispatcher::Get()->GetReaderInvoker())
            .Run();
    }

    virtual TFuture<TSharedRef> Read() override
    {
        ValidateAborted();

        TSharedRef block;
        if (!Reader_->ReadBlock(&block)) {
            return MakeFuture(TSharedRef());
        }

        if (block) {
            return MakeFuture(block);
        }

        return Reader_->GetReadyEvent().Apply(
            BIND(&TFileReader::Read, MakeStrong(this)));
    }

private:
    const IClientPtr Client_;
    const TYPath Path_;
    const TFileReaderOptions Options_;
    const TFileReaderConfigPtr Config_;

    TTransactionPtr Transaction_;

    IFileMultiChunkReaderPtr Reader_;

    NLogging::TLogger Logger;


    void DoOpen()
    {
        LOG_INFO("Opening file reader");

        auto cellTag = InvalidCellTag;
        TObjectId objectId;

        {
            LOG_INFO("Requesting basic attributes");

            auto channel = Client_->GetMasterChannel(EMasterChannelKind::LeaderOrFollower);
            TObjectServiceProxy proxy(channel);

            auto req = TFileYPathProxy::GetBasicAttributes(Path_);
            req->set_permissions(static_cast<ui32>(EPermission::Read));
            SetTransactionId(req, Transaction_);
            SetSuppressAccessTracking(req, Options_.SuppressAccessTracking);

            auto rspOrError = WaitFor(proxy.Execute(req));
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error getting basic attributes for file %v",
                Path_);

            const auto& rsp = rspOrError.Value();

            objectId = FromProto<TObjectId>(rsp->object_id());
            cellTag = rsp->cell_tag();

            LOG_INFO("Basic attributes received (ObjectId: %v, CellTag: %v)",
                objectId,
                cellTag);
        }

        auto objectIdPath = FromObjectId(objectId);

        {
            auto type = TypeFromId(objectId);
            if (type != EObjectType::File) {
                THROW_ERROR_EXCEPTION("Invalid type of %v: expected %Qlv, actual %Qlv",
                    Path_,
                    EObjectType::File,
                    type);
            }
        }

        auto nodeDirectory = New<TNodeDirectory>();
        std::vector<NChunkClient::NProto::TChunkSpec> chunkSpecs;

        {
            LOG_INFO("Fetching file chunks");

            auto channel = Client_->GetMasterChannel(EMasterChannelKind::LeaderOrFollower, cellTag);
            TObjectServiceProxy proxy(channel);

            auto req = TFileYPathProxy::Fetch(objectIdPath);

            TReadLimit lowerLimit, upperLimit;
            i64 offset = Options_.Offset.Get(0);
            if (Options_.Offset) {
                lowerLimit.SetOffset(offset);
            }
            if (Options_.Length) {
                upperLimit.SetOffset(offset + *Options_.Length);
            }

            ToProto(req->mutable_ranges(), std::vector<TReadRange>({TReadRange(lowerLimit, upperLimit)}));

            SetTransactionId(req, Transaction_);
            SetSuppressAccessTracking(req, Options_.SuppressAccessTracking);
            req->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);

            auto rspOrError = WaitFor(proxy.Execute(req));
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error fetching chunks for file %v",
                Path_);
            const auto& rsp = rspOrError.Value();

            chunkSpecs = ProcessFetchResponse(
                Client_,
                rsp,
                cellTag,
                nodeDirectory,
                Config_->MaxChunksPerLocateRequest,
                Logger);
        }

        Reader_ = CreateFileMultiChunkReader(
            Config_,
            New<TMultiChunkReaderOptions>(),
            Client_,
            Client_->GetConnection()->GetBlockCache(),
            nodeDirectory,
            std::move(chunkSpecs));

        WaitFor(Reader_->Open())
            .ThrowOnError();

        if (Transaction_) {
            ListenTransaction(Transaction_);
        }

        LOG_INFO("File reader opened");
    }

};

IFileReaderPtr CreateFileReader(
    IClientPtr client,
    const TYPath& path,
    const TFileReaderOptions& options)
{
    return New<TFileReader>(client, path, options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT
