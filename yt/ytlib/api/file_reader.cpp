#include "file_reader.h"
#include "private.h"
#include "config.h"
#include "native_connection.h"
#include "transaction.h"

#include <yt/ytlib/chunk_client/block.h>
#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/ytlib/chunk_client/dispatcher.h>
#include <yt/ytlib/chunk_client/helpers.h>
#include <yt/ytlib/chunk_client/read_limit.h>

#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/file_client/file_chunk_reader.h>
#include <yt/ytlib/file_client/file_ypath_proxy.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/ytlib/object_client/object_service_proxy.h>
#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/transaction_client/helpers.h>
#include <yt/ytlib/transaction_client/transaction_listener.h>

#include <yt/core/concurrency/async_stream.h>

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
    , public IAsyncZeroCopyInputStream
{
public:
    TFileReader(
        INativeClientPtr client,
        const TYPath& path,
        const TFileReaderOptions& options)
        : Client_(client)
        , Path_(path)
        , Options_(options)
        , Config_(options.Config ? options.Config : New<TFileReaderConfig>())
        , Logger(ApiLogger)
    {
        if (Options_.TransactionId) {
            Transaction_ = Client_->AttachTransaction(Options_.TransactionId);
        }

        BlockReadOptions_.WorkloadDescriptor = Config_->WorkloadDescriptor;
        BlockReadOptions_.ChunkReaderStatistics = New<TChunkReaderStatistics>();
        BlockReadOptions_.ReadSessionId = TReadSessionId::Create();

        Logger.AddTag("Path: %v, TransactionId: %v, ReadSessionId: %v",
            Path_,
            Options_.TransactionId,
            BlockReadOptions_.ReadSessionId);
    }

    TFuture<void> Open()
    {
        return BIND(&TFileReader::DoOpen, MakeStrong(this))
            .AsyncVia(NChunkClient::TDispatcher::Get()->GetReaderInvoker())
            .Run();
    }

    virtual TFuture<TSharedRef> Read() override
    {
        ValidateAborted();

        TBlock block;
        if (!Reader_ || !Reader_->ReadBlock(&block)) {
            return MakeFuture(TSharedRef());
        }

        if (block.Data) {
            return MakeFuture(block.Data);
        }

        return Reader_->GetReadyEvent().Apply(
            BIND(&TFileReader::Read, MakeStrong(this)));
    }

private:
    const INativeClientPtr Client_;
    const TYPath Path_;
    const TFileReaderOptions Options_;
    const TFileReaderConfigPtr Config_;

    ITransactionPtr Transaction_;

    TClientBlockReadOptions BlockReadOptions_;

    NFileClient::IFileReaderPtr Reader_;

    NLogging::TLogger Logger;


    void DoOpen()
    {
        LOG_INFO("Opening file reader");

        TUserObject userObject;
        userObject.Path = Path_;

        GetUserObjectBasicAttributes(
            Client_,
            TMutableRange<TUserObject>(&userObject, 1),
            Transaction_ ? Transaction_->GetId() : NullTransactionId,
            Logger,
            EPermission::Read,
            Options_.SuppressAccessTracking);

        const auto cellTag = userObject.CellTag;
        const auto& objectId = userObject.ObjectId;

        auto objectIdPath = FromObjectId(objectId);

        if (userObject.Type != EObjectType::File) {
            THROW_ERROR_EXCEPTION("Invalid type of %v: expected %Qlv, actual %Qlv",
                Path_,
                EObjectType::File,
                userObject.Type);
        }

        auto nodeDirectory = New<TNodeDirectory>();
        std::vector<NChunkClient::NProto::TChunkSpec> chunkSpecs;

        bool isEmptyRead = Options_.Length && *Options_.Length == 0;
        if (!isEmptyRead) {
            LOG_INFO("Fetching file chunks");

            auto channel = Client_->GetMasterChannelOrThrow(EMasterChannelKind::Follower, cellTag);
            TObjectServiceProxy proxy(channel);

            auto req = TFileYPathProxy::Fetch(objectIdPath);

            TReadLimit lowerLimit, upperLimit;
            i64 offset = Options_.Offset.Get(0);
            if (Options_.Offset) {
                lowerLimit.SetOffset(offset);
            }
            if (Options_.Length) {
                if (*Options_.Length < 0) {
                    THROW_ERROR_EXCEPTION("Invalid length to read from file: %v < 0", *Options_.Length);
                }
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

            ProcessFetchResponse(
                Client_,
                rsp,
                cellTag,
                nodeDirectory,
                Config_->MaxChunksPerLocateRequest,
                Null,
                Logger,
                &chunkSpecs);
        }

        Reader_ = CreateFileMultiChunkReader(
            Config_,
            New<TMultiChunkReaderOptions>(),
            Client_,
            TNodeDescriptor(),
            Client_->GetNativeConnection()->GetBlockCache(),
            nodeDirectory,
            BlockReadOptions_,
            std::move(chunkSpecs));

        if (Transaction_) {
            ListenTransaction(Transaction_);
        }

        LOG_INFO("File reader opened");
    }

};

TFuture<IAsyncZeroCopyInputStreamPtr> CreateFileReader(
    INativeClientPtr client,
    const NYPath::TYPath& path,
    const TFileReaderOptions& options)
{
    auto fileReader = New<TFileReader>(client, path, options);

    return fileReader->Open().Apply(BIND([=] {
        return static_cast<IAsyncZeroCopyInputStreamPtr>(fileReader);
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT
