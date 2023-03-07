#include "file_reader.h"
#include "config.h"
#include "connection.h"
#include "transaction.h"
#include "private.h"

#include <yt/ytlib/chunk_client/block.h>
#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/ytlib/chunk_client/dispatcher.h>
#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/file_client/file_chunk_reader.h>
#include <yt/ytlib/file_client/file_ypath_proxy.h>

#include <yt/ytlib/object_client/object_service_proxy.h>
#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/transaction_client/helpers.h>
#include <yt/ytlib/transaction_client/transaction_listener.h>

#include <yt/client/chunk_client/read_limit.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/client/object_client/helpers.h>

#include <yt/client/api/file_reader.h>

#include <yt/core/concurrency/async_stream.h>

#include <yt/core/yson/string.h>

#include <utility>

namespace NYT::NApi::NNative {

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
        : Client_(std::move(client))
        , Path_(path)
        , Options_(options)
        , Config_(options.Config ? options.Config : New<TFileReaderConfig>())
        , BlockReadOptions_{
            .WorkloadDescriptor = Config_->WorkloadDescriptor,
            .ChunkReaderStatistics = New<TChunkReaderStatistics>(),
            .ReadSessionId = TReadSessionId::Create()
        }
        , Logger(NLogging::TLogger(ApiLogger)
            .AddTag("Path: %v, TransactionId: %v, ReadSessionId: %v",
                Path_,
                Options_.TransactionId,
                BlockReadOptions_.ReadSessionId))
    {
        if (Options_.TransactionId) {
            Transaction_ = Client_->AttachTransaction(Options_.TransactionId);
        }
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

    virtual NHydra::TRevision GetRevision() const override
    {
        return Revision_;
    }

private:
    const IClientPtr Client_;
    const TYPath Path_;
    const TFileReaderOptions Options_;
    const TFileReaderConfigPtr Config_;

    const TClientBlockReadOptions BlockReadOptions_;
    const NLogging::TLogger Logger;

    NApi::ITransactionPtr Transaction_;
    NHydra::TRevision Revision_ = NHydra::NullRevision;
    NFileClient::IFileReaderPtr Reader_;


    void DoOpen()
    {
        YT_LOG_INFO("Opening file reader");

        TUserObject userObject(Path_);

        GetUserObjectBasicAttributes(
            Client_,
            {&userObject},
            Transaction_ ? Transaction_->GetId() : NullTransactionId,
            Logger,
            EPermission::Read,
            TGetUserObjectBasicAttributesOptions{
                .SuppressAccessTracking = Options_.SuppressAccessTracking
            });

        if (userObject.Type != EObjectType::File) {
            THROW_ERROR_EXCEPTION("Invalid type of %v: expected %Qlv, actual %Qlv",
                Path_,
                EObjectType::File,
                userObject.Type);
        }

        {
            YT_LOG_INFO("Requesting extended file attributes");

            auto channel = Client_->GetMasterChannelOrThrow(EMasterChannelKind::Follower, userObject.ExternalCellTag);
            TObjectServiceProxy proxy(channel);

            auto req = TYPathProxy::Get(userObject.GetObjectIdPath() + "/@");
            ToProto(req->mutable_attributes()->mutable_keys(), std::vector<TString>{
                "revision"
            });
            AddCellTagToSyncWith(req, userObject.ObjectId);
            SetTransactionId(req, userObject.ExternalTransactionId);
            SetSuppressAccessTracking(req, Options_.SuppressAccessTracking);

            auto rspOrError = WaitFor(proxy.Execute(req));
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error requesting extended attributes of file %v",
                Path_);
            const auto& rsp = rspOrError.Value();

            auto attributes = ConvertToAttributes(NYson::TYsonString(rsp->value()));
            Revision_ = attributes->Get<NHydra::TRevision>("revision", NHydra::NullRevision);
        }

        auto nodeDirectory = New<TNodeDirectory>();
        std::vector<NChunkClient::NProto::TChunkSpec> chunkSpecs;

        bool emptyRead = Options_.Length && *Options_.Length == 0;
        if (!emptyRead) {
            YT_LOG_INFO("Fetching file chunks");

            auto channel = Client_->GetMasterChannelOrThrow(EMasterChannelKind::Follower, userObject.ExternalCellTag);
            TObjectServiceProxy proxy(channel);

            auto req = TFileYPathProxy::Fetch(userObject.GetObjectIdPath());
            AddCellTagToSyncWith(req, userObject.ObjectId);

            TReadLimit lowerLimit, upperLimit;
            i64 offset = Options_.Offset.value_or(0);
            if (Options_.Offset) {
                lowerLimit.SetOffset(offset);
            }
            if (Options_.Length) {
                if (*Options_.Length < 0) {
                    THROW_ERROR_EXCEPTION("Invalid length to read from file: %v < 0",
                        *Options_.Length);
                }
                upperLimit.SetOffset(offset + *Options_.Length);
            }

            ToProto(req->mutable_ranges(), std::vector<TReadRange>({TReadRange(lowerLimit, upperLimit)}));

            SetTransactionId(req, userObject.ExternalTransactionId);
            SetSuppressAccessTracking(req, Options_.SuppressAccessTracking);
            req->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);

            auto rspOrError = WaitFor(proxy.Execute(req));
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error fetching chunks for file %v",
                Path_);
            const auto& rsp = rspOrError.Value();

            ProcessFetchResponse(
                Client_,
                rsp,
                userObject.ExternalCellTag,
                nodeDirectory,
                Config_->MaxChunksPerLocateRequest,
                std::nullopt,
                Logger,
                &chunkSpecs);
        }

        Reader_ = CreateFileMultiChunkReader(
            Config_,
            New<TMultiChunkReaderOptions>(),
            Client_,
            /* localDescriptor */ {},
            /* partitionTag */ std::nullopt,
            Client_->GetNativeConnection()->GetBlockCache(),
            nodeDirectory,
            BlockReadOptions_,
            std::move(chunkSpecs));

        if (Transaction_) {
            StartListenTransaction(Transaction_);
        }

        YT_LOG_INFO("File reader opened");
    }
};

TFuture<IFileReaderPtr> CreateFileReader(
    IClientPtr client,
    const NYPath::TYPath& path,
    const TFileReaderOptions& options)
{
    auto fileReader = New<TFileReader>(client, path, options);

    return fileReader->Open().Apply(BIND([=] {
        return static_cast<IFileReaderPtr>(fileReader);
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
