#include "stdafx.h"
#include "file_reader.h"
#include "file_chunk_reader.h"
#include "config.h"
#include "private.h"

#include <core/concurrency/fiber.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/cypress_client/rpc_helpers.h>

#include <ytlib/transaction_client/transaction_manager.h>
#include <ytlib/transaction_client/rpc_helpers.h>

#include <ytlib/chunk_client/private.h>
#include <ytlib/chunk_client/chunk_replica.h>
#include <ytlib/chunk_client/chunk_spec.h>
#include <ytlib/chunk_client/read_limit.h>

#include <ytlib/node_tracker_client/node_directory.h>

namespace NYT {
namespace NFileClient {
    
using namespace NRpc;
using namespace NYTree;
using namespace NConcurrency;
using namespace NYPath;
using namespace NChunkClient;
using namespace NTransactionClient;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NCypressClient;

////////////////////////////////////////////////////////////////////////////////

TAsyncReader::TAsyncReader(
    TFileReaderConfigPtr config,
    IChannelPtr masterChannel,
    IBlockCachePtr blockCache,
    TTransactionPtr transaction,
    const TRichYPath& richPath,
    const TNullable<i64>& offset,
    const TNullable<i64>& length)
    : Config(config)
    , MasterChannel(masterChannel)
    , BlockCache(blockCache)
    , Transaction(transaction)
    , RichPath(richPath.Simplify())
    , Offset(offset)
    , Length(length)
    , IsFirstBlock(true)
    , Size(0)
    , Logger(FileReaderLogger)
{
    YCHECK(Config);
    YCHECK(MasterChannel);
    YCHECK(BlockCache);

    Logger.AddTag(Sprintf("Path: %s, TransactionId: %s",
        ~RichPath.GetPath(),
        transaction ? ~ToString(transaction->GetId()) : ~ToString(NullTransactionId)));
}

TAsyncReader::~TAsyncReader()
{ }

TAsyncError TAsyncReader::Open()
{
    return BIND(&TAsyncReader::DoOpen, MakeStrong(this))
        .AsyncVia(NChunkClient::TDispatcher::Get()->GetReaderInvoker())
        .Run();
}

TError TAsyncReader::DoOpen()
{
    try {
        LOG_INFO("Opening file reader");

        LOG_INFO("Fetching file info");

        const auto& path = RichPath.GetPath();

        TObjectServiceProxy proxy(MasterChannel);
        auto batchReq = proxy.ExecuteBatch();

        {
            auto req = TYPathProxy::Get(path + "/@type");
            SetTransactionId(req, Transaction);
            batchReq->AddRequest(req, "get_type");
        }

        {
            auto attributes = CreateEphemeralAttributes();

            i64 lowerLimit = Offset.Get(0);
            if (Offset) {
                TReadLimit limit;
                limit.SetOffset(*Offset);
                attributes->SetYson("lower_limit", ConvertToYsonString(limit));
            }

            if (Length) {
                TReadLimit limit;
                limit.SetOffset(lowerLimit + *Length);
                attributes->SetYson("upper_limit", ConvertToYsonString(limit));
            }

            auto req = TFileYPathProxy::Fetch(RichPath.GetPath());
            ToProto(req->mutable_attributes(), *attributes);
            SetTransactionId(req, Transaction);
            SetSuppressAccessTracking(req, Config->SuppressAccessTracking);
            req->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
            batchReq->AddRequest(req, "fetch");
        }

        auto batchRsp = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(*batchRsp, "Error fetching file info");

        {
            auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_type");
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error getting object type");

            auto type = ConvertTo<EObjectType>(TYsonString(rsp->value()));
            if (type != EObjectType::File) {
                THROW_ERROR_EXCEPTION("Invalid type of %s: expected %s, actual %s",
                    ~RichPath.GetPath(),
                    ~FormatEnum(EObjectType(EObjectType::File)).Quote(),
                    ~FormatEnum(type).Quote());
            }
        }

        auto nodeDirectory = New<TNodeDirectory>();
        {
            auto rsp = batchRsp->GetResponse<TFileYPathProxy::TRspFetch>("fetch");
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error fetching file chunks");

            nodeDirectory->MergeFrom(rsp->node_directory());

            auto chunks = FromProto<NChunkClient::NProto::TChunkSpec>(rsp->chunks());
            for (const auto& chunk : chunks) {
                i64 dataSize;
                GetStatistics(chunk, &dataSize);
                Size += dataSize;
            }

            auto provider = New<TFileChunkReaderProvider>(Config);
            Reader = New<TReader>(
                Config,
                MasterChannel,
                BlockCache,
                nodeDirectory,
                std::move(chunks),
                provider);
        }

        {
            auto result = WaitFor(Reader->AsyncOpen());
            THROW_ERROR_EXCEPTION_IF_FAILED(result);
        }

        if (Transaction) {
            ListenTransaction(Transaction);
        }

        LOG_INFO("File reader opened");
        
        return TError();
    } catch (const std::exception& ex) {
        return ex;
    }
}

TFuture<TAsyncReader::TReadResult> TAsyncReader::Read()
{
    return BIND(&TAsyncReader::DoRead, MakeStrong(this))
        .AsyncVia(NChunkClient::TDispatcher::Get()->GetReaderInvoker())
        .Run();
}

TAsyncReader::TReadResult TAsyncReader::DoRead()
{
    try {
        CheckAborted();
        
        if (!IsFirstBlock && !Reader->FetchNext()) {
            auto result = WaitFor(Reader->GetReadyEvent());
            THROW_ERROR_EXCEPTION_IF_FAILED(result);
        }

        IsFirstBlock = false;

        auto* facade = Reader->GetFacade();
        return facade ? facade->GetBlock() : TSharedRef();
    } catch (const std::exception& ex) {
        return ex;
    }
}

i64 TAsyncReader::GetSize() const
{
    return Size;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
