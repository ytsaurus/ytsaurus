#include "stdafx.h"

#include "map_job_io.h"
#include "table_output.h"
#include "config.h"

// ToDo(psushin): use public.h everywhere.
#include <ytlib/chunk_client/client_block_cache.h>
#include <ytlib/table_client/chunk_sequence_reader.h>
#include <ytlib/table_client/chunk_sequence_writer.h>
#include <ytlib/table_client/sync_writer.h>
#include <ytlib/table_client/sync_reader.h>
#include <ytlib/table_client/table_producer.h>
#include <ytlib/table_client/schema.h>


/*
#include <ytlib/file_client/file_writer_base.h>
#include <ytlib/table_client/yson_table_input.h>
#include <ytlib/table_client/schema.h>
#include <ytlib/ytree/yson_writer.h>
#include <ytlib/transaction_server/transaction_ypath_proxy.h>
#include <ytlib/object_server/id.h>
#include <ytlib/chunk_server/chunk_list_ypath_proxy.h>
*/

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = JobProxyLogger;

////////////////////////////////////////////////////////////////////


using namespace NScheduler;
using namespace NScheduler::NProto;
using namespace NTableClient;
using namespace NYTree;
using namespace NTransactionClient;
using namespace NChunkClient;
using namespace NChunkServer;

/*
using namespace NFileClient;

using namespace NCypress;


////////////////////////////////////////////////////////////////////

class TErrorOutput
    : public TOutputStream
{
public:
    TErrorOutput(
        TFileWriterBase::TConfig* config, 
        NRpc::IChannel* masterChannel,
        const TTransactionId& transactionId,
        const TObjectId& chunkListId)
        : FileWriter(New<TFileWriterBase>(config, masterChannel))
        , MasterChannel(masterChannel)
        , TransactionId(transactionId)
        , ChunkListId(chunkListId)
    {
        FileWriter->Open(TransactionId);
    }

    ~TErrorOutput() throw()
    { }

protected: 
    void DoWrite(const void* buf, size_t len) 
    {
        FileWriter->Write(reinterpret_cast<const char*>(buf), len);
    }

    void DoFinish() 
    {
        FileWriter->Close();

        TCypressServiceProxy proxy(~MasterChannel);
        auto batchReq = proxy.ExecuteBatch();
        {
            auto req = TChunkListYPathProxy::Attach(FromObjectId(ChunkListId));
            req->add_children_ids(FileWriter->GetChunkId().ToProto());
            batchReq->AddRequest(~req);
        }
        {
            auto req = TTransactionYPathProxy::ReleaseObject(FromObjectId(TransactionId));
            req->set_object_id(FileWriter->GetChunkId().ToProto());
            batchReq->AddRequest(~req);
        }

        auto batchRsp = batchReq->Invoke()->Get();

        if (!batchRsp->IsOK()) {
            ythrow yexception() << Sprintf(
                "Request to attach chunk with stderr failed (error: %s)", 
                ~batchRsp->GetError().GetMessage());
        }

        for (int i = 0; i < batchRsp->GetSize(); ++i) {
            auto rsp = batchRsp->GetResponse(i);
            if (!rsp->IsOK()) {
                ythrow yexception() << Sprintf(
                    "Failed to attach chunk with stderr (error: %s)", 
                    ~rsp->GetError().GetMessage());
            }
        }
    }

private:
    TFileWriterBase::TPtr FileWriter;
    NRpc::IChannel::TPtr MasterChannel;
    TTransactionId TransactionId;
    TObjectId ChunkListId;
};

*/
////////////////////////////////////////////////////////////////////


TMapJobIO::TMapJobIO(
    TJobIOConfigPtr config,
    NRpc::IChannel* masterChannel,
    const NScheduler::NProto::TMapJobSpec& ioSpec)
    : Config(config)
    , MasterChannel(masterChannel)
    , IoSpec(ioSpec)
{ }

int TMapJobIO::GetInputCount() const 
{
    // Always single input for map.
    return 1;
}

int TMapJobIO::GetOutputCount() const
{
    return IoSpec.output_specs_size();
}

TAutoPtr<NTableClient::TTableProducer> 
TMapJobIO::CreateTableInput(int index, NYTree::IYsonConsumer* consumer) const
{
    YASSERT(index < GetInputCount());

    auto blockCache = CreateClientBlockCache(~New<TClientBlockCacheConfig>());

    std::vector<NTableClient::NProto::TInputChunk> chunks(
        IoSpec.input_spec().chunks().begin(),
        IoSpec.input_spec().chunks().end());

    LOG_DEBUG("Creating %d input from %d chunks", 
        index, 
        static_cast<int>(chunks.size()));

    auto reader = New<TChunkSequenceReader>(
        Config->ChunkSequenceReader,
        MasterChannel,
        blockCache,
        chunks);

    // ToDo(psushin): extract format from operation spec.
    return new TTableProducer(
        New<TSyncReaderAdapter>(reader), 
        consumer);
}

TAutoPtr<TOutputStream> TMapJobIO::CreateTableOutput(int index) const
{
    YASSERT(index < GetOutputCount());
    const TYson& channels = IoSpec.output_specs(index).channels();
    YASSERT(!channels.empty());

    auto chunkSequenceWriter = New<TChunkSequenceWriter>(
        Config->ChunkSequenceWriter,
        MasterChannel,
        TTransactionId::FromProto(IoSpec.output_transaction_id()),
        TChunkListId::FromProto(IoSpec.output_specs(index).chunk_list_id()),
        ChannelsFromYson(channels));

    return new TTableOutput(New<TSyncWriterAdapter>(chunkSequenceWriter));
}

TAutoPtr<TOutputStream> TMapJobIO::CreateErrorOutput() const
{
    /*
    if (ProtoSpec.has_std_err())
        return new TErrorOutput(
            ~Config->StdErr,
            ~MasterChannel,
            TObjectId::FromProto(ProtoSpec.std_err().transaction_id()),
            TObjectId::FromProto(ProtoSpec.std_err().chunk_list_id()));

    else*/

    return new TNullOutput();
}

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
