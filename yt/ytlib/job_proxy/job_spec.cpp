#include "stdafx.h"

#include "job_spec.h"

#include <ytlib/chunk_client/client_block_cache.h>
#include <ytlib/table_client/yson_table_input.h>
#include <ytlib/table_client/schema.h>
#include <ytlib/ytree/yson_writer.h>
#include <ytlib/election/leader_channel.h>
#include <ytlib/transaction_server/transaction_ypath_proxy.h>
#include <ytlib/object_server/id.h>
#include <ytlib/chunk_server/chunk_list_ypath_proxy.h>


namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = JobProxyLogger;

////////////////////////////////////////////////////////////////////

using namespace NScheduler;
using namespace NTableClient;
using namespace NFileClient;
using namespace NTransactionClient;
using namespace NTransactionServer;
using namespace NYTree;
using namespace NChunkClient;
using namespace NChunkServer;
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

////////////////////////////////////////////////////////////////////

TJobSpec::TJobSpec(
    TConfig* config,
    const NScheduler::NProto::TJobSpec& jobSpec,
    const TTransactionId& transactionId)
    : Config(config)
    , ProtoSpec(jobSpec)
    , TransactionId(transactionId)
    , StdErrTransactionId(NullTransactionId) // optional
    , StdErr(NULL)
{
    YASSERT(ProtoSpec.HasExtension(NScheduler::NProto::TMapJobSpec::map_job_spec));
    MapSpec = ProtoSpec.GetExtension(NScheduler::NProto::TMapJobSpec::map_job_spec);

    YASSERT(MapSpec.input_tables_size() ==
        ProtoSpec.operation_spec().input_tables_size());

    MasterChannel = CreateLeaderChannel(~Config->Masters);
}

int TJobSpec::GetInputCount() const 
{
    return ProtoSpec.operation_spec().input_tables_size();
}

int TJobSpec::GetOutputCount() const
{
    return ProtoSpec.operation_spec().output_tables_size();
}

TInputStream* TJobSpec::GetTableInput(int index)
{
    YASSERT(index < GetInputCount());

    TChannel channel = TChannel::FromProto(
        MapSpec.input_tables(index).channel());
    
    auto blockCache = CreateClientBlockCache(~New<TClientBlockCacheConfig>());

    yvector<NTableClient::NProto::TChunkSlice> slices(
        MapSpec.input_tables(index).chunk_slices().begin(),
        MapSpec.input_tables(index).chunk_slices().end());

    LOG_DEBUG("Creating %d input, %d slices.", 
        index, 
        slices.ysize());

    auto reader = New<TChunkSequenceReader>(
        ~Config->ChunkSequenceReader,
        channel,
        TransactionId,
        ~MasterChannel,
        ~blockCache,
        slices);

    return new TYsonTableInput(~reader, Config->OutputFormat);
}

NTableClient::ISyncWriter::TPtr TJobSpec::GetTableOutput(int index)
{
    const TYson& schema = MapSpec.output_tables(index).schema();
    auto chunkSequenceWriter = New<TChunkSequenceWriter>(
        ~Config->ChunkSequenceWriter,
        ~MasterChannel,
        TransactionId,
        NChunkServer::TChunkListId::FromProto(
            MapSpec.output_tables(index).chunk_list_id()),
        schema.empty()
            ? TSchema::Default()
            : TSchema::FromYson(schema)
    );

    return chunkSequenceWriter;
}

TOutputStream* TJobSpec::GetErrorOutput()
{
    if (ProtoSpec.has_std_err())
        return new TErrorOutput(
            ~Config->StdErr,
            ~MasterChannel,
            TObjectId::FromProto(ProtoSpec.std_err().transaction_id()),
            TObjectId::FromProto(ProtoSpec.std_err().chunk_list_id()));

    else
        return new TNullOutput();
}

Stroka TJobSpec::GetShellCommand() const
{
    return ProtoSpec.operation_spec().shell_command();
}

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT

