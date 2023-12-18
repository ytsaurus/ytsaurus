#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>

#include <yt/yt/ytlib/transaction_supervisor/transaction_participant_service_proxy.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/wire_protocol.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/client/rpc/helpers.h>

#include <yt/yt/core/bus/tcp/config.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/mpsc_stack.h>

#include <yt/yt/core/rpc/bus/channel.h>

#include <yt/yt/core/yson/string.h>

#include <util/string/cast.h>

#include <library/cpp/getopt/last_getopt.h>

#include <library/cpp/yt/memory/chunked_memory_pool.h>

#include <vector>
#include <thread>

namespace NYT {
namespace {

using namespace NRpc;
using namespace NApi;
using namespace NConcurrency;
using namespace NTableClient;
using namespace NTracing;
using namespace NTransactionClient;
using namespace NTabletClient;
using namespace NYTree;
using namespace NYson;
using namespace NHiveClient;
using namespace NTransactionSupervisor;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EDataNodeRequest,
    (LookupRows)
    (GetChunkFragmentSet)
);

struct TOpts
{
    TOpts()
        : Opts(NLastGetopt::TOpts::Default())
    {
        Opts.AddLongOption("data-node-request", "Type of data node request")
            .Required().RequiredArgument("STRING")
            .StoreResult(&DataNodeRequest);

        Opts.AddLongOption("host-address", "Host address")
            .Required().RequiredArgument("STRING")
            .StoreResult(&HostAddress);
        Opts.AddLongOption("chunk-id", "Chunk id")
            .Required().RequiredArgument("STRING")
            .StoreResult(&ChunkId);

        // LookupRows stuff.
        Opts.AddLongOption("key", "Key comprising a single int64 value")
            .RequiredArgument("STRING")
            .StoreResult(&Key);
        Opts.AddLongOption("table-id", "Table id")
            .RequiredArgument("STRING")
            .StoreResult(&TableId);
        Opts.AddLongOption("table-revision", "Table mount revision")
            .RequiredArgument("NUMBER")
            .StoreResult(&TableRevision);
        Opts.AddLongOption("table-schema", "Table schema")
            .RequiredArgument("STRING")
            .StoreResult(&TableSchema);

        // GetChunkFragmentSet stuff.
        Opts.AddLongOption("read-session-id", "Read session id")
            .RequiredArgument("STRING")
            .StoreResult(&ReadSessionId);
        Opts.AddLongOption("use-direct-io", "Whether to open file with direct IO")
            .NoArgument()
            .SetFlag(&UseDirectIO);
        Opts.AddLongOption("fragment-length", "Chunk fragment length")
            .RequiredArgument("NUMBER")
            .StoreResult(&FragmentLength);
        Opts.AddLongOption("block-index", "Block index of the fragment")
            .RequiredArgument("NUMBER")
            .StoreResult(&BlockIndex);
        Opts.AddLongOption("block-offset", "Offset of the fragment within the block")
            .RequiredArgument("NUMBER")
            .StoreResult(&BlockOffset);
    }

    NLastGetopt::TOpts Opts;

    TString DataNodeRequest;

    TString HostAddress;
    TString ChunkId;

    // LookupRows stuff.
    TString Key;
    TString TableId;
    ui64 TableRevision;
    TString TableSchema;

    // GetChunkFragmentSet stuff.
    TString ReadSessionId;
    bool UseDirectIO = false;
    i32 FragmentLength = -1;
    i32 BlockIndex = -1;
    i64 BlockOffset = -1;
};

////////////////////////////////////////////////////////////////////////////////

void LookupRows(const TOpts& opts)
{
    auto addr = opts.HostAddress;
    auto chunkId = TGuid::FromString(opts.ChunkId);

    auto keyValue = std::stoi(opts.Key);
    auto tableId = TGuid::FromString(opts.TableId);
    auto tableRevision = opts.TableRevision;
    auto tableSchemaString = opts.TableSchema;

    auto channel = NRpc::NBus::CreateTcpBusChannelFactory(New<NYT::NBus::TBusConfig>())->CreateChannel(addr);
    TDataNodeServiceProxy proxy(channel);

    auto req = proxy.LookupRows();
    SetRequestWorkloadDescriptor(req, TWorkloadDescriptor(EWorkloadCategory::UserInteractive));
    ToProto(req->mutable_chunk_id(), chunkId);
    ToProto(req->mutable_read_session_id(), TGuid::Create());
    req->set_produce_all_versions(true);

    NYT::NQueryClient::TOwningRowBuilder keyBuilder(1);
    keyBuilder.AddValue(MakeUnversionedInt64Value(keyValue, 0));
    auto key = keyBuilder.FinishRow();
    auto writer = NTableClient::CreateWireProtocolWriter();
    writer->WriteUnversionedRowset(MakeRange({TUnversionedRow(key)}));
    req->Attachments() = writer->Finish();

    TTableSchema tableSchema;
    Deserialize(tableSchema, ConvertToNode(TYsonString(TString(tableSchemaString))));

    auto schemaData = req->mutable_schema_data();
    ToProto(schemaData->mutable_table_id(), tableId);
    schemaData->set_revision(tableRevision);
    ToProto(schemaData->mutable_schema(), tableSchema);

    auto rsp = WaitFor(req->Invoke())
        .ValueOrThrow();

    Cout << "Fetched rows: " << rsp->fetched_rows() << Endl;
    Cout << "Requested schema: " << rsp->request_schema() << Endl;

    if (!rsp->fetched_rows() || rsp->request_schema()) {
        Cerr << "Unexpected." << Endl;
        return;
    }

    struct TDataBufferTag { };
    auto rowBuffer = New<TRowBuffer>(TDataBufferTag());
    auto tableSchemaData = IWireProtocolReader::GetSchemaData(tableSchema, NTableClient::TColumnFilter());
    auto reader = CreateWireProtocolReader(rsp->Attachments()[0], rowBuffer);

    auto fetchedRow = reader->ReadVersionedRow(tableSchemaData, true);

    Cout << "Fetched row: " << ToString(fetchedRow);
}

////////////////////////////////////////////////////////////////////////////////

void GetChunkFragmentSet(const TOpts& opts)
{
    auto addr = opts.HostAddress;
    auto chunkId = TGuid::FromString(opts.ChunkId);

    auto readSessionId = TGuid::FromString(opts.ReadSessionId);
    auto useDirectIO = opts.UseDirectIO;

    auto fragmentLength = opts.FragmentLength;
    auto blockIndex = opts.BlockIndex;
    auto blockOffset = opts.BlockOffset;
    if (fragmentLength < 0 || blockIndex < 0 || blockOffset < 0) {
        THROW_ERROR_EXCEPTION("Fragment length, block index and offset must be specified for"
            "GetChunkFragmentSet request and must be nonnegative");
    }

    auto channel = NRpc::NBus::CreateTcpBusChannelFactory(New<NYT::NBus::TBusConfig>())->CreateChannel(addr);
    TDataNodeServiceProxy proxy(channel);

    auto req = proxy.GetChunkFragmentSet();
    ToProto(req->mutable_read_session_id(), readSessionId);
    req->set_use_direct_io(useDirectIO);
    SetRequestWorkloadDescriptor(req, TWorkloadDescriptor(EWorkloadCategory::UserInteractive));

    auto* subrequest = req->add_subrequests();
    ToProto(subrequest->mutable_chunk_id(), chunkId);
    auto* fragment = subrequest->add_fragments();
    fragment->set_length(fragmentLength);
    fragment->set_block_index(blockIndex);
    fragment->set_block_offset(blockOffset);

    auto rsp = WaitFor(req->Invoke())
        .ValueOrThrow();

    Cout << "Got " << rsp->subresponses_size() << " subresponses" << Endl;
    Cout << "Got " << rsp->Attachments().size() << " attachments" << Endl;

    for (auto& subresponse : rsp->subresponses()) {
        Cout << "Has complete chunk: " << subresponse.has_complete_chunk() << Endl;
    }
}

////////////////////////////////////////////////////////////////////////////////

void GuardedMain(int argc, char* argv[])
{
    TOpts opts;
    NLastGetopt::TOptsParseResult parseResult(&opts.Opts, argc, argv);

    auto dataNodeRequest = ConvertTo<EDataNodeRequest>(TYsonString(opts.DataNodeRequest));
    switch (dataNodeRequest) {
        case EDataNodeRequest::LookupRows:
            LookupRows(opts);
            break;

        case EDataNodeRequest::GetChunkFragmentSet:
            GetChunkFragmentSet(opts);
            break;

        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT

int main(int argc, char* argv[])
{
    try {
        NYT::GuardedMain(argc, argv);
    } catch (std::exception& e) {
        Cerr << ToString(NYT::TError(e)) << Endl;
    }

    return 0;
}
