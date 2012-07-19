#include "stdafx.h"
#include "map_job_io.h"
#include "table_output.h"
#include "config.h"
#include "stderr_output.h"

#include <ytlib/chunk_client/client_block_cache.h>
#include <ytlib/table_client/table_chunk_sequence_reader.h>
#include <ytlib/table_client/sync_reader.h>
#include <ytlib/table_client/table_producer.h>

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

////////////////////////////////////////////////////////////////////

TMapJobIO::TMapJobIO(
    TJobIOConfigPtr config,
    NElection::TMasterDiscovery::TConfigPtr mastersConfig,
    const NScheduler::NProto::TJobSpec& jobSpec)
    : TUserJobIO(config, mastersConfig, jobSpec)
{ }

TAutoPtr<NTableClient::TTableProducer>
TMapJobIO::CreateTableInput(int index, NYTree::IYsonConsumer* consumer) const
{
    YASSERT(index < GetInputCount());

    auto blockCache = CreateClientBlockCache(New<TClientBlockCacheConfig>());

    std::vector<NTableClient::NProto::TInputChunk> chunks(
        JobSpec.input_specs(0).chunks().begin(),
        JobSpec.input_specs(0).chunks().end());

    LOG_DEBUG("Opening input %d with %d chunks", 
        index, 
        static_cast<int>(chunks.size()));

    auto reader = New<TTableChunkSequenceReader>(
        Config->ChunkSequenceReader,
        MasterChannel,
        blockCache,
        MoveRV(chunks));
    auto syncReader = CreateSyncReader(reader);
    syncReader->Open();

    return new TTableProducer(syncReader, consumer);
}

} // namespace NJobProxy
} // namespace NYT
