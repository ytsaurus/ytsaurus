#include "stdafx.h"

#include "reduce_job_io.h"
#include "config.h"

#include <ytlib/chunk_client/client_block_cache.h>
#include <ytlib/table_client/table_chunk_sequence_reader.h>
#include <ytlib/table_client/sync_reader.h>
#include <ytlib/table_client/table_producer.h>
#include <ytlib/table_client/merging_reader.h>

namespace NYT {
namespace NJobProxy {

using namespace NScheduler;
using namespace NScheduler::NProto;
using namespace NTableClient;
using namespace NYTree;
using namespace NTransactionClient;
using namespace NChunkClient;
using namespace NChunkServer;

////////////////////////////////////////////////////////////////////

TReduceJobIO::TReduceJobIO(
    TJobIOConfigPtr config,
    NElection::TMasterDiscovery::TConfigPtr mastersConfig,
    const NScheduler::NProto::TJobSpec& jobSpec)
    : TUserJobIO(config, mastersConfig, jobSpec)
{ }

TAutoPtr<NTableClient::TTableProducer>
TReduceJobIO::CreateTableInput(int index, NYTree::IYsonConsumer* consumer) const
{
    YASSERT(index < GetInputCount());

    auto blockCache = CreateClientBlockCache(New<TClientBlockCacheConfig>());

    std::vector<TTableChunkSequenceReaderPtr> readers;
    TReaderOptions options;
    options.ReadKey = true;

    FOREACH (const auto& inputSpec, JobSpec.input_specs()) {
        // ToDo(psushin): validate that input chunks are sorted.
        std::vector<NTableClient::NProto::TInputChunk> chunks(
            inputSpec.chunks().begin(),
            inputSpec.chunks().end());

        auto reader = New<TTableChunkSequenceReader>(
            Config->ChunkSequenceReader,
            MasterChannel,
            blockCache,
            MoveRV(chunks),
            options);

        readers.push_back(reader);
    }

    auto reader = New<TMergingReader>(readers);
    reader->Open();

    return new TTableProducer(reader, consumer);
}

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
