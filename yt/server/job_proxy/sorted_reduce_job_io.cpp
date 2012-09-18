#include "stdafx.h"
#include "sorted_reduce_job_io.h"
#include "config.h"

#include <ytlib/chunk_client/client_block_cache.h>
#include <ytlib/table_client/multi_chunk_sequential_reader.h>
#include <ytlib/table_client/table_chunk_reader.h>
#include <ytlib/table_client/sync_reader.h>
#include <ytlib/table_client/table_producer.h>
#include <ytlib/table_client/merging_reader.h>

namespace NYT {
namespace NJobProxy {

using namespace NScheduler;
using namespace NScheduler::NProto;
using namespace NTableClient;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////

class TSortedReduceJobIO
    : public TUserJobIO
{
public:
    TSortedReduceJobIO(
        TJobIOConfigPtr ioConfig,
        NMetaState::TMasterDiscoveryConfigPtr mastersConfig,
        const TJobSpec& jobSpec)
        : TUserJobIO(ioConfig, mastersConfig, jobSpec)
    { }

    TAutoPtr<TTableProducer> CreateTableInput(
        int index, 
        NYTree::IYsonConsumer* consumer) override
    {
        YCHECK(index >= 0 && index < GetInputCount());

        auto blockCache = CreateClientBlockCache(New<TClientBlockCacheConfig>());

        std::vector<TTableChunkSequenceReaderPtr> readers;
        TReaderOptions options;
        options.ReadKey = true;

        auto provider = New<TTableChunkReaderProvider>(IOConfig->TableReader, options);

        FOREACH (const auto& inputSpec, JobSpec.input_specs()) {
            // ToDo(psushin): validate that input chunks are sorted.
            std::vector<NTableClient::NProto::TInputChunk> chunks(
                inputSpec.chunks().begin(),
                inputSpec.chunks().end());

            auto reader = New<TTableChunkSequenceReader>(
                IOConfig->TableReader,
                MasterChannel,
                blockCache,
                MoveRV(chunks),
                provider);

            readers.push_back(reader);
        }

        auto reader = CreateMergingReader(readers);
        reader->Open();

        // ToDo(psushin): init all inputs in constructor, get rid of this check.
        YCHECK(index == Inputs.size());
        Inputs.push_back(reader);

        return new TTableProducer(reader, consumer);
    }

    virtual void PopulateResult(TJobResult* result) override
    {
        auto* resultExt = result->MutableExtension(NScheduler::NProto::TReduceJobResultExt::reduce_job_result_ext);
        PopulateUserJobResult(resultExt->mutable_reducer_result());
    }
};

TAutoPtr<TUserJobIO> CreateSortedReduceJobIO(
    TJobIOConfigPtr ioConfig,
    NMetaState::TMasterDiscoveryConfigPtr mastersConfig,
    const NScheduler::NProto::TJobSpec& jobSpec)
{
    return new TSortedReduceJobIO(ioConfig, mastersConfig, jobSpec);
}

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
