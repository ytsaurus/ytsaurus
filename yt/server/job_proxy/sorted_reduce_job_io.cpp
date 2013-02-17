#include "stdafx.h"
#include "sorted_reduce_job_io.h"
#include "config.h"
#include "user_job_io.h"
#include "job.h"

#include <ytlib/chunk_client/client_block_cache.h>

#include <ytlib/table_client/multi_chunk_sequential_reader.h>
#include <ytlib/table_client/table_chunk_reader.h>
#include <ytlib/table_client/sync_reader.h>
#include <ytlib/table_client/table_producer.h>
#include <ytlib/table_client/merging_reader.h>

#include <ytlib/scheduler/config.h>

#include <ytlib/rpc/channel.h>

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
        IJobHost* host)
        : TUserJobIO(ioConfig, host)
    { }

    TAutoPtr<TTableProducer> CreateTableInput(
        int index,
        NYson::IYsonConsumer* consumer) override
    {
        YCHECK(index >= 0 && index < GetInputCount());

        std::vector<TTableChunkSequenceReaderPtr> readers;
        TReaderOptions options;
        options.ReadKey = true;

        auto provider = New<TTableChunkReaderProvider>(IOConfig->TableReader, options);

        const auto& jobSpec = Host->GetJobSpec();

        FOREACH (const auto& inputSpec, jobSpec.input_specs()) {
            // ToDo(psushin): validate that input chunks are sorted.
            std::vector<NTableClient::NProto::TInputChunk> chunks(
                inputSpec.chunks().begin(),
                inputSpec.chunks().end());

            auto reader = New<TTableChunkSequenceReader>(
                IOConfig->TableReader,
                Host->GetMasterChannel(),
                Host->GetBlockCache(),
                std::move(chunks),
                provider);

            readers.push_back(reader);
        }

        auto reader = CreateMergingReader(readers);

        // ToDo(psushin): init all inputs in constructor, get rid of this check.
        YCHECK(index == Inputs.size());
        Inputs.push_back(reader);

        reader->Open();

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
    IJobHost* host)
{
    return new TSortedReduceJobIO(ioConfig, host);
}

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
