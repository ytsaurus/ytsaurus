#include "stdafx.h"
#include "sorted_reduce_job_io.h"
#include "config.h"
#include "user_job_io_detail.h"
#include "job.h"

#include <ytlib/chunk_client/client_block_cache.h>
#include <ytlib/chunk_client/chunk_spec.pb.h>

#include <ytlib/chunk_client/old_multi_chunk_sequential_reader.h>
#include <ytlib/table_client/table_chunk_reader.h>
#include <ytlib/table_client/sync_reader.h>
#include <ytlib/table_client/sync_writer.h>
#include <ytlib/table_client/merging_reader.h>

namespace NYT {
namespace NJobProxy {

using namespace NScheduler;
using namespace NScheduler::NProto;
using namespace NTableClient;
using namespace NChunkClient;
using namespace NTransactionClient;
using namespace NChunkClient::NProto;
using namespace NJobTrackerClient::NProto;

////////////////////////////////////////////////////////////////////

class TSortedReduceJobIO
    : public TUserJobIOBase
{
public:
    explicit TSortedReduceJobIO(IJobHost* host)
        : TUserJobIOBase(host)
    { }

    virtual std::vector<ISyncReaderPtr> DoCreateReaders() override
    {
        std::vector<TTableChunkSequenceReaderPtr> readers;
        auto options = New<TChunkReaderOptions>();
        options->ReadKey = true;

        for (const auto& inputSpec : SchedulerJobSpec_.input_specs()) {
            // ToDo(psushin): validate that input chunks are sorted.
            std::vector<TChunkSpec> chunks(inputSpec.chunks().begin(), inputSpec.chunks().end());

            auto provider = New<TTableChunkReaderProvider>(
                chunks,
                JobIOConfig_->TableReader,
                Host_->GetUncompressedBlockCache(),
                options);

            auto reader = New<TTableChunkSequenceReader>(
                JobIOConfig_->TableReader,
                Host_->GetMasterChannel(),
                Host_->GetCompressedBlockCache(),
                Host_->GetNodeDirectory(),
                std::move(chunks),
                provider);

            readers.push_back(reader);
        }

        auto reader = CreateMergingReader(readers);
        return std::vector<ISyncReaderPtr>(1, reader);
    }

    virtual ISyncWriterUnsafePtr DoCreateWriter(
        TTableWriterOptionsPtr options,
        const TChunkListId& chunkListId,
        const TTransactionId& transactionId) override
    {
        return CreateTableWriter(options, chunkListId, transactionId);
    }

};

std::unique_ptr<IUserJobIO> CreateSortedReduceJobIO(IJobHost* host)
{
    return std::unique_ptr<IUserJobIO>(new TSortedReduceJobIO(host));
}

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
