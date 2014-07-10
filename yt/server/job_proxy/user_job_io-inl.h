#ifndef USER_JOB_IO_INL_H_
#error "Direct inclusion of this file is not allowed, include user_job_io.h"
#endif
#undef USER_JOB_IO_INL_H_

#include "config.h"
#include "job.h"

#include <ytlib/table_client/table_chunk_reader.h>
#include <ytlib/table_client/sync_reader.h>
#include <ytlib/table_client/table_producer.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

template <template <typename> class TMultiChunkReader>
std::unique_ptr<NTableClient::TTableProducer> TUserJobIO::DoCreateTableInput(
    int index,
    NYson::IYsonConsumer* consumer)
{
    YCHECK(index >= 0 && index < GetInputCount());

    std::vector<NChunkClient::NProto::TChunkSpec> chunks;
    for (const auto& inputSpec : SchedulerJobSpecExt.input_specs()) {
        chunks.insert(
            chunks.end(),
            inputSpec.chunks().begin(),
            inputSpec.chunks().end());
    }

    LOG_DEBUG("Opening input %d with %d chunks",
        index,
        static_cast<int>(chunks.size()));

    typedef TMultiChunkReader<NTableClient::TTableChunkReader> TReader;

    auto provider = New<NTableClient::TTableChunkReaderProvider>(
        chunks,
        IOConfig->TableReader);
    auto reader = New<TReader>(
        IOConfig->TableReader,
        Host->GetMasterChannel(),
        Host->GetBlockCache(),
        Host->GetNodeDirectory(),
        std::move(chunks),
        provider);

    auto syncReader = NTableClient::CreateSyncReader(reader);

    {
        TGuard<TSpinLock> guard(SpinLock);

        // ToDo(psushin): init all inputs in constructor, get rid of this check.
        YCHECK(index == Inputs.size());
        Inputs.push_back(syncReader);
    }

    syncReader->Open();

    return std::unique_ptr<NTableClient::TTableProducer>(new NTableClient::TTableProducer(syncReader, consumer, IOConfig->TableReader->EnableTableIndex));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
