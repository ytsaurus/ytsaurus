#ifndef USER_JOB_IO_INL_H_
#error "Direct inclusion of this file is not allowed, include user_job_io.h"
#endif
#undef USER_JOB_IO_INL_H_

#include "config.h"
#include "job.h"

#include <ytlib/chunk_client/client_block_cache.h>
#include <ytlib/table_client/table_chunk_reader.h>

#include <ytlib/table_client/sync_reader.h>
#include <ytlib/table_client/table_producer.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

template <template <typename> class TMultiChunkReader>
TAutoPtr<NTableClient::TTableProducer> TUserJobIO::DoCreateTableInput(
    int index,
    NYson::IYsonConsumer* consumer)
{
    YCHECK(index >= 0 && index < GetInputCount());

    auto blockCache = NChunkClient::CreateClientBlockCache(New<NChunkClient::TClientBlockCacheConfig>());

    const auto& jobSpec = Host->GetJobSpec();

    std::vector<NChunkClient::NProto::TInputChunk> chunks;
    for (int i = 0; i < jobSpec.input_specs_size(); ++i) {
        chunks.insert(
            chunks.end(),
            jobSpec.input_specs(i).chunks().begin(),
            jobSpec.input_specs(i).chunks().end());
    }

    LOG_DEBUG("Opening input %d with %d chunks",
        index,
        static_cast<int>(chunks.size()));

    typedef TMultiChunkReader<NTableClient::TTableChunkReader> TReader;

    auto provider = New<NTableClient::TTableChunkReaderProvider>(IOConfig->TableReader);
    auto reader = New<TReader>(
        IOConfig->TableReader,
        Host->GetMasterChannel(),
        blockCache,
        std::move(chunks),
        provider);

    auto syncReader = NTableClient::CreateSyncReader(reader);

    // ToDo(psushin): init all inputs in constructor, get rid of this check.
    YCHECK(index == Inputs.size());
    Inputs.push_back(syncReader);

    syncReader->Open();

    return new NTableClient::TTableProducer(syncReader, consumer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT