#ifndef USER_JOB_IO_INL_H_
#error "Direct inclusion of this file is not allowed, include user_job_io.h"
#endif
#undef USER_JOB_IO_INL_H_

#include "config.h"

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
    NYTree::IYsonConsumer* consumer) const
{
    YCHECK(index >= 0 && index < GetInputCount());

    auto blockCache = NChunkClient::CreateClientBlockCache(New<NChunkClient::TClientBlockCacheConfig>());

    std::vector<NTableClient::NProto::TInputChunk> chunks;
    for (int i = 0; i < JobSpec.input_specs_size(); ++i) {
        chunks.insert(
            chunks.end(), 
            JobSpec.input_specs(i).chunks().begin(), 
            JobSpec.input_specs(0).chunks().end());
    }

    LOG_DEBUG("Opening input %d with %d chunks", 
        index, 
        static_cast<int>(chunks.size()));

    typedef TMultiChunkReader<NTableClient::TTableChunkReader> TReader;

    auto provider = New<NTableClient::TTableChunkReaderProvider>(IOConfig->TableReader);
    auto reader = New<TReader>(
        IOConfig->TableReader,
        MasterChannel,
        blockCache,
        MoveRV(chunks),
        provider);

    auto syncReader = NTableClient::CreateSyncReader(reader);
    syncReader->Open();

    return new NTableClient::TTableProducer(syncReader, consumer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT