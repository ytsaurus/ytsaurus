#include "chunk_reader_options.h"

#include <yt/yt/ytlib/table_client/hunks.h>
#include <yt/yt/ytlib/table_client/key_filter.h>

namespace NYT::NChunkClient {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

void TClientChunkReadOptions::ResetStatistics()
{
    ChunkReaderStatistics = New<TChunkReaderStatistics>();

    if (HunkChunkReaderStatistics) {
        HunkChunkReaderStatistics = HunkChunkReaderStatistics->CloneEmpty();
    }
}

void TClientChunkReadOptions::AddStatisticsFrom(const TClientChunkReadOptions& from) const
{
    ChunkReaderStatistics->AddFrom(from.ChunkReaderStatistics);

    if (HunkChunkReaderStatistics) {
        YT_VERIFY(from.HunkChunkReaderStatistics);
        HunkChunkReaderStatistics->AddFrom(from.HunkChunkReaderStatistics);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
