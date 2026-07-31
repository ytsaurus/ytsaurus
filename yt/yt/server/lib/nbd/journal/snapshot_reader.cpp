#include "snapshot_reader.h"

#include <yt/yt/server/lib/nbd/journal/records/snapshot_block.record.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>

#include <yt/yt/ytlib/table_client/config.h>
#include <yt/yt/ytlib/table_client/hunks.h>
#include <yt/yt/ytlib/table_client/schemaful_reader_adapter.h>
#include <yt/yt/ytlib/table_client/schemaless_multi_chunk_reader.h>
#include <yt/yt/ytlib/table_client/table_read_spec.h>

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

namespace NYT::NNbd::NJournal {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NTableClient;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

std::vector<TSnapshotBlock> ReadJournalSnapshot(
    const NApi::NNative::IClientPtr& client,
    const TSnapshotLoadSpec& loadSpec,
    const NLogging::TLogger& Logger)
{
    YT_LOG_INFO("Reading snapshot block map");

    const auto& schema = NRecords::TSnapshotBlockDescriptor::Get()->GetSchema();

    // Read the payload column as a raw global hunk reference instead of fetching the referenced block:
    // DecodeHunks disables hunk decoding, so hunk values pass through with the EValueFlags::Hunk flag.
    auto readerOptions = New<NTableClient::TTableReaderOptions>();
    readerOptions->DecodeHunks = false;

    TClientChunkReadOptions chunkReadOptions{
        .WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::UserBatch),
    };

    auto reader = CreateSchemafulReaderAdapter(
        [&] (TNameTablePtr nameTable, const TColumnFilter& columnFilter) {
            return CreateAppropriateSchemalessMultiChunkReader(
                readerOptions,
                New<NTableClient::TTableReaderConfig>(),
                New<TChunkReaderHost>(client),
                loadSpec,
                chunkReadOptions,
                /*unordered*/ false,
                nameTable,
                columnFilter);
        },
        schema);

    std::vector<TSnapshotBlock> blocks;
    while (auto batch = ReadRowBatch(reader)) {
        for (auto row : batch->MaterializeRows()) {
            auto [blockIndex, payload] = FromUnversionedRow<int, TStringBuf>(row);

            auto hunkValue = ReadHunkValue(TRef(payload.data(), payload.size()));
            const auto* globalRef = std::get_if<TGlobalRefHunkValue>(&hunkValue);
            if (!globalRef) {
                THROW_ERROR_EXCEPTION("Journal snapshot payload is not a global hunk reference");
            }

            blocks.push_back({
                .Index = blockIndex,
                .Ref = TStoredBlockRef{
                    .ChunkId = globalRef->ChunkId,
                    .RecordIndex = globalRef->BlockIndex,
                    .RecordOffset = globalRef->BlockOffset,
                    .PayloadLength = globalRef->Length,
                },
            });
        }
    }

    YT_LOG_INFO("Snapshot block map read (BlockCount: %v)",
        blocks.size());

    return blocks;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NJournal
