#include "stdafx.h"
#include "private.h"
#include "job_detail.h"
#include "config.h"
#include "simple_sort_job.h"
#include "small_key.h"

#include <ytlib/misc/sync.h>
#include <ytlib/object_server/id.h>
#include <ytlib/meta_state/leader_channel.h>
#include <ytlib/chunk_client/client_block_cache.h>
#include <ytlib/chunk_server/public.h>
#include <ytlib/table_client/table_chunk_sequence_writer.h>
#include <ytlib/table_client/table_chunk_sequence_reader.h>
#include <ytlib/table_client/sync_writer.h>
#include <ytlib/ytree/lexer.h>

namespace NYT {
namespace NJobProxy {

using namespace NElection;
using namespace NTableClient;
using namespace NChunkClient;
using namespace NChunkServer;
using namespace NObjectServer;
using namespace NYTree;
using namespace NScheduler::NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = JobProxyLogger;
static NProfiling::TProfiler& Profiler = JobProxyProfiler;

////////////////////////////////////////////////////////////////////////////////

class TSimpleSortJob
    : public TJob
{
public:
    explicit TSimpleSortJob(IJobHost* host)
        : TJob(host)
    {
        const auto& jobSpec = Host->GetJobSpec();
        auto config = Host->GetConfig();

        YCHECK(jobSpec.input_specs_size() == 1);
        YCHECK(jobSpec.output_specs_size() == 1);

        auto masterChannel = CreateLeaderChannel(config->Masters);
        auto blockCache = CreateClientBlockCache(New<TClientBlockCacheConfig>());
        auto jobSpecExt = jobSpec.GetExtension(TSortJobSpecExt::sort_job_spec_ext);

        KeyColumns = FromProto<Stroka>(jobSpecExt.key_columns());

        TReaderOptions options;
        options.KeepBlocks = true;

        std::vector<NTableClient::NProto::TInputChunk> chunks(
            jobSpec.input_specs(0).chunks().begin(),
            jobSpec.input_specs(0).chunks().end());

        srand(time(NULL));
        std::random_shuffle(chunks.begin(), chunks.end());

        Reader = New<TTableChunkSequenceReader>(
            config->JobIO->ChunkSequenceReader, 
            masterChannel, 
            blockCache, 
            MoveRV(chunks),
            options);

        Writer = New<TTableChunkSequenceWriter>(
            config->JobIO->ChunkSequenceWriter,
            masterChannel,
            TTransactionId::FromProto(jobSpec.output_transaction_id()),
            TChunkListId::FromProto(jobSpec.output_specs(0).chunk_list_id()),
            ChannelsFromYson(TYsonString(jobSpec.output_specs(0).channels())),
            KeyColumns);
    }

    virtual NScheduler::NProto::TJobResult Run() OVERRIDE
    {
        PROFILE_TIMING ("/sort_time") {

            auto keyColumnCount = KeyColumns.size();

            yhash_map<TStringBuf, int> keyColumnToIndex;

            std::vector< std::pair<TStringBuf, TStringBuf> > valueBuffer;
            std::vector<TSmallKeyPart> keyBuffer;
            std::vector<ui32> valueIndexBuffer;
            std::vector<ui32> rowIndexBuffer;

            LOG_INFO("Somple sort job.");
            {
                for (int i = 0; i < KeyColumns.size(); ++i) {
                    TStringBuf name(~KeyColumns[i], KeyColumns[i].size());
                    keyColumnToIndex[name] = i;
                }

                Sync(~Reader, &TTableChunkSequenceReader::AsyncOpen);

                valueBuffer.reserve(Reader->GetValueCount());
                keyBuffer.reserve(Reader->GetRowCount() * keyColumnCount);
                valueIndexBuffer.reserve(Reader->GetRowCount() + 1);
                rowIndexBuffer.reserve(Reader->GetRowCount());

                // Add fake row.
                valueIndexBuffer.push_back(0);
            }

            PROFILE_TIMING_CHECKPOINT("init");

            LOG_INFO("Reading");
            {
                TLexer lexer;
                while (Reader->IsValid()) {
                    rowIndexBuffer.push_back(rowIndexBuffer.size());
                    YASSERT(rowIndexBuffer.back() <= std::numeric_limits<ui32>::max());

                    keyBuffer.resize(keyBuffer.size() + keyColumnCount);

                    FOREACH (const auto& pair, Reader->GetRow()) {
                        auto it = keyColumnToIndex.find(pair.first);
                        if (it != keyColumnToIndex.end()) {
                            auto& keyPart = keyBuffer[rowIndexBuffer.back() * keyColumnCount + it->second];
                            SetSmallKeyPart(keyPart, pair.second, lexer);
                        }
                        valueBuffer.push_back(pair);
                    }

                    valueIndexBuffer.push_back(valueBuffer.size());

                    if (!Reader->FetchNextItem()) {
                        Sync(~Reader, &TTableChunkSequenceReader::GetReadyEvent);
                    }
                }
            }
            PROFILE_TIMING_CHECKPOINT("read");

            LOG_INFO("Sorting");

            std::sort(
                rowIndexBuffer.begin(), 
                rowIndexBuffer.end(),
                [&] (ui32 lhs, ui32 rhs) -> bool {
                    for (int i = 0; i < keyColumnCount; ++i) {
                        auto res = CompareSmallKeyParts(
                            keyBuffer[lhs * keyColumnCount + i], 
                            keyBuffer[rhs * keyColumnCount + i]);

                        if (res < 0)
                            return true;
                        if (res > 0)
                            return false;
                    }

                    return false;
            }
            );

            PROFILE_TIMING_CHECKPOINT("sort");

            LOG_INFO("Writing");
            {
                auto writer = CreateSyncWriter(Writer);
                writer->Open();

                TRow row;
                TNonOwningKey key(keyColumnCount);
                for (size_t progressIndex = 0; progressIndex < rowIndexBuffer.size(); ++progressIndex) {
                    row.clear();
                    key.Clear();

                    auto rowIndex = rowIndexBuffer[progressIndex];
                    for (auto valueIndex = valueIndexBuffer[rowIndex];
                        valueIndex < valueIndexBuffer[rowIndex + 1]; 
                        ++valueIndex)
                    {
                        row.push_back(valueBuffer[valueIndex]);
                    }

                    for (int keyIndex = 0; keyIndex < keyColumnCount; ++keyIndex) {
                        auto& keyPart = keyBuffer[rowIndex * keyColumnCount + keyIndex];
                        SetKeyPart(&key, keyPart, keyIndex);
                    }

                    writer->WriteRowUnsafe(row, key);

                    if (progressIndex % 1000 == 0) {
                        Writer->SetProgress(double(progressIndex) / rowIndexBuffer.size());
                    }
                }

                writer->Close();
            }

            PROFILE_TIMING_CHECKPOINT("write");

            LOG_INFO("Finalizing");
            {
                TJobResult result;
                auto* resultExt = result.MutableExtension(TSortJobResultExt::sort_job_result_ext);
                ToProto(resultExt->mutable_chunks(), Writer->GetWrittenChunks());
                *result.mutable_error() = TError().ToProto();
                return result;
            }
        }
    }

private:
    TKeyColumns KeyColumns;
    TTableChunkSequenceReaderPtr Reader;
    TTableChunkSequenceWriterPtr Writer;

};

TAutoPtr<IJob> CreateSimpleSortJob(IJobHost* host)
{
    return new TSimpleSortJob(host);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
