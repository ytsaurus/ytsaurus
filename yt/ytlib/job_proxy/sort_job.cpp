#include "stdafx.h"
#include "private.h"
#include "config.h"
#include "sort_job.h"

#include <ytlib/misc/sync.h>
#include <ytlib/object_server/id.h>
#include <ytlib/election/leader_channel.h>
#include <ytlib/chunk_client/client_block_cache.h>
#include <ytlib/chunk_server/public.h>
#include <ytlib/table_client/table_chunk_sequence_writer.h>
#include <ytlib/table_client/chunk_sequence_reader.h>
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

struct TSmallKeyPart
{
    EKeyPartType Type;
    ui32 Length;

    union {
        i64 Int;
        double Double;
        const char* Str;
    } Value;

    TStringBuf GetString() const
    {
        return TStringBuf(Value.Str, Value.Str + Length);
    }

    TSmallKeyPart() 
        : Type(EKeyPartType::Null)
    { }
};

void SetSmallKeyPart(TSmallKeyPart& keyPart, const TStringBuf& yson, TLexer& lexer)
{
    lexer.Reset();
    YVERIFY(lexer.Read(yson) > 0);
    YASSERT(lexer.GetState() == NYTree::TLexer::EState::Terminal);

    const auto& token = lexer.GetToken();
    switch (token.GetType()) {
        case ETokenType::Integer:
            keyPart.Type = EKeyPartType::Integer;
            keyPart.Value.Int = token.GetIntegerValue();
            break;

        case NYTree::ETokenType::Double:
            keyPart.Type = EKeyPartType::Double;
            keyPart.Value.Double = token.GetDoubleValue();
            break;

        case ETokenType::String: {
            keyPart.Type = EKeyPartType::String;
            auto& value = token.GetStringValue();
            keyPart.Value.Str = ~value;
            keyPart.Length = static_cast<ui32>(value.size());
            break;
        }

        default:
            keyPart.Type = EKeyPartType::Composite;
            break;
    }
}

int CompareSmallKeyParts(const TSmallKeyPart& lhs, const TSmallKeyPart& rhs)
{
    if (lhs.Type != rhs.Type) {
        return static_cast<int>(lhs.Type) - static_cast<int>(rhs.Type);
    }

    switch (lhs.Type) {
        case EKeyPartType::Integer:
            if (lhs.Value.Int > rhs.Value.Int)
                return 1;
            if (lhs.Value.Int < rhs.Value.Int)
                return -1;
            return 0;

        case EKeyPartType::Double:
            if (lhs.Value.Double > rhs.Value.Double)
                return 1;
            if (lhs.Value.Double < rhs.Value.Double)
                return -1;
            return 0;

        case EKeyPartType::String:
            return lhs.GetString().compare(rhs.GetString());

        case EKeyPartType::Composite:
        case EKeyPartType::Null:
            return 0;
    }

    YUNREACHABLE();
}

////////////////////////////////////////////////////////////////////////////////

TSortJob::TSortJob(
    TJobProxyConfigPtr proxyConfig,
    const TJobSpec& jobSpec)
{
    YCHECK(jobSpec.input_specs_size() == 1);
    YCHECK(jobSpec.output_specs_size() == 1);

    auto masterChannel = CreateLeaderChannel(proxyConfig->Masters);
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

    Reader = New<TChunkSequenceReader>(
        proxyConfig->JobIO->ChunkSequenceReader, 
        masterChannel, 
        blockCache, 
        chunks,
        jobSpecExt.partition_tag(),
        options);

    Writer = New<TTableChunkSequenceWriter>(
        proxyConfig->JobIO->ChunkSequenceWriter,
        masterChannel,
        TTransactionId::FromProto(jobSpec.output_transaction_id()),
        TChunkListId::FromProto(jobSpec.output_specs(0).chunk_list_id()),
        ChannelsFromYson(TYsonString(jobSpec.output_specs(0).channels())),
        KeyColumns);
}

TJobResult TSortJob::Run()
{
    PROFILE_TIMING ("/sort_time") {

        auto keyColumnCount = KeyColumns.size();

        yhash_map<TStringBuf, int> keyColumnToIndex;

        std::vector< std::pair<TStringBuf, TStringBuf> > valueBuffer;
        std::vector<TSmallKeyPart> keyBuffer;
        std::vector<ui32> valueIndexBuffer;
        std::vector<ui32> rowIndexBuffer;

        LOG_INFO("Initializing");
        {
            for (int i = 0; i < KeyColumns.size(); ++i) {
                TStringBuf name(~KeyColumns[i], KeyColumns[i].size());
                keyColumnToIndex[name] = i;
            }

            Sync(~Reader, &TChunkSequenceReader::AsyncOpen);

            // TODO(babenko): fix reserve below once the reader can provide per-partition statistics

            //valueBuffer.reserve(Reader->GetValueCount());
            valueBuffer.reserve(1000000);

            //rowBuffer.reserve(Reader->GetRowCount());
            keyBuffer.reserve(1000000);

            //indexBuffer.reserve(Reader->GetRowCount());
            valueIndexBuffer.reserve(1000000);
            rowIndexBuffer.reserve(1000000);

            // Add fake row.
            valueIndexBuffer.push_back(0);
        }

        PROFILE_TIMING_CHECKPOINT("init");

        LOG_INFO("Reading");
        {
            TLexer lexer;
            while (Reader->IsValid()) {
                // Avoid constructing row on stack and then copying it into the buffer.
                // TODO(babenko): consider using emplace_back
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

                Sync(~Reader, &TChunkSequenceReader::AsyncNextRow);
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
            Sync(~Writer, &TTableChunkSequenceWriter::AsyncOpen);

            TRow row;
            TNonOwningKey key;
            for (size_t progressIndex = 0; progressIndex < rowIndexBuffer.size(); ++progressIndex) {
                row.clear();
                key.Reset(keyColumnCount);

                auto rowIndex = rowIndexBuffer[progressIndex];
                for (auto valueIndex = valueIndexBuffer[rowIndex];
                     valueIndex < valueIndexBuffer[rowIndex + 1]; 
                     ++valueIndex)
                {
                    row.push_back(valueBuffer[valueIndex]);
                }

                for (int keyIndex = 0; keyIndex < keyColumnCount; ++keyIndex) {
                    auto& keyPart = keyBuffer[rowIndex * keyColumnCount + keyIndex];
                    switch (keyPart.Type) {
                        case EKeyPartType::Integer:
                            key.SetValue(keyIndex, keyPart.Value.Int);
                            break;

                        case EKeyPartType::Double:
                            key.SetValue(keyIndex, keyPart.Value.Double);
                            break;

                        case EKeyPartType::String:
                            key.SetValue(keyIndex, keyPart.GetString());
                            break;

                        case EKeyPartType::Composite:
                            key.SetComposite(keyIndex);
                            break;

                        default:
                            // Do nothing.
                            break;
                    }
                }

                while (!Writer->TryWriteRow(row, key)) {
                    Sync(~Writer, &TTableChunkSequenceWriter::GetReadyEvent);
                }

                if (progressIndex % 1000 == 0) {
                    Writer->SetProgress(double(progressIndex) / rowIndexBuffer.size());
                }
            }

            Sync(~Writer, &TTableChunkSequenceWriter::AsyncClose);
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
