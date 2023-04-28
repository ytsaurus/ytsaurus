#include "indexed_versioned_chunk_reader.h"
#include "chunk_index_read_controller.h"

#include <yt/yt/ytlib/chunk_client/chunk_fragment_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>

#include <yt/yt/client/table_client/versioned_reader.h>

namespace NYT::NTableClient {

using namespace NChunkClient;
using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

class TIndexedVersionedChunkReader
    : public IVersionedReader
{
public:
    TIndexedVersionedChunkReader(
        TClientChunkReadOptions options,
        IChunkIndexReadControllerPtr controller,
        IChunkReaderPtr chunkReader,
        IChunkFragmentReaderPtr chunkFragmentReader)
        : Options_(std::move(options))
        , Controller_(std::move(controller))
        , ChunkReader_(std::move(chunkReader))
        , ChunkFragmentReader_(std::move(chunkFragmentReader))
        , TraceContext_(CreateTraceContextFromCurrent("IndexedChunkReader"))
        , FinishGuard_(TraceContext_)
    { }

    TFuture<void> Open() override
    {
        YT_VERIFY(!std::exchange(Opened_, true));

        {
            TCurrentTraceContextGuard guard(TraceContext_);
            ReadyEvent_ = DoRead();
        }

        return ReadyEvent_;
    }

    IVersionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        TCurrentTraceContextGuard guard(TraceContext_);

        YT_VERIFY(options.MaxRowsPerRead > 0);

        if (!ReadyEvent_.IsSet() || !ReadyEvent_.Get().IsOK()) {
            return CreateEmptyVersionedRowBatch();
        }

        YT_VERIFY(Controller_->IsFinished());

        const auto& result = Controller_->GetResult();
        auto rowsLeft = std::ssize(result) - RowCount_;
        if (rowsLeft == 0) {
            return nullptr;
        }

        std::vector<TVersionedRow> rows;
        rows.reserve(std::min(rowsLeft, options.MaxRowsPerRead));
        while (rows.size() < rows.capacity()) {
            rows.push_back(result[RowCount_++]);
            ExistingRowCount_ += static_cast<bool>(rows.back());
            DataWeight_ += GetDataWeight(rows.back());
        }

        return CreateBatchFromVersionedRows(MakeSharedRange(std::move(rows), MakeStrong(this)));
    }

    TFuture<void> GetReadyEvent() const override
    {
        return ReadyEvent_;
    }

    NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        NChunkClient::NProto::TDataStatistics dataStatistics;
        dataStatistics.set_chunk_count(1);
        dataStatistics.set_row_count(ExistingRowCount_);
        dataStatistics.set_data_weight(DataWeight_);
        return dataStatistics;
    }

    TCodecStatistics GetDecompressionStatistics() const override
    {
        return {};
    }

    bool IsFetchingCompleted() const override
    {
        YT_ABORT();
    }

    std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return {};
    }

private:
    const TClientChunkReadOptions Options_;
    const IChunkIndexReadControllerPtr Controller_;
    const IChunkReaderPtr ChunkReader_;
    const IChunkFragmentReaderPtr ChunkFragmentReader_;

    const NTracing::TTraceContextPtr TraceContext_;
    const NTracing::TTraceContextFinishGuard FinishGuard_;

    bool Opened_ = false;
    TFuture<void> ReadyEvent_;

    int RowCount_ = 0;
    int ExistingRowCount_ = 0;
    i64 DataWeight_ = 0;


    TFuture<void> DoRead() const
    {
        while (true) {
            if (Controller_->IsFinished()) {
                return VoidFuture;
            }

            auto request = Controller_->GetReadRequest();
            YT_VERIFY(!request.FragmentSubrequests.empty() || !request.SystemBlockIndexes.empty());

            std::vector<TFuture<void>> readFutures;

            TFuture<IChunkFragmentReader::TReadFragmentsResponse> fragmentsFuture;
            if (!request.FragmentSubrequests.empty()) {
                fragmentsFuture = ChunkFragmentReader_->ReadFragments(
                    Options_,
                    std::move(request.FragmentSubrequests));
                readFutures.push_back(fragmentsFuture.AsVoid());
            }

            TFuture<std::vector<TBlock>> blocksFuture;
            if (!request.SystemBlockIndexes.empty()) {
                blocksFuture = ChunkReader_->ReadBlocks(
                    IChunkReader::TReadBlocksOptions{
                        .ClientOptions = Options_,
                    },
                    request.SystemBlockIndexes);
                readFutures.push_back(blocksFuture.AsVoid());
            }

            auto future = AllSucceeded(std::move(readFutures));
            if (auto error = future.TryGet()) {
                if (!error->IsOK()) {
                    return MakeFuture(*error);
                }

                OnDataRead(fragmentsFuture, blocksFuture);
            } else {
                return future.Apply(BIND([
                    =,
                    this,
                    this_ = MakeStrong(this),
                    fragmentsFuture = std::move(fragmentsFuture),
                    blocksFuture = std::move(blocksFuture)
                ] {
                    OnDataRead(fragmentsFuture, blocksFuture);
                    return DoRead();
                })
                    .AsyncVia(GetCurrentInvoker()));
            }
        }
    }

    void OnDataRead(
        const TFuture<IChunkFragmentReader::TReadFragmentsResponse>& fragmentsFuture,
        const TFuture<std::vector<TBlock>>& blocksFuture) const
    {
        IChunkIndexReadController::TReadResponse response;

        if (fragmentsFuture) {
            YT_VERIFY(fragmentsFuture.IsSet() && fragmentsFuture.Get().IsOK());
            response.Fragments = std::move(fragmentsFuture.GetUnique().Value().Fragments);
        }

        if (blocksFuture) {
            YT_VERIFY(blocksFuture.IsSet() && blocksFuture.Get().IsOK());
            response.SystemBlocks = std::move(blocksFuture.GetUnique().Value());
        }

        Controller_->HandleReadResponse(std::move(response));
    }
};

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateIndexedVersionedChunkReader(
    TClientChunkReadOptions options,
    IChunkIndexReadControllerPtr controller,
    IChunkReaderPtr chunkReader,
    IChunkFragmentReaderPtr chunkFragmentReader)
{
    return New<TIndexedVersionedChunkReader>(
        std::move(options),
        std::move(controller),
        std::move(chunkReader),
        std::move(chunkFragmentReader));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
