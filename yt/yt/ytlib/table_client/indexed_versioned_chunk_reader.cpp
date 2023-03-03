#include "indexed_versioned_chunk_reader.h"
#include "chunk_index_read_controller.h"

#include <yt/yt/ytlib/chunk_client/chunk_fragment_reader.h>
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
        IChunkFragmentReaderPtr chunkFragmentReader)
        : Options_(std::move(options))
        , Controller_(std::move(controller))
        , ChunkFragmentReader_(std::move(chunkFragmentReader))
        , TraceContext_(CreateTraceContextFromCurrent("IndexedChunkReader"))
        , FinishGuard_(TraceContext_)
    { }

    TFuture<void> Open() override
    {
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
        dataStatistics.set_row_count(RowCount_);
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
    const IChunkFragmentReaderPtr ChunkFragmentReader_;

    const NTracing::TTraceContextPtr TraceContext_;
    const NTracing::TTraceContextFinishGuard FinishGuard_;

    TFuture<void> ReadyEvent_;

    int RowCount_ = 0;
    i64 DataWeight_ = 0;


    TFuture<void> DoRead() const
    {
        while (true) {
            if (Controller_->IsFinished()) {
                return VoidFuture;
            }

            auto requests = Controller_->GetFragmentRequests();
            YT_VERIFY(!requests.empty());

            auto responseFuture = ChunkFragmentReader_->ReadFragments(Options_, std::move(requests));
            if (auto responseOrError = responseFuture.TryGetUnique()) {
                if (!responseOrError->IsOK()) {
                    return MakeFuture(TError(*responseOrError));
                }

                Controller_->HandleFragmentsResponse(std::move(responseOrError->Value().Fragments));
            } else {
                return responseFuture.ApplyUnique(BIND([
                    =,
                    this,
                    this_ = MakeStrong(this)
                ] (IChunkFragmentReader::TReadFragmentsResponse&& response) {
                    Controller_->HandleFragmentsResponse(std::move(response.Fragments));

                    return DoRead();
                })
                    .AsyncVia(GetCurrentInvoker()));
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateIndexedVersionedChunkReader(
    TClientChunkReadOptions options,
    IChunkIndexReadControllerPtr controller,
    IChunkFragmentReaderPtr chunkFragmentReader)
{
    return New<TIndexedVersionedChunkReader>(
        std::move(options),
        std::move(controller),
        std::move(chunkFragmentReader));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
