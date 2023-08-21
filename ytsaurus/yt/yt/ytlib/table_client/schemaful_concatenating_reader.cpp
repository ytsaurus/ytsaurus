#include "schemaful_concatencaing_reader.h"

#include <yt/yt/client/table_client/unversioned_reader.h>

namespace NYT::NTableClient {

using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

class TSchemafulConcatenatingReader
    : public ISchemafulUnversionedReader
{
public:
    explicit TSchemafulConcatenatingReader(
        std::vector<std::function<ISchemafulUnversionedReaderPtr()>> underlyingReaderFactories)
        : UnderlyingReaderFactories_(std::move(underlyingReaderFactories))
    { }

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        if (!CurrentReader_) {
            SwitchCurrentReader();
        }

        while (CurrentReader_) {
            if (auto batch = CurrentReader_->Read(options)) {
                return batch;
            }
            SwitchCurrentReader();
        }
        return nullptr;
    }

    TFuture<void> GetReadyEvent() const override
    {
        return CurrentReader_
            ? CurrentReader_->GetReadyEvent()
            : VoidFuture;
    }

    TDataStatistics GetDataStatistics() const override
    {
        TDataStatistics dataStatistics;
        for (const auto& reader : Readers_) {
            dataStatistics += reader->GetDataStatistics();
        }
        return dataStatistics;
    }

    NChunkClient::TCodecStatistics GetDecompressionStatistics() const override
    {
        NChunkClient::TCodecStatistics result;
        for (const auto& reader : Readers_) {
            result += reader->GetDecompressionStatistics();
        }
        return result;
    }

    bool IsFetchingCompleted() const override
    {
        for (const auto& reader : Readers_) {
            if (!reader->IsFetchingCompleted()) {
                return false;
            }
        }
        return true;
    }

    std::vector<NChunkClient::TChunkId> GetFailedChunkIds() const override
    {
        std::vector<NChunkClient::TChunkId> result;
        for (const auto& reader : Readers_) {
            auto failedChunkIds = reader->GetFailedChunkIds();
            result.insert(result.end(), failedChunkIds.begin(), failedChunkIds.end());
        }
        return result;
    }

private:
    const std::vector<std::function<ISchemafulUnversionedReaderPtr()>> UnderlyingReaderFactories_;

    int CurrentReaderIndex_ = -1;
    ISchemafulUnversionedReaderPtr CurrentReader_;
    std::vector<ISchemafulUnversionedReaderPtr> Readers_;

    void SwitchCurrentReader()
    {
        ++CurrentReaderIndex_;
        if (CurrentReaderIndex_ < std::ssize(UnderlyingReaderFactories_)) {
            CurrentReader_ = UnderlyingReaderFactories_[CurrentReaderIndex_]();
            Readers_.push_back(CurrentReader_);
        } else {
            CurrentReader_.Reset();
        }
    }
};

ISchemafulUnversionedReaderPtr CreateSchemafulConcatenatingReader(
    std::vector<std::function<ISchemafulUnversionedReaderPtr()>> underlyingReaderFactories)
{
    return New<TSchemafulConcatenatingReader>(std::move(underlyingReaderFactories));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
