#include "schemaful_concatencaing_reader.h"
#include "schemaful_reader.h"

namespace NYT {
namespace NTableClient {

using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

class TSchemafulConcatenatingReader
    : public ISchemafulReader
{
public:
    explicit TSchemafulConcatenatingReader(
        std::vector<std::function<ISchemafulReaderPtr()>> underlyingReaderFactories)
        : UnderlyingReaderFactories_(std::move(underlyingReaderFactories))
    { }

    virtual bool Read(std::vector<TUnversionedRow>* rows) override
    {
        auto resetCurrentReader = [&] () {
            ++CurrentReaderIndex_;
            if (CurrentReaderIndex_ < UnderlyingReaderFactories_.size()) {
                CurrentReader_ = UnderlyingReaderFactories_[CurrentReaderIndex_]();
                Readers_.push_back(CurrentReader_);
            } else {
                CurrentReader_.Reset();
            }
        };

        if (!CurrentReader_) {
            resetCurrentReader();
        }

        while (CurrentReader_) {
            if (CurrentReader_->Read(rows)) {
                return true;
            }

            resetCurrentReader();
        }
        return false;
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        return CurrentReader_
            ? CurrentReader_->GetReadyEvent()
            : VoidFuture;
    }

    virtual TDataStatistics GetDataStatistics() const override
    {
        TDataStatistics dataStatistics;
        for (const auto& reader : Readers_) {
            dataStatistics += reader->GetDataStatistics();
        }
        return dataStatistics;
    }

private:
    const std::vector<std::function<ISchemafulReaderPtr()>> UnderlyingReaderFactories_;

    int CurrentReaderIndex_ = -1;
    ISchemafulReaderPtr CurrentReader_;
    std::vector<ISchemafulReaderPtr> Readers_;

};

ISchemafulReaderPtr CreateSchemafulConcatenatingReader(
    std::vector<std::function<ISchemafulReaderPtr()>> underlyingReaderFactories)
{
    return New<TSchemafulConcatenatingReader>(std::move(underlyingReaderFactories));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
