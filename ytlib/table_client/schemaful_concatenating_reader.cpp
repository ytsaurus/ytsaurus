#include "schemaful_concatencaing_reader.h"
#include "schemaful_reader.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TSchemafulConcatenatingReader
    : public ISchemafulReader
{
public:
    explicit TSchemafulConcatenatingReader(
        std::vector<ISchemafulReaderPtr> underlyingReaders)
        : UnderlyingReaders_(std::move(underlyingReaders))
    { }

    virtual bool Read(std::vector<TUnversionedRow>* rows) override
    {
        while (CurrentReaderIndex_ < UnderlyingReaders_.size()) {
            if (UnderlyingReaders_[CurrentReaderIndex_]->Read(rows)) {
                return true;
            }
            ++CurrentReaderIndex_;
        }
        return false;
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        return CurrentReaderIndex_ < UnderlyingReaders_.size()
            ? UnderlyingReaders_[CurrentReaderIndex_]->GetReadyEvent()
            : VoidFuture;
    }

private:
    const std::vector<ISchemafulReaderPtr> UnderlyingReaders_;

    int CurrentReaderIndex_ = 0;

};

ISchemafulReaderPtr CreateSchemafulConcatencatingReader(
    std::vector<ISchemafulReaderPtr> underlyingReaders)
{
    return New<TSchemafulConcatenatingReader>(std::move(underlyingReaders));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
