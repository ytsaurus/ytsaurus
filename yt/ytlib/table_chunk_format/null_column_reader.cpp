#include "null_column_reader.h"

namespace NYT {
namespace NTableChunkFormat {

using namespace NProto;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TUnversionedNullColumnReader
    : public IUnversionedColumnReader
{
public:
    TUnversionedNullColumnReader(int columnIndex, int id)
        : ColumnIndex_(columnIndex)
        , ColumnId_(id)
    { }

    virtual void ResetBlock(TSharedRef block, int blockIndex) override
    {
        YUNREACHABLE();
    }

    virtual void SkipToRowIndex(i64 rowIndex) override
    {
        RowIndex_ = rowIndex;
    }

    virtual i64 GetCurrentRowIndex() const override
    {
        return RowIndex_;
    }

    virtual i64 GetBlockUpperRowIndex() const override
    {
        return std::numeric_limits<i64>::max();
    }

    virtual i64 GetReadyUpperRowIndex() const override
    {
        return GetBlockUpperRowIndex();
    }

    virtual int GetCurrentBlockIndex() const override
    {
        YUNREACHABLE();
    }

    virtual TNullable<int> GetNextBlockIndex() const override
    {
        return Null;
    }

    virtual std::pair<i64, i64> GetEqualRange(const TUnversionedValue& value, i64 lowerRowIndex, i64 upperRowIndex) override
    {
        if (value.Type == EValueType::Null) {
            return std::make_pair(lowerRowIndex, upperRowIndex);
        } else {
            return std::make_pair(lowerRowIndex, lowerRowIndex);
        }
    }
    
    virtual void ReadValues(TMutableRange<NTableClient::TMutableVersionedRow> rows) override
    {
        for (auto row : rows) {
            if (row) {
                row.BeginKeys()[ColumnIndex_] = MakeUnversionedSentinelValue(EValueType::Null, ColumnId_);
            }
        }
    }

private:
    const int ColumnIndex_;
    const int ColumnId_;

    i64 RowIndex_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IUnversionedColumnReader> CreateUnversionedNullColumnReader(int columnIndex, int columnId)
{
    return std::make_unique<TUnversionedNullColumnReader>(columnIndex, columnId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableChunkFormat
} // namespace NYT
