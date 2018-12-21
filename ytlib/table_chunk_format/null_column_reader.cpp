#include "null_column_reader.h"

#include "helpers.h"

namespace NYT::NTableChunkFormat {

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
        Y_UNREACHABLE();
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
        Y_UNREACHABLE();
    }

    virtual std::optional<int> GetNextBlockIndex() const override
    {
        return std::nullopt;
    }

    virtual std::pair<i64, i64> GetEqualRange(const TUnversionedValue& value, i64 lowerRowIndex, i64 upperRowIndex) override
    {
        if (value.Type == EValueType::Null) {
            return std::make_pair(lowerRowIndex, upperRowIndex);
        } else {
            // Any other value is greater than null.
            return std::make_pair(upperRowIndex, upperRowIndex);
        }
    }
    
    virtual void ReadValues(TMutableRange<NTableClient::TMutableVersionedRow> rows) override
    {
        DoReadValues(rows);
    }

    virtual void ReadValues(TMutableRange<NTableClient::TMutableUnversionedRow> rows) override
    {
        DoReadValues(rows);
    }

private:
    const int ColumnIndex_;
    const int ColumnId_;

    i64 RowIndex_ = 0;

    template <class TRow>
    void DoReadValues(TMutableRange<TRow> rows)
    {
        for (auto row : rows) {
            if (row) {
                GetUnversionedValue(row, ColumnIndex_) = MakeUnversionedSentinelValue(EValueType::Null, ColumnId_);
            }
        }
    }

};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IUnversionedColumnReader> CreateUnversionedNullColumnReader(int columnIndex, int columnId)
{
    return std::make_unique<TUnversionedNullColumnReader>(columnIndex, columnId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
