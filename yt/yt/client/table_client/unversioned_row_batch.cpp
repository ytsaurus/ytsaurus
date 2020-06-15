#include "unversioned_row_batch.h"
#include "unversioned_row.h"

#include <atomic>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

bool IUnversionedRowBatch::IsEmpty() const
{
    return GetRowCount() == 0;
}

IUnversionedColumnarRowBatchPtr IUnversionedRowBatch::TryAsColumnar()
{
    return dynamic_cast<IUnversionedColumnarRowBatch*>(this);
}

////////////////////////////////////////////////////////////////////////////////

IUnversionedColumnarRowBatch::TDictionaryId IUnversionedColumnarRowBatch::GenerateDictionaryId()
{
    static std::atomic<TDictionaryId> CurrentId;
    return ++CurrentId;
}

////////////////////////////////////////////////////////////////////////////////

IUnversionedRowBatchPtr CreateBatchFromUnversionedRows(
    TSharedRange<TUnversionedRow> rows)
{
    class TUnversionedRowBatch
        : public IUnversionedRowBatch
    {
    public:
        explicit TUnversionedRowBatch(TSharedRange<TUnversionedRow> rows)
            : Rows_(std::move(rows))
        { }

        virtual int GetRowCount() const override
        {
            return static_cast<int>(Rows_.size());
        }

        virtual TSharedRange<TUnversionedRow> MaterializeRows() override
        {
            return Rows_;
        }

    private:
        const TSharedRange<TUnversionedRow> Rows_;
    };

    return New<TUnversionedRowBatch>(rows);
}

////////////////////////////////////////////////////////////////////////////////

IUnversionedRowBatchPtr CreateEmptyUnversionedRowBatch()
{
    class TUnversionedRowBatch
        : public IUnversionedRowBatch
    {
    public:
        virtual int GetRowCount() const override
        {
            return 0;
        }

        virtual TSharedRange<TUnversionedRow> MaterializeRows() override
        {
            return {};
        }

    };

    return New<TUnversionedRowBatch>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
