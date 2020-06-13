#include "unversioned_row_batch.h"
#include "unversioned_row.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

bool IUnversionedRowBatch::IsEmpty() const
{
    return GetRowCount() == 0;
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

        virtual bool IsColumnar() const override
        {
            return false;
        }

        virtual TRange<TUnversionedRow> MaterializeRows() override
        {
            return Rows_;
        }

        virtual TRange<const TColumn*> MaterializeColumns() override
        {
            YT_ABORT();
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

        virtual bool IsColumnar() const override
        {
            return false;
        }

        virtual TRange<TUnversionedRow> MaterializeRows() override
        {
            return {};
        }

        virtual TRange<const TColumn*> MaterializeColumns() override
        {
            YT_ABORT();
        }
    };

    return New<TUnversionedRowBatch>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
