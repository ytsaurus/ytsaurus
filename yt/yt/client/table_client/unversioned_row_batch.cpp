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
    TRange<TUnversionedRow> rows,
    TIntrusivePtr<TRefCounted> holder)
{
    class TUnversionedRowBatch
        : public IUnversionedRowBatch
    {
    public:
        TUnversionedRowBatch(
            TRange<TUnversionedRow> rows,
            TIntrusivePtr<TRefCounted> holder)
            : Rows_(rows)
            , Holder_(std::move(holder))
        { }

        virtual int GetRowCount() const override
        {
            return static_cast<int>(Rows_.size());
        }

        virtual TRange<TUnversionedRow> MaterializeRows() override
        {
            return Rows_;
        }

        virtual bool IsColumnar() const override
        {
            return false;
        }

    private:
        const TRange<TUnversionedRow> Rows_;
        const TIntrusivePtr<TRefCounted> Holder_;
    };

    return New<TUnversionedRowBatch>(rows, std::move(holder));
}

IUnversionedRowBatchPtr CreateBatchFromUnversionedRows(
    std::vector<TUnversionedRow>&& rows,
    TIntrusivePtr<TRefCounted> holder)
{
    class TUnversionedRowBatch
        : public IUnversionedRowBatch
    {
    public:
        TUnversionedRowBatch(
            std::vector<TUnversionedRow>&& rows,
            TIntrusivePtr<TRefCounted> holder)
            : Rows_(std::move(rows))
            , Holder_(std::move(holder))
        { }

        virtual int GetRowCount() const override
        {
            return static_cast<int>(Rows_.size());
        }

        virtual TRange<TUnversionedRow> MaterializeRows() override
        {
            return MakeRange(Rows_);
        }

        virtual bool IsColumnar() const override
        {
            return false;
        }

    private:
        const std::vector<TUnversionedRow> Rows_;
        const TIntrusivePtr<TRefCounted> Holder_;
    };

    return New<TUnversionedRowBatch>(std::move(rows), std::move(holder));
}

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

        virtual TRange<TUnversionedRow> MaterializeRows() override
        {
            return {};
        }

        virtual bool IsColumnar() const override
        {
            return false;
        }
    };

    return New<TUnversionedRowBatch>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
