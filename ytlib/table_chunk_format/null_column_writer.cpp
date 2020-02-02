#include "null_column_writer.h"

#include "column_writer_detail.h"

#include <yt/client/table_client/versioned_row.h>

namespace NYT::NTableChunkFormat {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TUnversionedNullColumnWriter
    : public TColumnWriterBase
{
public:
    explicit TUnversionedNullColumnWriter(TDataBlockWriter* blockWriter)
        : TColumnWriterBase(blockWriter)
    { }

    virtual void WriteValues(TRange<TVersionedRow> rows) override
    {
        RowCount_ += rows.size();
    }

    virtual void WriteUnversionedValues(TRange<TUnversionedRow> rows) override
    {
        RowCount_ += rows.size();
    }

    virtual i32 GetCurrentSegmentSize() const override
    {
        return RowCount_ == PrevRowCount_ ? 0 : 1;
    }

    virtual void FinishCurrentSegment() override
    {
        if (RowCount_ > PrevRowCount_) {
            TSegmentInfo segmentInfo;
            segmentInfo.SegmentMeta.set_type(0);
            segmentInfo.SegmentMeta.set_version(0);
            segmentInfo.SegmentMeta.set_row_count(RowCount_ - PrevRowCount_);
            segmentInfo.Data.push_back(TSharedRef::MakeCopy<TSegmentWriterTag>(TRef::FromPod('\0')));
            TColumnWriterBase::DumpSegment(&segmentInfo);
            PrevRowCount_ = RowCount_;
        }
    }

private:
    i64 PrevRowCount_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IValueColumnWriter> CreateUnversionedNullColumnWriter(TDataBlockWriter* blockWriter)
{
    return std::make_unique<TUnversionedNullColumnWriter>(blockWriter);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
