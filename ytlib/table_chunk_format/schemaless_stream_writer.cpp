#include "schemaless_stream_writer.h"

#include "helpers.h"

#include <yt/ytlib/table_chunk_format/stream_meta.pb.h>

#include <yt/ytlib/table_client/unversioned_row.h>

namespace NYT {
namespace NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

using namespace NTableClient;
using namespace NProto;

const int UnversionedSegmentType = 0;

////////////////////////////////////////////////////////////////////////////////

class TSchemalessStreamWriter
    : public IValueStreamWriter
{
public:
    TSchemalessStreamWriter(const std::vector<int>& ignoreIds, TDataBlockWriter* blockWriter)
        : IgnoreIds_(ignoreIds.begin(), ignoreIds.end())
        , BlockWriter_(blockWriter)
    {
        BlockWriter_->RegisterStreamWriter(this);
        Reset();
    }

    virtual void WriteValues(const TRange<TVersionedRow>& rows) override
    {
        DoWriteValues(rows);
    }

    virtual void WriteValues(const TRange<TUnversionedRow>& rows) override
    {
        DoWriteValues(rows);
    }

    virtual void FinishBlock(int blockIndex) override
    {
        Cout << "Finish schemaless writer" << Endl;
        TSegmentMeta segmentMeta;
        segmentMeta.set_version(0);
        segmentMeta.set_type(UnversionedSegmentType);
        segmentMeta.set_row_count(SegmentRowCount_);
        segmentMeta.set_block_index(blockIndex);
        segmentMeta.set_offset(BlockWriter_->GetCurrentSize());
        segmentMeta.set_chunk_row_count(RowCount_);
        segmentMeta.set_size(GetCurrentSegmentSize());

        StreamMeta_.add_segments()->Swap(&segmentMeta);

        auto data = OffsetBuffer_->Flush();
        auto valueData = ValueBuffer_->Flush();
        data.insert(data.end(), valueData.begin(), valueData.end());

        BlockWriter_->WriteSegment(data);

        Reset();
    }

    virtual i32 GetCurrentSegmentSize() const override
    {
        return OffsetBuffer_->GetCurrentSize() + ValueBuffer_->GetCurrentSize();
    }

private:
    std::unique_ptr<TPreallocatingBuffer> OffsetBuffer_;
    std::unique_ptr<TPreallocatingBuffer> ValueBuffer_;

    yhash_set<int> IgnoreIds_;

    TStreamMeta StreamMeta_;

    i64 RowCount_ = 0;
    i64 SegmentRowCount_ = 0;

    TDataBlockWriter* BlockWriter_;

    void Reset()
    {
        OffsetBuffer_ = std::make_unique<TPreallocatingBuffer>();
        ValueBuffer_ = std::make_unique<TPreallocatingBuffer>();
        SegmentRowCount_ = 0;
    }

    template <class TRow>
    void DoWriteValues(const TRange<TRow>& rows)
    {
        OffsetBuffer_->EnsureCapacity(rows.Size() * sizeof(ui32));
        ui32 cumulativeSize = 0;
        for (auto row : rows) {
            for (int i = 0; i < GetUnversionedValueCount(row); ++i) {
                const auto& value = GetUnversionedValue(row, i);
                if (IgnoreIds_.find(value.Id) == IgnoreIds_.end()) {
                    cumulativeSize += GetByteSize(value);
                }
            }
        }

        ValueBuffer_->EnsureCapacity(cumulativeSize);
        for (auto row : rows) {
            *reinterpret_cast<ui32*>(OffsetBuffer_->GetCurrentPointer()) = ValueBuffer_->GetCurrentSize();
            OffsetBuffer_->Advance(sizeof(ui32));

            for (int i = 0; i < GetUnversionedValueCount(row); ++i) {
                const auto& value = GetUnversionedValue(row, i);
                if (IgnoreIds_.find(value.Id) == IgnoreIds_.end()) {
                    //Cout << "Writing value " << ToString(value) << Endl; 
                    auto size = WriteValue(ValueBuffer_->GetCurrentPointer(), value);
                    ValueBuffer_->Advance(size);
                }
            }
        }

        RowCount_ += rows.Size();
        SegmentRowCount_ += rows.Size();
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IValueStreamWriter> CreateSchemalessStreamWriter(
    const std::vector<int>& ignoreIds,
    TDataBlockWriter* dataBlockWriter)
{
    return std::make_unique<TSchemalessStreamWriter>(ignoreIds, dataBlockWriter);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableChunkFormat
} // namespace NYT
