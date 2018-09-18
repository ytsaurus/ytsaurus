#pragma once

#include "public.h"

#include <yt/client/table_chunk_format/proto/column_meta.pb.h>
#include <yt/ytlib/table_client/block_writer.h>

#include <yt/core/misc/ref.h>

namespace NYT {
namespace NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

class TDataBlockWriter
    : public TNonCopyable
{
public:
    void WriteSegment(TRange<TSharedRef> segment);

    void RegisterColumnWriter(IColumnWriterBase* streamWriter);

    NTableClient::TBlock DumpBlock(int blockIndex, i64 currentRowCount);

    i64 GetOffset() const;
    i32 GetCurrentSize() const;

private:
    i64 CurrentOffset_ = 0;
    std::vector<TSharedRef> Data_;
    std::vector<IColumnWriterBase*> ColumnWriters_;

    i64 LastRowCount_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableChunkFormat
} // namespace NYT
