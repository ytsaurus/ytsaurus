#pragma once

#include "public.h"

#include <yt/yt_proto/yt/client/table_chunk_format/proto/column_meta.pb.h>
#include <yt/yt/ytlib/table_client/block.h>

#include <yt/yt/core/misc/property.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

class TDataBlockWriter
    : public TNonCopyable
{
public:
    DEFINE_BYVAL_RW_PROPERTY(std::optional<int>, GroupIndex);

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

} // namespace NYT::NTableChunkFormat
