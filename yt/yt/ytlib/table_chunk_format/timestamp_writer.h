#pragma once

#include "public.h"

#include "column_writer.h"

namespace NYT::NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

struct ITimestampWriter
    : public IColumnWriterBase
{
    virtual void WriteTimestamps(TRange<NTableClient::TVersionedRow> rows) = 0;

    virtual NTableClient::TTimestamp GetMinTimestamp() const = 0;
    virtual NTableClient::TTimestamp GetMaxTimestamp() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<ITimestampWriter> CreateTimestampWriter(TDataBlockWriter* blockWriter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
