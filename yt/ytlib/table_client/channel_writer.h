#pragma once

#include "../misc/common.h"
#include "../misc/ptr.h"
#include "value.h"
#include "schema.h"

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

class TChannelWriter
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TChannelWriter> TPtr;

    TChannelWriter(const TChannel& channel);
    void AddRow(const TTableRow& tableRow);
    size_t GetCurrentSize() const;
    bool HasData() const;
    TSharedRef FlushBlock();

private:
    size_t EmptySize() const;
    void AppendSize(i32 size, TBlob* data);
    void AppendValue(const TValue& value, TBlob* data);

    TChannel Channel;

    //! Current buffers for fixed columns
    yvector<TBlob> FixedColumns;

    //! Current buffer for range columns
    TBlob RangeColumns;

    //! Mapping from fixed column names of the #Channel to their indexes in #FixedColumns
    yhash_map<TValue, int> ColumnIndexes;

    //! Overall size of current buffers
    i64 CurrentSize;
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
