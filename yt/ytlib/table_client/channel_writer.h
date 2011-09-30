#pragma once
#include "value.h"

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

class TChannelBuffer
{
public:
    typedef TAutoPtr<TChannelBuffer> TPtr;

    void AddRow(const TTableRow& tableRow);
    i64 GetBufferSize() const;
    TSharedRef GetBlock();

private:
    TChannel Channel;
    yvector<TBlob> FixedColums;
    TBlob RangeColumns;

    yhash_map<THValue, >

    i64 CurrentSize;
    i64 FullSize;
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
