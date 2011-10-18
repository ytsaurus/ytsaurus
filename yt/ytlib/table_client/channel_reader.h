#pragma once

#include "common.h"
#include "schema.h"
#include "value.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TChannelReader
{
public:
    TChannelReader(const TChannel& channel);
    void SetBlock(const TSharedRef& block);

    bool NextRow();
    bool NextColumn();

    TColumn GetColumn() const;
    TValue GetValue() const;

private:
    const TChannel Channel;

    TSharedRef CurrentBlock;

    yvector<TMemoryInput> ColumnBuffers;
    
    int CurrentColumnIndex;
    TValue CurrentColumn;
    TValue CurrentValue;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

