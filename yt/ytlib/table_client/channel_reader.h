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
    void SetBlock(TSharedRef&& block);

    bool NextRow();
    bool NextColumn();

    TColumn GetColumn() const;
    TValue GetValue() const;

private:
    // TODO(sandello): This was stored as (const T) hence prohibiting 
    // copy assignment. What is the proper way to constify the Channel?
    TChannel Channel;

    TSharedRef CurrentBlock;

    std::vector<TMemoryInput> ColumnBuffers;

    int CurrentColumnIndex;
    TValue CurrentColumn;
    TValue CurrentValue;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

