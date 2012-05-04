#pragma once

#include "public.h"
#include "schema.h"

#include <ytlib/misc/ref.h>
#include <util/stream/mem.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TChannelReader
    : public virtual TRefCounted
{
public:
    explicit TChannelReader(const TChannel& channel);
    void SetBlock(const TSharedRef& block);

    bool NextRow();
    bool NextColumn();

    TStringBuf GetColumn() const;
    TStringBuf GetValue() const;

private:
    const TChannel Channel;

    TSharedRef CurrentBlock;

    std::vector<TMemoryInput> ColumnBuffers;

    int CurrentColumnIndex;
    TStringBuf CurrentColumn;
    TStringBuf CurrentValue;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

