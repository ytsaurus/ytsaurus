#pragma once

#include "public.h"
#include <ytlib/chunk_client/schema.h>

#include <core/misc/ref.h>
#include <util/stream/mem.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TLegacyChannelReader
    : public TRefCounted
{
public:
    explicit TLegacyChannelReader(const NChunkClient::TChannel& channel);
    void SetBlock(const TSharedRef& block);

    bool NextRow();
    bool NextColumn();

    TStringBuf GetColumn() const;
    const TStringBuf& GetValue() const;

private:
    const NChunkClient::TChannel Channel;

    TSharedRef CurrentBlock;

    std::vector<TMemoryInput> ColumnBuffers;

    int CurrentColumnIndex = -1;
    TStringBuf CurrentColumn;
    TStringBuf CurrentValue;
    bool BlockFinished = true;

    TStringBuf LoadValue(TMemoryInput* input);
};

DEFINE_REFCOUNTED_TYPE(TLegacyChannelReader);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

