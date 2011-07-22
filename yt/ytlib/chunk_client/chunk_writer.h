#pragma once

#include "../misc/common.h"
#include "../misc/ptr.h"

namespace NYT
{

///////////////////////////////////////////////////////////////////////////////

struct IChunkWriter
    : virtual public TRefCountedBase
{
    typedef TIntrusivePtr<IChunkWriter> TPtr;

    virtual void AddBlock(const TSharedRef& data) = 0;
    virtual void Close() = 0;
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
