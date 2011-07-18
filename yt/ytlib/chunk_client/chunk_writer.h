#pragma once

#include "../misc/common.h"
#include "../misc/ptr.h"

namespace NYT
{

///////////////////////////////////////////////////////////////////////////////

struct IChunkWriter
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<IChunkWriter> TPtr;

    virtual void AddBlock(TBlob *buffer) = 0;
    virtual void Close() = 0;
};

///////////////////////////////////////////////////////////////////////////////

}
