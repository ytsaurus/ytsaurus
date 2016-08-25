#pragma once

#include "public.h"

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct IReaderFactory
    : public virtual TRefCounted
{
    virtual i64 GetMemoryFootprint() const = 0;

    virtual IReaderBasePtr CreateReader() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IReaderFactory)

////////////////////////////////////////////////////////////////////////////////

IReaderFactoryPtr CreateReaderFactory(
    std::function<IReaderBasePtr()> factory, 
    i64 memoryFootprint);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT