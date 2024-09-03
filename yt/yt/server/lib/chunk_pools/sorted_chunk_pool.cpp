#include "sorted_chunk_pool.h"

#include <yt/yt/ytlib/table_client/chunk_slice_fetcher.h>

namespace NYT::NChunkPools {

using namespace NChunkClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

void IChunkSliceFetcherFactory::RegisterMetadata(auto&& /*registrar*/)
{ }

PHOENIX_DEFINE_TYPE(IChunkSliceFetcherFactory);

////////////////////////////////////////////////////////////////////////////////

class TCallbackChunkSliceFetcherFactory
    : public IChunkSliceFetcherFactory
{
public:
    TCallbackChunkSliceFetcherFactory(TCallback<IChunkSliceFetcherPtr()> factoryCallback)
        : FactoryCallback_(std::move(factoryCallback))
    { }

    IChunkSliceFetcherPtr CreateChunkSliceFetcher() override
    {
        return FactoryCallback_();
    }

private:
    TCallback<IChunkSliceFetcherPtr()> FactoryCallback_;

    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TCallbackChunkSliceFetcherFactory, 0x994c8d2d);
};

void TCallbackChunkSliceFetcherFactory::RegisterMetadata(auto&& /*registrar*/)
{
    // This implementation is not persistable.
}

PHOENIX_DEFINE_TYPE(TCallbackChunkSliceFetcherFactory);

IChunkSliceFetcherFactoryPtr CreateCallbackChunkSliceFetcherFactory(TCallback<IChunkSliceFetcherPtr()> factoryCallback)
{
    return New<TCallbackChunkSliceFetcherFactory>(std::move(factoryCallback));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
