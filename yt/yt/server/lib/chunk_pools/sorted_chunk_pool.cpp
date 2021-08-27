#include "sorted_chunk_pool.h"

#include <yt/yt/ytlib/table_client/chunk_slice_fetcher.h>

namespace NYT::NChunkPools {

using namespace NChunkClient;
using namespace NTableClient;

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

    void Persist(const TPersistenceContext& /*context*/) override
    {
        // This implementation is not persistable.
        Y_UNREACHABLE();
    }

private:
    TCallback<IChunkSliceFetcherPtr()> FactoryCallback_;
};

IChunkSliceFetcherFactoryPtr CreateCallbackChunkSliceFetcherFactory(TCallback<IChunkSliceFetcherPtr()> factoryCallback)
{
    return New<TCallbackChunkSliceFetcherFactory>(std::move(factoryCallback));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
