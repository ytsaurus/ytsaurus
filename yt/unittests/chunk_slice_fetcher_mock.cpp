#include "chunk_slice_fetcher_mock.h"

namespace NYT {
namespace NChunkPools {

using namespace NTableClient;
using namespace NPhoenix;

////////////////////////////////////////////////////////////////////////////////

TMockChunkSliceFetcherFactory::TMockChunkSliceFetcherFactory(std::vector<TStrictMockChunkSliceFetcherPtr>* fetchers)
    : Fetchers_(fetchers)
{ }

IChunkSliceFetcherPtr TMockChunkSliceFetcherFactory::CreateChunkSliceFetcher()
{
    return Fetchers_->at(CurrentIndex_++);
}

void TMockChunkSliceFetcherFactory::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    if (context.IsSave()) {
        intptr_t fetchersAddress = reinterpret_cast<intptr_t>(Fetchers_);
        Persist(context, fetchersAddress);
    } else {
        intptr_t fetchersAddress;
        Persist(context, fetchersAddress);
        Fetchers_ = reinterpret_cast<std::vector<TStrictMockChunkSliceFetcherPtr>*>(fetchersAddress);
    }
    Persist(context, CurrentIndex_);
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_DYNAMIC_PHOENIX_TYPE(TMockChunkSliceFetcherFactory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkPools
} // namespace NYT

