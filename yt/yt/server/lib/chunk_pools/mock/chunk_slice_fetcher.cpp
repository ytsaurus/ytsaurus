#include "chunk_slice_fetcher.h"

namespace NYT::NChunkPools {

using namespace NTableClient;
using namespace NPhoenix;

////////////////////////////////////////////////////////////////////////////////

TMockChunkSliceFetcherFactory::TMockChunkSliceFetcherFactory(std::vector<TStrictMockChunkSliceFetcherPtr>* fetchers)
    : Fetchers_(fetchers)
{ }

IChunkSliceFetcherPtr TMockChunkSliceFetcherFactory::CreateChunkSliceFetcher()
{
    YT_VERIFY(CurrentIndex_ < std::ssize(*Fetchers_));
    return Fetchers_->at(CurrentIndex_++);
}

void TMockChunkSliceFetcherFactory::RegisterMetadata(auto&& registrar)
{
    // NB: This is a very bad way to persist pointers, but it is ok for unittests.
    registrar.template PropertyField<1,
        [] (const TThis* this_) {
            return reinterpret_cast<intptr_t>(this_->Fetchers_);
        },
        [] (TThis* this_, intptr_t fetchersAddress) {
            this_->Fetchers_ = reinterpret_cast<std::vector<TStrictMockChunkSliceFetcherPtr>*>(fetchersAddress);
        }>("FetchersAddress_")();
    PHOENIX_REGISTER_FIELD(2, CurrentIndex_);
}

PHOENIX_DEFINE_TYPE(TMockChunkSliceFetcherFactory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools

