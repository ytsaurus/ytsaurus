#include "chunk_slice_fetcher.h"

namespace NYT::NChunkPools {

using namespace NTableClient;
using namespace NPhoenix2;

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
    // NB: this is a very bad way to persist pointers, but it is ok for unittests.
    registrar.template VirtualField<1>("FetchersAddress_", [] (TThis* this_, auto& context) {
        auto fetchersAddress = Load<intptr_t>(context);
        this_->Fetchers_ = reinterpret_cast<std::vector<TStrictMockChunkSliceFetcherPtr>*>(fetchersAddress);
    }, [] (const TThis* this_, auto& context) {
        auto fetchersAddress = reinterpret_cast<intptr_t>(this_->Fetchers_);
        NYT::Save(context, fetchersAddress);
    })();
    PHOENIX_REGISTER_FIELD(2, CurrentIndex_);
}

PHOENIX_DEFINE_TYPE(TMockChunkSliceFetcherFactory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools

