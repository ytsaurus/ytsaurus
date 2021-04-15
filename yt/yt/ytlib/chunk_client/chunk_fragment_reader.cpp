#include "chunk_fragment_reader.h"

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TChunkFragmentReader
    : public IChunkFragmentReader
{
public:
    virtual TFuture<std::vector<TSharedRef>> ReadFragments(
        const TClientChunkReadOptions& /*options*/,
        std::vector<TChunkFragmentRequest> /*requests*/) override
    {
        return MakeFuture<std::vector<TSharedRef>>(TError("Not implemented"));
    }
};

IChunkFragmentReaderPtr CreateChunkFragmentReader(
    TChunkFragmentReaderConfigPtr /*config*/,
    NApi::NNative::IClientPtr /*client*/,
    NNodeTrackerClient::INodeStatusDirectoryPtr /*nodeStatusDirectory*/)
{
    return New<TChunkFragmentReader>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
