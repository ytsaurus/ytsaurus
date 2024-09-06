#include "client_impl.h"

namespace NYT::NApi::NNative {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TShuffleHandlePtr TClient::DoStartShuffle(
    const TString& /*account*/,
    int /*partitionCount*/,
    const TStartShuffleOptions& /*options*/)
{
    YT_UNIMPLEMENTED();
}

void TClient::DoFinishShuffle(
    const TShuffleHandlePtr& /*shuffleHandle*/,
    const TFinishShuffleOptions& /*options*/)
{
    YT_UNIMPLEMENTED();
}

void TClient::DoRegisterShuffleChunks(
    const TShuffleHandlePtr& /*shuffleHandle*/,
    const std::vector<NChunkClient::NProto::TChunkSpec>& /*chunkSpecs*/,
    const TRegisterShuffleChunksOptions& /*options*/)
{
    YT_UNIMPLEMENTED();
}

std::vector<NChunkClient::NProto::TChunkSpec> TClient::DoFetchShuffleChunks(
    const TShuffleHandlePtr& /*shuffleHandle*/,
    int /*partitionIndex*/,
    const TFetchShuffleChunksOptions& /*options*/)
{
    YT_UNIMPLEMENTED();
}

////////////////////////////////////////////////////////////////////////////////

TFuture<IRowBatchReaderPtr> TClient::CreateShuffleReader(
    const TShuffleHandlePtr& /*shuffleHandle*/,
    int /*partitionIndex*/,
    const TTableReaderConfigPtr& /*config*/)
{
    YT_UNIMPLEMENTED();
}

TFuture<IRowBatchWriterPtr> TClient::CreateShuffleWriter(
    const TShuffleHandlePtr& /*shuffleHandle*/,
    const TString& /*partitionColumn*/,
    const TTableWriterConfigPtr& /*config*/)
{
    YT_UNIMPLEMENTED();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
