#include "helpers.h"

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/ytlib/chunk_client/config.h>

namespace NYT::NFileClient {

using namespace NChunkClient;
using namespace NYTree;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

TMultiChunkWriterOptionsPtr GetWriterOptions(
    const IAttributeDictionaryPtr& attributes,
    const TRichYPath& path,
    const IMemoryUsageTrackerPtr& tracker)
{
    auto writerOptions = New<TMultiChunkWriterOptions>();

    auto attributesCompressionCodec = attributes->Get<NCompression::ECodec>("compression_codec");
    auto attributesErasureCodec = attributes->Get<NErasure::ECodec>("erasure_codec");

    writerOptions->ReplicationFactor = attributes->Get<int>("replication_factor");
    writerOptions->MediumName = attributes->Get<std::string>("primary_medium");
    writerOptions->Account = attributes->Get<std::string>("account");
    writerOptions->CompressionCodec = path.GetCompressionCodec().value_or(attributesCompressionCodec);
    writerOptions->ErasureCodec = path.GetErasureCodec().value_or(attributesErasureCodec);

    writerOptions->EnableStripedErasure = attributes->Get<bool>("enable_striped_erasure", false);
    writerOptions->MemoryUsageTracker = tracker;

    return writerOptions;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFileClient
