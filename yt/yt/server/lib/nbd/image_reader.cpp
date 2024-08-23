#include "image_reader.h"

#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/ytree/ypath_proxy.h>

namespace NYT::NNbd {

using namespace NApi::NNative;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NLogging;
using namespace NObjectClient;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TCypressFileImageReader
    : public IImageReader
{
public:
    TCypressFileImageReader(
        std::vector<NChunkClient::NProto::TChunkSpec> chunkSpecs,
        TString path,
        IClientPtr client,
        IThroughputThrottlerPtr inThrottler,
        IThroughputThrottlerPtr outRpsThrottler,
        TLogger logger);

    void Initialize() override;

    TFuture<TSharedRef> Read(
        i64 offset,
        i64 length) override;

    i64 GetSize() const override;

    TReadersStatistics GetStatistics() const override;

private:
    const TString Path_;
    const IClientPtr Client_;
    const IThroughputThrottlerPtr InThrottler_;
    const IThroughputThrottlerPtr OutRpsThrottler_;
    const TLogger Logger;
    const IRandomAccessFileReaderPtr Reader_;
};

DECLARE_REFCOUNTED_CLASS(TCypressFileImageReader)
DEFINE_REFCOUNTED_TYPE(TCypressFileImageReader)

////////////////////////////////////////////////////////////////////////////////

TCypressFileImageReader::TCypressFileImageReader(
    std::vector<NChunkClient::NProto::TChunkSpec> chunkSpecs,
    TString path,
    IClientPtr client,
    IThroughputThrottlerPtr inThrottler,
    IThroughputThrottlerPtr outRpsThrottler,
    TLogger logger)
    : Path_(std::move(path))
    , Client_(std::move(client))
    , InThrottler_(std::move(inThrottler))
    , OutRpsThrottler_(std::move(outRpsThrottler))
    , Logger(std::move(logger))
    , Reader_(CreateRandomAccessFileReader(
        std::move(chunkSpecs),
        Path_,
        Client_,
        InThrottler_,
        OutRpsThrottler_,
        Logger))
{ }

void TCypressFileImageReader::Initialize()
{
    YT_LOG_INFO("Initializing Cypress file image reader (Path: %v)", Path_);

    Reader_->Initialize();

    YT_LOG_INFO("Initialized Cypress file image reader (Path: %v)", Path_);
}

TFuture<TSharedRef> TCypressFileImageReader::Read(
    i64 offset,
    i64 length)
{
    return Reader_->Read(
        offset,
        length);
}

i64 TCypressFileImageReader::GetSize() const
{
    return Reader_->GetSize();
}

TReadersStatistics TCypressFileImageReader::GetStatistics() const
{
    return Reader_->GetStatistics();
}

////////////////////////////////////////////////////////////////////////////////

IImageReaderPtr CreateCypressFileImageReader(
    std::vector<NChunkClient::NProto::TChunkSpec> chunkSpecs,
    TString path,
    NApi::NNative::IClientPtr client,
    IThroughputThrottlerPtr inThrottler,
    IThroughputThrottlerPtr outRpsThrottler,
    NLogging::TLogger logger)
{
    return New<TCypressFileImageReader>(
        std::move(chunkSpecs),
        std::move(path),
        std::move(client),
        std::move(inThrottler),
        std::move(outRpsThrottler),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
