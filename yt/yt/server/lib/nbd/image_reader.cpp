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
using namespace NSquashFs;
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

struct TVirtualSquashFsImageReaderTag {};

////////////////////////////////////////////////////////////////////////////////

class TVirtualSquashFsImageReader
    : public IImageReader
{
public:
    TVirtualSquashFsImageReader(
        std::unordered_map<TString, std::vector<NChunkClient::NProto::TChunkSpec>> pathToChunkSpecs,
        TSquashFsImagePtr image,
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
    const TSquashFsImagePtr Image_;
    const IClientPtr Client_;
    const IThroughputThrottlerPtr InThrottler_;
    const IThroughputThrottlerPtr OutRpsThrottler_;
    const TLogger Logger;
    const i64 Size_;
    std::vector<IRandomAccessFileReaderPtr> Readers_;
};

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

TVirtualSquashFsImageReader::TVirtualSquashFsImageReader(
    std::unordered_map<TString, std::vector<NChunkClient::NProto::TChunkSpec>> pathToChunkSpecs,
    TSquashFsImagePtr image,
    IClientPtr client,
    IThroughputThrottlerPtr inThrottler,
    IThroughputThrottlerPtr outRpsThrottler,
    TLogger logger)
    : Image_(std::move(image))
    , Client_(std::move(client))
    , InThrottler_(std::move(inThrottler))
    , OutRpsThrottler_(std::move(outRpsThrottler))
    , Logger(std::move(logger))
    , Size_(Image_->GetSize())
{
    const auto& files = Image_->Files();
    Readers_.resize(std::ssize(files));

    for (int i = 0; i < std::ssize(files); ++i) {
        const auto& path = files[i].Path;
        Readers_[i] = CreateRandomAccessFileReader(
            std::move(pathToChunkSpecs[path]),
            path,
            Client_,
            InThrottler_,
            OutRpsThrottler_,
            Logger);
    }
}

void TVirtualSquashFsImageReader::Initialize()
{
    YT_LOG_INFO("Initializing Virtual squashFs image reader");

    for (const auto& reader : Readers_) {
        reader->Initialize();
    }

    YT_LOG_INFO("Initialized Virtual squashFs image reader");
}

TFuture<TSharedRef> TVirtualSquashFsImageReader::Read(
    i64 offset,
    i64 length)
{
    if (offset < 0 ||
        length < 0 ||
        offset + length > Size_) {
        THROW_ERROR_EXCEPTION(
            "Invalid read offset %v with length %v",
            offset,
            length);
    }

    YT_LOG_DEBUG(
        "Start read (Offset: %v, Length: %v)",
        offset,
        length);

    if (length == 0) {
        YT_LOG_DEBUG(
            "Finish read (Offset: %v, Length: %v)",
            offset,
            length);
        return MakeFuture<TSharedRef>({});
    }

    std::vector<TFuture<TSharedRef>> readFutures;

    // Read the header from the image.
    i64 headerSize = Image_->GetHeaderSize();
    if (offset < headerSize) {
        i64 sizeWithinHeader = std::min(headerSize - offset, length);
        readFutures.push_back(MakeFuture(
            Image_->ReadHeader(offset, sizeWithinHeader)));

        offset += sizeWithinHeader;
        length -= sizeWithinHeader;
    }

    // Read the files from the readers.
    const auto& files = Image_->Files();
    for (int i = 0; i < std::ssize(files); ++i) {
        auto& part = files[i];
        auto partBegin = part.Offset;
        auto partEnd = partBegin + part.Size;

        if (offset >= partEnd || offset + length <= partBegin) {
            continue;
        }

        i64 beginWithinPart = std::max(offset - part.Offset, 0l);
        i64 endWithinPart = std::min(beginWithinPart + length, part.Size);
        i64 sizeWithinPart = endWithinPart - beginWithinPart;

        YT_VERIFY(0 <= beginWithinPart);
        YT_VERIFY(beginWithinPart < endWithinPart);
        YT_VERIFY(endWithinPart <= part.Size);
        YT_VERIFY(sizeWithinPart <= part.Size);
        YT_VERIFY(sizeWithinPart <= length);

        auto readFuture = Readers_[i]->Read(
            beginWithinPart,
            sizeWithinPart);
        readFutures.push_back(std::move(readFuture));

        length -= sizeWithinPart;
        offset += sizeWithinPart;
    }

    // Add the padding.
    if (length > 0) {
        readFutures.push_back(MakeFuture(
            TSharedRef(TSharedMutableRef::Allocate(length))));
    }

    return AllSucceeded(readFutures).Apply(BIND([=, Logger = Logger] (const std::vector<TSharedRef>& partReadResults) {
        // Merge refs into single ref.
        auto mergedRefs = MergeRefsToRef<TVirtualSquashFsImageReaderTag>(partReadResults);
        YT_LOG_DEBUG(
            "Finish read (Offset: %v, ExpectedLength: %v, ResultLength: %v)",
            offset,
            length,
            mergedRefs.Size());
        return mergedRefs;
    }));
}

i64 TVirtualSquashFsImageReader::GetSize() const
{
    return Size_;
}

TReadersStatistics TVirtualSquashFsImageReader::GetStatistics() const
{
    TReadersStatistics cumulativeStatistics;
    for (const auto& reader : Readers_) {
        auto readerStatistics = reader->GetStatistics();
        cumulativeStatistics.ReadBytes += readerStatistics.ReadBytes;
        cumulativeStatistics.ReadBlockBytesFromCache += readerStatistics.ReadBlockBytesFromCache;
        cumulativeStatistics.ReadBlockBytesFromDisk += readerStatistics.ReadBlockBytesFromDisk;
        cumulativeStatistics.ReadBlockMetaBytesFromDisk += readerStatistics.ReadBlockMetaBytesFromDisk;
    }

    return cumulativeStatistics;
}

////////////////////////////////////////////////////////////////////////////////

IImageReaderPtr CreateCypressFileImageReader(
    std::vector<NChunkClient::NProto::TChunkSpec> chunkSpecs,
    TString path,
    IClientPtr client,
    IThroughputThrottlerPtr inThrottler,
    IThroughputThrottlerPtr outRpsThrottler,
    TLogger logger)
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

IImageReaderPtr CreateVirtualSquashFsImageReader(
    std::unordered_map<TString, std::vector<NChunkClient::NProto::TChunkSpec>> pathToChunkSpecs,
    TSquashFsImagePtr image,
    IClientPtr client,
    IThroughputThrottlerPtr inThrottler,
    IThroughputThrottlerPtr outRpsThrottler,
    TLogger logger)
{
    return New<TVirtualSquashFsImageReader>(
        std::move(pathToChunkSpecs),
        std::move(image),
        std::move(client),
        std::move(inThrottler),
        std::move(outRpsThrottler),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
