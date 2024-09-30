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
using namespace NSquashFS;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TCypressFileImageReader
    : public IImageReader
{
public:
    TCypressFileImageReader(
        IRandomAccessFileReaderPtr reader,
        TLogger logger)
        : Reader_(std::move(reader))
        , Logger(std::move(logger.WithTag("Path: %v", Reader_->GetPath())))
    { }

    void Initialize() override
    {
        YT_LOG_INFO("Initializing cypress file image reader");

        Reader_->Initialize();

        YT_LOG_INFO("Initialized cypress file image reader");
    }

    TFuture<TSharedRef> Read(
        i64 offset,
        i64 length) override
    {
        return Reader_->Read(
            offset,
            length);
    }

    i64 GetSize() const override
    {
        return Reader_->GetSize();
    }

    TReadersStatistics GetStatistics() const override
    {
        return Reader_->GetStatistics();
    }

    TString GetPath() const override
    {
        return Reader_->GetPath();
    }

private:
    const IRandomAccessFileReaderPtr Reader_;
    const TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

struct TVirtualSquashFSImageReaderTag {};

////////////////////////////////////////////////////////////////////////////////

class TVirtualSquashFSImageReader
    : public IImageReader
{
public:
    TVirtualSquashFSImageReader(
        std::unordered_map<TYPath, std::vector<NChunkClient::NProto::TChunkSpec>> pathToChunkSpecs,
        TSquashFSLayoutPtr layout,
        IClientPtr client,
        IThroughputThrottlerPtr inThrottler,
        IThroughputThrottlerPtr outRpsThrottler,
        IInvokerPtr invoker,
        TLogger logger)
        : Layout_(std::move(layout))
        , Client_(std::move(client))
        , InThrottler_(std::move(inThrottler))
        , OutRpsThrottler_(std::move(outRpsThrottler))
        , Invoker_(std::move(invoker))
        , Logger(std::move(logger.WithTag("VirtualImageId: %v", TGuid::Create())))
        , Size_(Layout_->GetSize())
    {
        const auto& files = Layout_->Files();
        Readers_.resize(std::ssize(files));

        for (int i = 0; i < std::ssize(files); ++i) {
            const auto& path = files[i].Path;
            Readers_[i] = CreateRandomAccessFileReader(
                std::move(pathToChunkSpecs[path]),
                path,
                Client_,
                InThrottler_,
                OutRpsThrottler_,
                Invoker_,
                Logger);
        }
    }

    void Initialize() override
    {
        YT_LOG_INFO("Initializing virtual squashfs image reader");

        for (const auto& reader : Readers_) {
            reader->Initialize();
        }

        YT_LOG_INFO("Initialized virtual squashfs image reader");
    }

    TFuture<TSharedRef> Read(
        i64 offset,
        i64 length) override
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
            "Start read image (Offset: %v, Length: %v)",
            offset,
            length);

        if (length == 0) {
            YT_LOG_DEBUG(
                "Finish read image (Offset: %v, Length: %v)",
                offset,
                length);
            return MakeFuture<TSharedRef>({});
        }

        std::vector<TFuture<TSharedRef>> readFutures;

        // Read the head from the image.
        i64 headSize = Layout_->GetHeadSize();
        if (offset < headSize) {
            i64 sizeWithinHead = std::min(headSize - offset, length);
            readFutures.push_back(MakeFuture(
                Layout_->ReadHead(offset, sizeWithinHead)));

            offset += sizeWithinHead;
            length -= sizeWithinHead;
        }

        // Read the files from the readers.
        const auto& files = Layout_->Files();
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

        // Read the tail from the image.
        i64 tailOffset = Layout_->GetTailOffset();
        i64 tailSize = Layout_->GetTailSize();
        if (length > 0 &&
            tailOffset <= offset &&
            offset < tailOffset + tailSize)
        {
            i64 beginWithinTail = std::max(offset - tailOffset, 0l);
            i64 endWithinTail = std::min(beginWithinTail + length, tailSize);
            i64 sizeWithinTail = endWithinTail - beginWithinTail;

            readFutures.push_back(MakeFuture(
                Layout_->ReadTail(beginWithinTail, sizeWithinTail)));

            offset += sizeWithinTail;
            length -= sizeWithinTail;
        }

        // Add the padding.
        if (length > 0) {
            readFutures.push_back(MakeFuture(
                TSharedRef(TSharedMutableRef::Allocate(length))));
        }

        return AllSucceeded(readFutures).Apply(BIND([=, this, this_ = MakeStrong(this)] (const std::vector<TSharedRef>& partReadResults) {
            // Merge refs into single ref.
            auto mergedRefs = MergeRefsToRef<TVirtualSquashFSImageReaderTag>(partReadResults);
            YT_LOG_DEBUG(
                "Finish read image (Offset: %v, ExpectedLength: %v, ResultLength: %v)",
                offset,
                length,
                mergedRefs.Size());
            return mergedRefs;
        }).AsyncVia(Invoker_));
    }

    i64 GetSize() const override
    {
        return Size_;
    }

    TReadersStatistics GetStatistics() const override
    {
        TReadersStatistics cumulativeStatistics;
        for (const auto& reader : Readers_) {
            auto readerStatistics = reader->GetStatistics();
            cumulativeStatistics.ReadBytes += readerStatistics.ReadBytes;
            cumulativeStatistics.DataBytesReadFromCache += readerStatistics.DataBytesReadFromCache;
            cumulativeStatistics.DataBytesReadFromDisk += readerStatistics.DataBytesReadFromDisk;
            cumulativeStatistics.MetaBytesReadFromDisk += readerStatistics.MetaBytesReadFromDisk;
        }

        return cumulativeStatistics;
    }

    TString GetPath() const override
    {
        return "virtual";
    }

private:
    const TSquashFSLayoutPtr Layout_;
    const IClientPtr Client_;
    const IThroughputThrottlerPtr InThrottler_;
    const IThroughputThrottlerPtr OutRpsThrottler_;
    const IInvokerPtr Invoker_;
    const TLogger Logger;
    const i64 Size_;
    std::vector<IRandomAccessFileReaderPtr> Readers_;
};

////////////////////////////////////////////////////////////////////////////////

IImageReaderPtr CreateCypressFileImageReader(
    IRandomAccessFileReaderPtr reader,
    TLogger logger)
{
    return New<TCypressFileImageReader>(
        std::move(reader),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

IImageReaderPtr CreateVirtualSquashFSImageReader(
    std::unordered_map<TYPath, std::vector<NChunkClient::NProto::TChunkSpec>> pathToChunkSpecs,
    TSquashFSLayoutPtr layout,
    IClientPtr client,
    IThroughputThrottlerPtr inThrottler,
    IThroughputThrottlerPtr outRpsThrottler,
    IInvokerPtr invoker,
    TLogger logger)
{
    return New<TVirtualSquashFSImageReader>(
        std::move(pathToChunkSpecs),
        std::move(layout),
        std::move(client),
        std::move(inThrottler),
        std::move(outRpsThrottler),
        std::move(invoker),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
