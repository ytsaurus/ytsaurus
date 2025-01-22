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
        YT_LOG_INFO("Initializing Cypress file image reader");

        Reader_->Initialize();

        YT_LOG_INFO("Initialized Cypress file image reader");
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
        std::vector<TArtifactMountOptions> mountOptions,
        TSquashFSLayoutBuilderOptions builderOptions,
        IInvokerPtr invoker,
        TLogger logger)
        : MountOptions_(std::move(mountOptions))
        , BuilderOptions_(std::move(builderOptions))
        , Invoker_(std::move(invoker))
        , Logger(std::move(logger))
    { }

    void Initialize() override
    {
        YT_LOG_INFO("Initializing virtual squashfs image reader (FileCount: %v)", std::ssize(MountOptions_));

        auto builder = CreateSquashFSLayoutBuilder(BuilderOptions_);

        for (const auto& mount : MountOptions_) {
            mount.Reader->Initialize();
            YT_LOG_DEBUG("Add file to virtual layer (Path: %v, Size: %v)",
                mount.Path,
                mount.Reader->GetSize());
            builder->AddFile(
                mount.Path,
                mount.Permissions,
                mount.Reader);
        }

        Layout_ = builder->Build();
        Size_ = Layout_->GetSize();

        YT_LOG_INFO("Initialized virtual squashfs image reader (Size: %v, HeadSize: %v, TailSize: %v, FileCount: %v)",
            Size_,
            Layout_->GetHeadSize(),
            Layout_->GetTailSize(),
            std::ssize(MountOptions_));
    }

    TFuture<TSharedRef> Read(
        i64 offset,
        i64 length) override
    {
        if (offset < 0 ||
            length < 0 ||
            offset + length > Size_)
        {
            return MakeFuture<TSharedRef>(TError("Invalid image read request")
                << TErrorAttribute("offset", offset)
                << TErrorAttribute("length", length));
        }

        YT_LOG_INFO("Start read virtual squashfs image (Offset: %v, Length: %v)",
            offset,
            length);

        if (length == 0) {
            YT_LOG_INFO("Finish read virtual squashfs image (Offset: %v, Length: %v)",
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
        for (const auto& part : Layout_->GetParts()) {
            YT_VERIFY(part.Size > 0);

            if (length <= 0) {
                break;
            }

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

            auto readFuture = part.Reader->Read(
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
            YT_LOG_INFO("Finish read virtual squashfs image (Offset: %v, ExpectedLength: %v, ResultLength: %v)",
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
        for (const auto& part : Layout_->GetParts()) {
            auto readerStatistics = part.Reader->GetStatistics();
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
    const std::vector<TArtifactMountOptions> MountOptions_;
    const TSquashFSLayoutBuilderOptions BuilderOptions_;
    const IInvokerPtr Invoker_;
    const TLogger Logger;
    TSquashFSLayoutPtr Layout_;
    i64 Size_;
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
    std::vector<TArtifactMountOptions> mountOptions,
    NSquashFS::TSquashFSLayoutBuilderOptions builderOptions,
    IInvokerPtr invoker,
    TLogger logger)
{
    return New<TVirtualSquashFSImageReader>(
        std::move(mountOptions),
        std::move(builderOptions),
        std::move(invoker),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
