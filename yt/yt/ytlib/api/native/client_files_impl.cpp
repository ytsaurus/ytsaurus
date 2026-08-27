#include "client_impl.h"
#include "file_reader.h"
#include "file_writer.h"

namespace NYT::NApi::NNative {

using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

TFuture<IFileReaderPtr> TClient::CreateFileReader(
    const TYPath& path,
    const TFileReaderOptions& options)
{
    return NNative::CreateFileReader(this, path, options, HeavyRequestMemoryUsageTracker_);
}

IFileWriterPtr TClient::CreateFileWriter(
    const TRichYPath& path,
    const TFileWriterOptions& options)
{
    return NNative::CreateFileWriter(this, path, options, HeavyRequestMemoryUsageTracker_);
}

TFuture<TFilePartitions> TClient::PartitionFile(
    const TYPath& /*path*/,
    const std::vector<TFileReadRange>& /*ranges*/,
    const TPartitionFileOptions& /*options*/)
{
    THROW_ERROR_EXCEPTION("PartitionFile is not implemented yet");
}

TFuture<IFileReaderPtr> TClient::CreateFilePartitionReader(
    const TFilePartitionCookiePtr& /*cookie*/,
    const TReadFilePartitionOptions& /*options*/)
{
    THROW_ERROR_EXCEPTION("CreateFilePartitionReader is not implemented yet");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
