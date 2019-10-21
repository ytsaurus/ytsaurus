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
    return NNative::CreateFileReader(this, path, options);
}

IFileWriterPtr TClient::CreateFileWriter(
    const TRichYPath& path,
    const TFileWriterOptions& options)
{
    return NNative::CreateFileWriter(this, path, options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
