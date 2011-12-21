#include "stdafx.h"
#include "file_commands.h"

#include "../file_client/file_reader.h"
#include "../file_client/file_writer.h"

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NFileClient;

////////////////////////////////////////////////////////////////////////////////

void TDownloadCommand::DoExecute(TDownloadRequest* request)
{
    auto config = DriverImpl->GetConfig()->FileDownloader;

    auto reader = New<TFileReader>(
        ~config,
        DriverImpl->GetMasterChannel(),
        DriverImpl->GetCurrentTransaction(),
        request->Path);

    auto output = DriverImpl->CreateOutputStream(request->Stream);

    while (true) {
        auto block = reader->Read();
        if (~block == NULL)
            break;

        output->Write(block.Begin(), block.Size());
    }
}

////////////////////////////////////////////////////////////////////////////////

void TUploadCommand::DoExecute(TUploadRequest* request)
{
    auto config = DriverImpl->GetConfig()->FileUploader;

    auto writer = New<TFileWriter>(
        ~config,
        DriverImpl->GetMasterChannel(),
        DriverImpl->GetCurrentTransaction(),
        request->Path);

    auto input = DriverImpl->CreateInputStream(request->Stream);
    
    TBlob buffer(config->BlockSize);
    while (true) {
        size_t bytesRead = input->Read(buffer.begin(), buffer.size());
        if (bytesRead == 0)
            break;
        TRef block(buffer.begin(), bytesRead);
        writer->Write(block);
    }

    writer->Close();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
