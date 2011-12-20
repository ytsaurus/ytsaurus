#include "stdafx.h"
#include "file_commands.h"

#include "../file_client/file_reader.h"
#include "../file_client/file_writer.h"

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NFileClient;

// TODO: make configurable
static const int WriteBlockSize = 1 << 20;

////////////////////////////////////////////////////////////////////////////////

void TReadFileCommand::DoExecute(TReadFileRequest* request)
{
    auto reader = New<TFileReader>(
        // TODO: fixme
        ~New<TFileReader::TConfig>(),
        ~DriverImpl->GetMasterChannel(),
        ~DriverImpl->GetTransaction(),
        request->Path);

    auto output = DriverImpl->CreateOutputStream(request->Out);

    while (true) {
        auto block = reader->Read();
        if (~block == NULL)
            break;

        output->Write(block.Begin(), block.Size());
    }
}

////////////////////////////////////////////////////////////////////////////////

void TWriteFileCommand::DoExecute(TWriteFileRequest* request)
{
    auto writer = New<TFileWriter>(
        // TODO: fixme
        ~New<TFileWriter::TConfig>(),
        ~DriverImpl->GetMasterChannel(),
        ~DriverImpl->GetTransaction(),
        request->Path);

    auto input = DriverImpl->CreateInputStream(request->In);
    
    TBlob buffer(WriteBlockSize);
    while (true) {
        size_t bytesRead = input->Read(buffer.begin(), buffer.size());
        TRef block(buffer.begin(), bytesRead);
        writer->Write(block);
    }

    writer->Close();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
