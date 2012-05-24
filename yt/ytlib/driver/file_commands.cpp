#include "stdafx.h"
#include "file_commands.h"
#include "config.h"
#include "driver.h"

#include <ytlib/file_client/file_reader.h>
#include <ytlib/file_client/file_writer.h>

namespace NYT {
namespace NDriver {

using namespace NFileClient;

////////////////////////////////////////////////////////////////////////////////

void TDownloadCommand::DoExecute()
{
    auto config = Context->GetConfig()->FileReader;

    auto reader = New<TFileReader>(
        config,
        Context->GetMasterChannel(),
        ~GetTransaction(false),
        ~Context->GetBlockCache(),
        Request->Path);
    reader->Open();

    // TODO(babenko): use FileName and Executable values

    auto output = Context->GetRequest()->OutputStream;

    while (true) {
        auto block = reader->Read();
        if (!block) {
            break;
        }
        output->Write(block.Begin(), block.Size());
    }
}

//////////////////////////////////////////////////////////////////////////////////

void TUploadCommand::DoExecute()
{
    auto config = Context->GetConfig()->FileWriter;

    auto writer = New<TFileWriter>(
        config,
        Context->GetMasterChannel(),
        GetTransaction(false),
        Context->GetTransactionManager(),
        Request->Path);
    writer->Open();

    auto input = Context->GetRequest()->InputStream;
    
    TBlob buffer(config->BlockSize);
    while (true) {
        size_t bytesRead = input->Read(buffer.begin(), buffer.size());
        if (bytesRead == 0)
            break;
        TRef block(buffer.begin(), bytesRead);
        writer->Write(block);
    }

    writer->Close();

    auto id = writer->GetNodeId();
    BuildYsonFluently(~Context->CreateOutputConsumer())
        .Scalar(id.ToString());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
