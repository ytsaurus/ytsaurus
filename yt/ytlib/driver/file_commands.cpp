#include "stdafx.h"
#include "file_commands.h"
#include "config.h"
#include "driver.h"

#include <ytlib/ytree/fluent.h>
#include <ytlib/transaction_client/transaction_manager.h>
#include <ytlib/file_client/file_reader.h>
#include <ytlib/file_client/file_writer.h>

namespace NYT {
namespace NDriver {

using namespace NFileClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TDownloadCommand::DoExecute()
{
    auto config = Context->GetConfig()->FileReader;

    auto reader = New<TFileReader>(
        config,
        Context->GetMasterChannel(),
        GetTransaction(false),
        Context->GetBlockCache(),
        Request->Path);
    reader->Open();

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

    auto attributes =
        Request->Attributes
        ? ConvertToAttributes(Request->Attributes)
        : CreateEphemeralAttributes();

    auto writer = New<TFileWriter>(
        config,
        Context->GetMasterChannel(),
        GetTransaction(false),
        Context->GetTransactionManager(),
        Request->Path,
        ~attributes);
    writer->Open();

    auto input = Context->GetRequest()->InputStream;

    TBlob buffer(config->BlockSize, false);
    while (true) {
        size_t bytesRead = input->Read(buffer.Begin(), buffer.Size());
        if (bytesRead == 0)
            break;
        TRef block(buffer.Begin(), bytesRead);
        writer->Write(block);
    }

    writer->Close();

    auto id = writer->GetNodeId();
    BuildYsonFluently(~Context->CreateOutputConsumer())
        .Value(id.ToString());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
