#include "stdafx.h"
#include "file_commands.h"

#include <ytlib/file_client/file_reader.h>
#include <ytlib/file_client/file_writer.h>

namespace NYT {
namespace NDriver {

using namespace NFileClient;

////////////////////////////////////////////////////////////////////////////////

void TUploadCommand::DoExecute()
{
    auto config = DriverImpl->GetConfig()->FileWriter;

    auto writer = New<TFileWriter>(
        ~config,
        DriverImpl->GetMasterChannel(),
        ~DriverImpl->GetTransaction(TxArg->getValue()),
        DriverImpl->GetTransactionManager(),
        PathArg->getValue());
    writer->Open();

    auto input = DriverImpl->CreateInputStream();

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
    BuildYsonFluently(~DriverImpl->CreateOutputConsumer())
        .BeginMap()
            .Item("object_id").Scalar(id.ToString())
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

void TDownloadCommand::DoExecute()
{
    auto config = DriverImpl->GetConfig()->FileReader;

    auto reader = New<TFileReader>(
        ~config,
        DriverImpl->GetMasterChannel(),
        ~DriverImpl->GetTransaction(TxArg->getValue()),
        DriverImpl->GetBlockCache(),
        PathArg->getValue());
    reader->Open();

    // TODO(babenko): use FileName and Executable values

    auto output = DriverImpl->CreateOutputStream();

    while (true) {
        auto block = reader->Read();
        if (!block) {
            break;
        }
        output->Write(block.Begin(), block.Size());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
