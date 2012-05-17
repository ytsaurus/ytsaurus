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


TCommandDescriptor TDownloadCommand::GetDescriptor()
{
    return TCommandDescriptor(EDataType::Null, EDataType::Binary);
}

void TDownloadCommand::DoExecute(TDownloadRequestPtr request)
{
    auto config = Context->GetConfig()->FileReader;

    auto reader = New<TFileReader>(
        ~config,
        ~Context->GetMasterChannel(),
        ~Context->GetTransaction(request),
        ~Context->GetBlockCache(),
        request->Path);
    reader->Open();

    // TODO(babenko): use FileName and Executable values

    auto output = Context->GetOutputStream();

    while (true) {
        auto block = reader->Read();
        if (!block) {
            break;
        }
        output->Write(block.Begin(), block.Size());
    }
}

////////////////////////////////////////////////////////////////////////////////

TCommandDescriptor TUploadCommand::GetDescriptor()
{
    return TCommandDescriptor(EDataType::Binary, EDataType::Null);
}

void TUploadCommand::DoExecute(TUploadRequestPtr request)
{
    auto config = Context->GetConfig()->FileWriter;

    auto writer = New<TFileWriter>(
        ~config,
        ~Context->GetMasterChannel(),
        ~Context->GetTransaction(request),
        ~Context->GetTransactionManager(),
        request->Path);
    writer->Open();

    auto input = Context->GetInputStream();
    
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
        .BeginMap()
            .Item("object_id").Scalar(id.ToString())
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
