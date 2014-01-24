#include "stdafx.h"
#include "file_commands.h"
#include "config.h"
#include "driver.h"

#include <core/concurrency/fiber.h>

#include <ytlib/file_client/file_reader.h>
#include <ytlib/file_client/file_writer.h>

namespace NYT {
namespace NDriver {

using namespace NFileClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

void TDownloadCommand::DoExecute()
{
    // COMPAT(babenko): remove Request->FileReader
    auto config = UpdateYsonSerializable(
        Context->GetConfig()->FileReader,
        Request->FileReader);
    config = UpdateYsonSerializable(
        config,
        Request->GetOptions());

    auto reader = New<TAsyncReader>(
        config,
        Context->GetMasterChannel(),
        Context->GetBlockCache(),
        GetTransaction(EAllowNullTransaction::Yes, EPingTransaction::Yes),
        Request->Path,
        Request->Offset,
        Request->Length);

    {
        auto result = WaitFor(reader->AsyncOpen());
        THROW_ERROR_EXCEPTION_IF_FAILED(result);
    }

    auto output = Context->Request().OutputStream;

    while (true) {
        auto blockOrError = WaitFor(reader->AsyncRead());

        THROW_ERROR_EXCEPTION_IF_FAILED(blockOrError);
        auto block = blockOrError.GetValue();

        if (!block)
            break;

        if (!output->Write(block.Begin(), block.Size())) {
            auto result = WaitFor(output->GetReadyEvent());
            THROW_ERROR_EXCEPTION_IF_FAILED(result);
        }
    }
}

//////////////////////////////////////////////////////////////////////////////////

void TUploadCommand::DoExecute()
{
    auto config = UpdateYsonSerializable(
        Context->GetConfig()->FileWriter,
        Request->FileWriter);

    auto writer = New<TAsyncWriter>(
        config,
        Context->GetMasterChannel(),
        GetTransaction(EAllowNullTransaction::Yes, EPingTransaction::Yes),
        Context->GetTransactionManager(),
        Request->Path);

    {
        auto result = WaitFor(writer->Open());
        THROW_ERROR_EXCEPTION_IF_FAILED(result);
    }

    struct TUploadBufferTag { };
    auto buffer = TSharedRef::Allocate<TUploadBufferTag>(config->BlockSize);

    auto input = Context->Request().InputStream;

    while (true) {
        if (!input->Read(buffer.Begin(), buffer.Size())) {
            auto result = WaitFor(input->GetReadyEvent());
            THROW_ERROR_EXCEPTION_IF_FAILED(result);
        }

        size_t length = input->GetReadLength();
        if (length == 0)
            break;

        {
            auto result = WaitFor(writer->Write(TRef(buffer.Begin(), length)));
            THROW_ERROR_EXCEPTION_IF_FAILED(result);
        }
    }

    {
        auto result = WaitFor(writer->Close());
        THROW_ERROR_EXCEPTION_IF_FAILED(result);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
