#include "stdafx.h"
#include "file_commands.h"
#include "config.h"
#include "driver.h"

#include <ytlib/api/file_reader.h>
#include <ytlib/api/file_writer.h>

#include <ytlib/chunk_client/chunk_spec.h>

#include <core/concurrency/fiber.h>

namespace NYT {
namespace NDriver {

using namespace NApi;
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

    TFileReaderOptions options;
    options.Offset = Request->Offset;
    options.Length = Request->Length;
    SetTransactionalOptions(&options);
    SetSuppressableAccessTrackingOptions(&options);

    auto reader = Context->GetClient()->CreateFileReader(
        Request->Path.GetPath(),
        options,
        config);

    {
        auto result = WaitFor(reader->Open());
        THROW_ERROR_EXCEPTION_IF_FAILED(result);
    }

    auto output = Context->Request().OutputStream;

    while (true) {
        auto blockOrError = WaitFor(reader->Read());

        THROW_ERROR_EXCEPTION_IF_FAILED(blockOrError);
        auto block = blockOrError.Value();

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

    TFileWriterOptions options;
    options.Append = Request->Path.GetAppend();
    SetTransactionalOptions(&options);

    auto writer = Context->GetClient()->CreateFileWriter(
        Request->Path.GetPath(),
        options,
        config);

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

        size_t bytesRead = input->GetReadLength();
        if (bytesRead == 0)
            break;

        {
            auto result = WaitFor(writer->Write(TRef(buffer.Begin(), bytesRead)));
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
