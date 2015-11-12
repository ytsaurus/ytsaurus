#include "file_commands.h"
#include "config.h"
#include "driver.h"

#include <yt/ytlib/api/file_reader.h>
#include <yt/ytlib/api/file_writer.h>

#include <yt/ytlib/chunk_client/chunk_spec.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/common.h>

namespace NYT {
namespace NDriver {

using namespace NApi;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

void TReadFileCommand::Execute(ICommandContextPtr context)
{
    // COMPAT(babenko): remove Request_->FileReader
    auto config = UpdateYsonSerializable(
        context->GetConfig()->FileReader,
        FileReader);
    config = UpdateYsonSerializable(
        config,
        GetOptions());

    Options.Config = std::move(config);

    auto reader = context->GetClient()->CreateFileReader(
        Path.GetPath(),
        Options);

    WaitFor(reader->Open())
        .ThrowOnError();

    auto output = context->Request().OutputStream;

    while (true) {
        auto block = WaitFor(reader->Read())
            .ValueOrThrow();

        if (!block)
            break;

        WaitFor(output->Write(block))
            .ThrowOnError();
    }
}

//////////////////////////////////////////////////////////////////////////////////

void TWriteFileCommand::Execute(ICommandContextPtr context)
{
    // COMPAT(sandello): remove Request_->FileReader ??
    auto config = UpdateYsonSerializable(
        context->GetConfig()->FileWriter,
        FileWriter);
    config = UpdateYsonSerializable(
        config,
        GetOptions());

    Options.Config = std::move(config);
    Options.Append = Path.GetAppend();

    auto writer = context->GetClient()->CreateFileWriter(
        Path.GetPath(),
        Options);

    WaitFor(writer->Open())
        .ThrowOnError();

    struct TWriteBufferTag { };

    auto buffer = TSharedMutableRef::Allocate<TWriteBufferTag>(context->GetConfig()->WriteBufferSize, false);

    auto input = context->Request().InputStream;

    while (true) {
        auto bytesRead = WaitFor(input->Read(buffer))
            .ValueOrThrow();

        if (bytesRead == 0)
            break;

        WaitFor(writer->Write(buffer.Slice(0, bytesRead)))
            .ThrowOnError();
    }

    WaitFor(writer->Close())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
