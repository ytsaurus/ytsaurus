#include "file_commands.h"
#include "config.h"

#include <yt/ytlib/api/file_reader.h>
#include <yt/ytlib/api/file_writer.h>

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NDriver {

using namespace NApi;
using namespace NConcurrency;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TReadFileCommand::TReadFileCommand()
{
    RegisterParameter("path", Path);
    RegisterParameter("offset", Options.Offset)
        .Optional();
    RegisterParameter("length", Options.Length)
        .Optional();
    RegisterParameter("file_reader", FileReader)
        .Default(nullptr);

}

void TReadFileCommand::DoExecute(ICommandContextPtr context)
{
    Options.Config = UpdateYsonSerializable(
        context->GetConfig()->FileReader,
        FileReader);

    auto reader = WaitFor(
        context->GetClient()->CreateFileReader(
            Path.GetPath(),
            Options))
        .ValueOrThrow();

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

////////////////////////////////////////////////////////////////////////////////

TWriteFileCommand::TWriteFileCommand()
{
    RegisterParameter("path", Path);
    RegisterParameter("file_writer", FileWriter)
        .Default();
    RegisterParameter("compute_md5", ComputeMD5)
        .Default(false);
}

void TWriteFileCommand::DoExecute(ICommandContextPtr context)
{
    Options.Config = UpdateYsonSerializable(
        context->GetConfig()->FileWriter,
        FileWriter);
    Options.Append = Path.GetAppend();
    Options.ComputeMD5 = ComputeMD5;

    if (Path.GetAppend() && Path.GetCompressionCodec()) {
        THROW_ERROR_EXCEPTION("YPath attributes \"append\" and \"compression_codec\" are not compatible")
            << TErrorAttribute("path", Path);
    }
    Options.CompressionCodec = Path.GetCompressionCodec();

    if (Path.GetAppend() && Path.GetErasureCodec()) {
        THROW_ERROR_EXCEPTION("YPath attributes \"append\" and \"erasure_codec\" are not compatible")
            << TErrorAttribute("path", Path);
    }
    Options.ErasureCodec = Path.GetErasureCodec();

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

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

TGetFileFromCacheCommand::TGetFileFromCacheCommand()
{
    RegisterParameter("md5", MD5);
    RegisterParameter("cache_path", Options.CachePath);
}

void TGetFileFromCacheCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->GetFileFromCache(MD5, Options);
    auto result = WaitFor(asyncResult)
        .ValueOrThrow();
    context->ProduceOutputValue(BuildYsonStringFluently()
        .Value(result.Path));
}

////////////////////////////////////////////////////////////////////////////////

TPutFileToCacheCommand::TPutFileToCacheCommand()
{
    RegisterParameter("path", Path);
    RegisterParameter("md5", MD5);
    RegisterParameter("cache_path", Options.CachePath);
}

void TPutFileToCacheCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->PutFileToCache(Path, MD5, Options);
    auto result = WaitFor(asyncResult)
        .ValueOrThrow();
    context->ProduceOutputValue(BuildYsonStringFluently()
        .Value(result.Path));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
