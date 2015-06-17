#include "stdafx.h"

#include "subprocess.h"

#include "proc.h"
#include "finally.h"

#include <core/pipes/async_reader.h>

#include <core/logging/log.h>

#include <util/system/execpath.h>

#include <array>

namespace NYT {

using namespace NConcurrency;
using namespace NPipes;

////////////////////////////////////////////////////////////////////////////////

const static size_t PipeBlockSize = 64 * 1024;
static NLogging::TLogger Logger("Subprocess");

////////////////////////////////////////////////////////////////////////////////

TSubprocess::TSubprocess(const Stroka& path)
    : Process_(path)
{ }

TSubprocess TSubprocess::CreateCurrentProcessSpawner()
{
    return TSubprocess(GetExecPath());
}

void TSubprocess::AddArgument(TStringBuf arg)
{
    Process_.AddArgument(arg);
}

void TSubprocess::AddArguments(std::initializer_list<TStringBuf> args)
{
    Process_.AddArguments(args);
}

TSubprocessResult TSubprocess::Execute()
{
#ifndef _win_
    TAsyncReaderPtr outputStream, errorStream;
    {
        std::array<TPipe, 3> pipes;

        {
            TPipeFactory pipeFactory(3);
            for (auto& pipe: pipes) {
                pipe = pipeFactory.Create();
            }
            pipeFactory.Clear();
        }

        for (int index = 0; index < pipes.size(); ++index) {
            const auto& pipe = pipes[index];

            auto fd = index == 0 ? pipe.GetReadFD() : pipe.GetWriteFD();
            Process_.AddDup2FileAction(fd, index);
        }

        Process_.Spawn();

        outputStream = pipes[1].CreateAsyncReader();
        errorStream = pipes[2].CreateAsyncReader();
    }

    auto readIntoBlob = [] (IAsyncInputStreamPtr stream) {
        TBlob output;
        auto buffer = TSharedMutableRef::Allocate(PipeBlockSize, false);
        while (true) {
            auto size = WaitFor(stream->Read(buffer))
                .ValueOrThrow();

            if (size == 0)
                break;

            // ToDo(psushin): eliminate copying.
            output.Append(buffer.Begin(), size);
        }
        return TSharedRef::FromBlob(std::move(output));
    };

    std::vector<TFuture<TSharedRef>> futures = {
        BIND(readIntoBlob, outputStream).AsyncVia(GetCurrentInvoker()).Run(),
        BIND(readIntoBlob, errorStream).AsyncVia(GetCurrentInvoker()).Run(),
    };

    try {
        auto outputsOrError = WaitFor(Combine(futures));
        THROW_ERROR_EXCEPTION_IF_FAILED(outputsOrError, "IO error occured during subprocess call");

        const auto& outputs = outputsOrError.Value();
        YCHECK(outputs.size() == 2);

        // This can block indefinitely.
        auto exitCode = Process_.Wait();
        return TSubprocessResult{outputs[0], outputs[1], exitCode};
    } catch (...) {
        Process_.KillAndWait();
        throw;
    }
#else
    THROW_ERROR_EXCEPTION("Unsupported platform");
#endif
}

void TSubprocess::Kill(int signal)
{
    Process_.Kill(signal);
}

Stroka TSubprocess::GetCommandLine() const
{
    return Process_.GetCommandLine();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
