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

        auto FD = index == 0 ? pipe.ReadFD : pipe.WriteFD;
        Process_.AddDup2FileAction(FD, index);
    }

    try {
        Process_.Spawn();
    } catch (...) {
        for (const auto& pipe : pipes) {
            YCHECK(TryClose(pipe.ReadFD, false));
            YCHECK(TryClose(pipe.WriteFD, false));
        }
        throw;
    }

    for (int index = 0; index < pipes.size(); ++index) {
        auto& pipe = pipes[index];

        YCHECK(TryClose(pipe.WriteFD, false));
        pipe.WriteFD = TPipe::InvalidFD;
        if (index == 0) {
            YCHECK(TryClose(pipe.ReadFD, false));
            pipe.ReadFD = TPipe::InvalidFD;
        } else {
            YCHECK(TryMakeNonblocking(pipe.ReadFD));
        }
    }

    auto outputStream = New<TAsyncReader>(pipes[1].ReadFD);
    auto errorStream = New<TAsyncReader>(pipes[2].ReadFD);

    auto readIntoBlob = [] (IAsyncInputStreamPtr stream) {
        TBlob output;
        auto buffer = TSharedRef::Allocate(PipeBlockSize, false);
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
        // Cleanup: trying to kill and wait for completion is case of errors.
        try {
            Process_.Kill(9);
        } catch (...) { }
        if (!Process_.Finished()) {
            Process_.Wait();
        }
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
