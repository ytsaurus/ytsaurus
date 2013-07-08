#include "stdafx.h"
#include "writer.h"
#include "log.h"

#include <ytlib/misc/fs.h>

#include <yt/build.h>

namespace NYT {
namespace NLog {

////////////////////////////////////////////////////////////////////////////////

const char* const SystemLoggingCategory = "Logging";
static TLogger Logger(SystemLoggingCategory);

////////////////////////////////////////////////////////////////////////////////

namespace {

TLogEvent GetBannerEvent()
{
    return TLogEvent(
        SystemLoggingCategory,
        ELogLevel::Info,
        Sprintf("Logging started (Version: %s, BuildHost: %s, BuildTime: %s)",
            YT_VERSION,
            YT_BUILD_HOST,
            YT_BUILD_TIME));
}

} // namespace

TStreamLogWriter::TStreamLogWriter(
    TOutputStream* stream,
    Stroka pattern)
    : Stream(stream)
    , Pattern(pattern)
{ }

void TStreamLogWriter::Write(const TLogEvent& event)
{
    *Stream << FormatEvent(event, Pattern) << Endl;
}

void TStreamLogWriter::Flush()
{
    Stream->Flush();
}

void TStreamLogWriter::Reload()
{ }

////////////////////////////////////////////////////////////////////////////////

TStdErrLogWriter::TStdErrLogWriter(const Stroka& pattern)
    : TStreamLogWriter(&StdErrStream(), pattern)
{ }

////////////////////////////////////////////////////////////////////////////////

TStdOutLogWriter::TStdOutLogWriter(const Stroka& pattern)
    : TStreamLogWriter(&StdOutStream(), pattern)
{ }

////////////////////////////////////////////////////////////////////////////////

TFileLogWriter::TFileLogWriter(
    Stroka fileName,
    Stroka pattern)
    : FileName(fileName)
    , Pattern(pattern)
    , Initialized(false)
{
    EnsureInitialized();
}

void TFileLogWriter::EnsureInitialized()
{
    if (Initialized)
        return;

    try {
        NFS::ForcePath(NFS::GetDirectoryName(FileName));
        File.reset(new TFile(FileName, OpenAlways|ForAppend|WrOnly|Seq|CloseOnExec));
        FileOutput.reset(new TBufferedFileOutput(*File, BufferSize));
        FileOutput->SetFinishPropagateMode(true);
        *FileOutput << Endl;
    } catch (const std::exception& ex) {
        LOG_ERROR(ex, "Error opening log file: %s", ~FileName);
        // Still let's pretend we're initialized to avoid subsequent attempts.
        Initialized = true;
        return;
    }

    LogWriter = New<TStreamLogWriter>(~FileOutput, Pattern);
    LogWriter->Write(GetBannerEvent());

    Initialized = true;
}

void TFileLogWriter::Write(const TLogEvent& event)
{
    if (LogWriter) {
        try {
            LogWriter->Write(event);
        } catch (...) {
            // I wish I could log it, but I'm a logger myself.
        }
    }
}

void TFileLogWriter::Flush()
{
    if (LogWriter) {
        try {
            LogWriter->Flush();
        } catch (...) {
        }
    }
}

void TFileLogWriter::Reload()
{
    Flush();

    if (~File) {
        try {
            File->Close();
        } catch (...) {
        }
    }

    Initialized = false;
    EnsureInitialized();
}

////////////////////////////////////////////////////////////////////////////////

TRawFileLogWriter::TRawFileLogWriter(const Stroka& fileName)
    : FileName(fileName)
    , Initialized(false)
    , Buffer(new TMessageBuffer())
{
    EnsureInitialized();
}

void TRawFileLogWriter::EnsureInitialized()
{
    if (Initialized)
        return;

    try {
        NFS::ForcePath(NFS::GetDirectoryName(FileName));
        File.reset(new TFile(FileName, OpenAlways|ForAppend|WrOnly|Seq|CloseOnExec));
        FileOutput.reset(new TBufferedFileOutput(*File, BufferSize));
        FileOutput->SetFinishPropagateMode(true);
        *FileOutput << Endl;
    } catch (const std::exception& ex) {
        LOG_ERROR(ex, "Error opening log file: %s", ~FileName);
        // Still let's pretend we're initialized to avoid subsequent attempts.
    }

    Initialized = true;

    Write(GetBannerEvent());
}

void TRawFileLogWriter::Write(const TLogEvent& event)
{
    if (!FileOutput) {
        return;
    }

    auto* buffer = ~Buffer;
    buffer->Reset();

    FormatDateTime(buffer, event.DateTime);
    buffer->AppendChar('\t');
    FormatLevel(~Buffer, event.Level);
    buffer->AppendChar('\t');
    buffer->AppendString(~event.Category);
    buffer->AppendChar('\t');
    FormatMessage(buffer, event.Message);
    buffer->AppendChar('\t');
    if (event.FileName) {
        buffer->AppendString(event.FileName);
    }
    buffer->AppendChar('\t');
    if (event.Line >= 0) {
        buffer->AppendNumber(event.Line);
    }
    buffer->AppendChar('\t');
    if (event.Function) {
        buffer->AppendString(event.Function);
    }
    buffer->AppendChar('\t');
    if (event.ThreadId != 0) {
        buffer->AppendNumber(event.ThreadId, 16);
    }
    buffer->AppendChar('\n');

    FileOutput->Write(buffer->GetData(), buffer->GetBytesWritten());
}

void TRawFileLogWriter::Flush()
{
    if (~FileOutput) {
        try {
            FileOutput->Flush();
        } catch (...) {
            // Silently ignore FS errors.
        }
    }
}

void TRawFileLogWriter::Reload()
{
    Flush();

    if (~File) {
        try {
            File->Close();
        } catch (...) {
        }
    }

    Initialized = false;
    EnsureInitialized();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NLog
} // namespace NYT
