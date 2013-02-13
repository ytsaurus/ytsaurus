#include "stdafx.h"
#include "writer.h"
#include "log.h"

#include <ytlib/misc/fs.h>

namespace NYT {
namespace NLog {

////////////////////////////////////////////////////////////////////////////////

const char* const SystemLoggingCategory = "Logging";
static TLogger Logger(SystemLoggingCategory);

////////////////////////////////////////////////////////////////////////////////

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
{ }

void TFileLogWriter::EnsureInitialized()
{
    if (Initialized)
        return;

    try {
        NFS::ForcePath(NFS::GetDirectoryName(FileName));
        File.Reset(new TFile(FileName, OpenAlways|ForAppend|WrOnly|Seq|CloseOnExec));
        FileOutput.Reset(new TBufferedFileOutput(*File, BufferSize));
        FileOutput->SetFinishPropagateMode(true);
        *FileOutput << Endl;
    } catch (const std::exception& ex) {
        LOG_ERROR(ex, "Error opening log file: %s", ~FileName);
        // Still let's pretend we're initialized to avoid subsequent attempts.
        Initialized = true;
        return;
    }

    LogWriter = New<TStreamLogWriter>(~FileOutput, Pattern);
    LogWriter->Write(TLogEvent(
        SystemLoggingCategory,
        ELogLevel::Info,
        "Log file opened"));

    Initialized = true;
}

void TFileLogWriter::Write(const TLogEvent& event)
{
    EnsureInitialized();
    if (LogWriter) {
        LogWriter->Write(event);
    }
}

void TFileLogWriter::Flush()
{
    if (LogWriter) {
        LogWriter->Flush();
    }
}

void TFileLogWriter::Reload()
{
    Flush();
    File->Close();
    Initialized = false;
}

////////////////////////////////////////////////////////////////////////////////

TRawFileLogWriter::TRawFileLogWriter(const Stroka& fileName)
    : FileName(fileName)
    , Initialized(false)
    , Buffer(new TMessageBuffer())
{ }

void TRawFileLogWriter::EnsureInitialized()
{
    if (Initialized)
        return;

    try {
        NFS::ForcePath(NFS::GetDirectoryName(FileName));
        File.Reset(new TFile(FileName, OpenAlways|ForAppend|WrOnly|Seq|CloseOnExec));
        FileOutput.Reset(new TBufferedFileOutput(*File, BufferSize));
        FileOutput->SetFinishPropagateMode(true);
        *FileOutput << Endl;
    } catch (const std::exception& ex) {
        LOG_ERROR(ex, "Error opening log file: %s", ~FileName);
        // Still let's pretend we're initialized to avoid subsequent attempts.
    }

    Initialized = true;

    Write(TLogEvent(
        SystemLoggingCategory,
        ELogLevel::Info,
        "Log file opened"));
}

void TRawFileLogWriter::Write(const TLogEvent& event)
{
    EnsureInitialized();

    if (!FileOutput.Get())
        return;

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
        FileOutput->Flush();
    }
}

void TRawFileLogWriter::Reload()
{
    Flush();

    if (~File) {
        File->Close();
    }

    Initialized = false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NLog
} // namespace NYT
