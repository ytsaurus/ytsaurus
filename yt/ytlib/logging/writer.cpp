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

void TStreamLogWriter::Reopen()
{ }

////////////////////////////////////////////////////////////////////////////////

TStdErrLogWriter::TStdErrLogWriter(Stroka pattern)
    : TStreamLogWriter(&StdErrStream(), pattern)
{ }

////////////////////////////////////////////////////////////////////////////////

TStdOutLogWriter::TStdOutLogWriter(Stroka pattern)
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
        File.Reset(new TFile(FileName, OpenAlways|ForAppend|WrOnly|Seq));
        FileOutput.Reset(new TBufferedFileOutput(*File, BufferSize));
        FileOutput->SetFinishPropagateMode(true);
        *FileOutput << Endl;
    } catch (const std::exception& ex) {
        LOG_ERROR("Error opening log file %s\n%s",
            ~FileName.Quote(),
            ex.what());
        // Still let's pretend we're initialized to avoid subsequent attempts.
        Initialized = true;
        return;
    }

    LogWriter = New<TStreamLogWriter>(~FileOutput, Pattern);
    LogWriter->Write(TLogEvent(
        SystemLoggingCategory,
        ELogLevel::Debug,
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

void TFileLogWriter::Reopen()
{
    Flush();
    Initialized = false;
    File->Close();
}

////////////////////////////////////////////////////////////////////////////////

TRawFileLogWriter::TRawFileLogWriter(const Stroka& fileName)
    : FileName(fileName)
    , Initialized(false)
{ }

void TRawFileLogWriter::EnsureInitialized()
{
    if (Initialized)
        return;

    try {
        NFS::ForcePath(NFS::GetDirectoryName(FileName));
        File.Reset(new TFile(FileName, OpenAlways|ForAppend|WrOnly|Seq));
        FileOutput.Reset(new TBufferedFileOutput(*File, BufferSize));
        FileOutput->SetFinishPropagateMode(true);
        *FileOutput << Endl;
    } catch (const std::exception& ex) {
        LOG_ERROR("Error opening log file %s\n%s",
            ~FileName.Quote(),
            ex.what());
        // Still let's pretend we're initialized to avoid subsequent attempts.
    }

    Initialized = true;

    Write(TLogEvent(
        SystemLoggingCategory,
        ELogLevel::Debug,
        "Log file opened"));
}


void TRawFileLogWriter::Write(const TLogEvent& event)
{
    EnsureInitialized();
    if (~FileOutput) {
        *FileOutput
            << FormatDateTime(event.DateTime) << "\t"
            << FormatLevel(event.Level) << "\t"
            << event.Category << "\t"
            << FormatMessage(event.Message) 
            << "\t"
            << event.FileName << "\t"
            << event.Line << "\t"
            << event.Function << "\t"
            << event.ThreadId << Endl;
    }
}

void TRawFileLogWriter::Flush()
{
    if (~FileOutput) {
        FileOutput->Flush();
    }
}

void TRawFileLogWriter::Reopen()
{
    Flush();
    Initialized = false;
    File->Close();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NLog
} // namespace NYT
