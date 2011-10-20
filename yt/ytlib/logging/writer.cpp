#include "../misc/stdafx.h"
#include "writer.h"
#include "log.h"

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
        File.Reset(new TFile(FileName, OpenAlways|ForAppend|WrOnly|Seq));
        FileOutput.Reset(new TBufferedFileOutput(*File, BufferSize));
    } catch (yexception& e) {
        LOG_ERROR("Error opening log file %s: %s",
            ~FileName,
            e.what());
        // Still let's pretend we're initialized to avoid subsequent attempts.
        Initialized = true;
        return;
    }

    LogWriter = ~New<TStreamLogWriter>(~FileOutput, Pattern);
    LogWriter->Write(TLogEvent(
        SystemLoggingCategory,
        ELogLevel::Info,
        "Log file opened"));

    Initialized = true;
}

void TFileLogWriter::Write(const TLogEvent& event)
{
    EnsureInitialized();
    if (~LogWriter != NULL) {
        LogWriter->Write(event);
    }
}

void TFileLogWriter::Flush()
{
    if (~LogWriter != NULL) {
        LogWriter->Flush();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NLog
} // namespace NYT
