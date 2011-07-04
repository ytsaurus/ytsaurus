#include "writer.h"
#include "log.h"

#include "../misc/pattern_formatter.h"

#include <quality/util/file_utils.h>

namespace NYT {
namespace NLog {

////////////////////////////////////////////////////////////////////////////////

const char* const SystemLoggingCategory = "Logging";
static TLogger Logger(SystemLoggingCategory);

////////////////////////////////////////////////////////////////////////////////

namespace {

Stroka FormatDateTime(TInstant dateTime)
{
    timeval time1 = dateTime.TimeVal();
    tm time2;
    dateTime.LocalTime(&time2);
    return Sprintf("%04d-%02d-%02d %02d:%02d:%02d,%03d",
                   time2.tm_year + 1900,
                   time2.tm_mon + 1,
                   time2.tm_mday,
                   time2.tm_hour,
                   time2.tm_min,
                   time2.tm_sec,
                   (int) time1.tv_usec / 1000);
}

Stroka FormatLevel(ELogLevel level)
{
    switch (level)
    {
        case ELogLevel::Debug:   return "D";
        case ELogLevel::Info:    return "I";
        case ELogLevel::Warning: return "W";
        case ELogLevel::Error:   return "E";
        case ELogLevel::Fatal:   return "F";
        default: YASSERT(false); return "?";
    }
}

void SetupFormatter(TPatternFormatter& formatter, const TLogEvent& event)
{
    formatter.AddProperty("level", FormatLevel(event.GetLevel()));
    formatter.AddProperty("datetime", FormatDateTime(event.GetDateTime()));
    formatter.AddProperty("message", event.GetMessage());
    formatter.AddProperty("category", event.GetCategory());
    formatter.AddProperty("tab", "\t");

    const TLogEvent::TProperties& properties = event.GetProperties();
    for (TLogEvent::TProperties::const_iterator it = properties.begin();
        it != properties.end();
        ++it)
    {
        formatter.AddProperty(it->First(), it->Second());
    }
}

Stroka FormatEvent(const TLogEvent& event, Stroka pattern)
{
    TPatternFormatter formatter;
    SetupFormatter(formatter, event);
    return formatter.Format(pattern);
}

} // namespace <anonymous>

////////////////////////////////////////////////////////////////////////////////

TLogEvent::TLogEvent(Stroka category, ELogLevel level, Stroka message)
    : Category(category)
    , Level(level)
    , Message(message)
    , DateTime(TInstant::Now())
{ }

void TLogEvent::AddProperty(Stroka name, Stroka value)
{
    Properties.push_back(MakePair(name, value));
}

Stroka TLogEvent::GetCategory() const
{
    return Category;
}

NYT::NLog::ELogLevel TLogEvent::GetLevel() const
{
    return Level;
}

Stroka TLogEvent::GetMessage() const
{
    return Message;
}

TInstant TLogEvent::GetDateTime() const
{
    return DateTime;
}

const TLogEvent::TProperties& TLogEvent::GetProperties() const
{
    return Properties;
}

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

    LogWriter = new TStreamLogWriter(~FileOutput, Pattern);
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
