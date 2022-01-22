#include "yt_log.h"

#include "log.h"
#include "logger.h"

#include <util/generic/guid.h>

namespace NYT {

using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

namespace {

class TLogManager
    : public ILogManager
{
public:
    static constexpr TStringBuf CategoryName = "Wrapper";

public:
    void RegisterStaticAnchor(
        TLoggingAnchor* position,
        ::TSourceLocation sourceLocation,
        TStringBuf anchorMessage) override
    {
        position->SourceLocation = sourceLocation;
        position->AnchorMessage = anchorMessage;
        position->Enabled.store(true);
        position->Registered.store(true);
    }

    void UpdateAnchor(TLoggingAnchor* /*position*/) override
    { }

    void Enqueue(TLogEvent&& event) override
    {
        auto message = TString(event.Message.begin(), event.Message.end());
        LogMessage(
            ToImplLevel(event.Level),
            ::TSourceLocation(event.SourceFile, event.SourceLine),
            "%.*s",
            event.Message.size(),
            event.Message.begin());
    }

    const TLoggingCategory* GetCategory(TStringBuf categoryName) override
    {
        Y_VERIFY(categoryName == CategoryName);
        return &Category_;
    }

    void UpdateCategory(TLoggingCategory* /*category*/) override
    {
        Y_FAIL();
    }

    bool GetAbortOnAlert() const override
    {
        return false;
    }

private:
    static ILogger::ELevel ToImplLevel(ELogLevel level)
    {
        switch (level) {
            case ELogLevel::Minimum:
            case ELogLevel::Trace:
            case ELogLevel::Debug:
                return ILogger::ELevel::DEBUG;
            case ELogLevel::Info:
                return ILogger::ELevel::INFO;
            case ELogLevel::Warning:
            case ELogLevel::Error:
                return ILogger::ELevel::ERROR;
            case ELogLevel::Alert:
            case ELogLevel::Fatal:
            case ELogLevel::Maximum:
                return ILogger::ELevel::FATAL;
        }
    }

private:
    std::atomic<int> ActualVersion_{1};
    const TLoggingCategory Category_{
        .Name{CategoryName},
        .MinPlainTextLevel{ELogLevel::Minimum},
        .CurrentVersion{1},
        .ActualVersion = &ActualVersion_,
    };
};

TLogManager LogManager;

} // namespace

////////////////////////////////////////////////////////////////////////////////

TLogger Logger(&LogManager, TLogManager::CategoryName);

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TGUID& value, TStringBuf /*format*/)
{
    builder->AppendString(GetGuidAsString(value));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
