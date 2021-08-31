#include "config.h"
#include "private.h"

#include <util/string/vector.h>

#include <library/cpp/iterator/functools.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

TWriterConfig::TWriterConfig()
{
    RegisterParameter("type", Type);
    RegisterParameter("file_name", FileName)
        .Default();
    RegisterParameter("format", Format)
        .Alias("accepted_message_format")
        .Default(ELogFormat::PlainText);
    RegisterParameter("rate_limit", RateLimit)
        .Default();
    RegisterParameter("enable_compression", EnableCompression)
        .Default(false);
    RegisterParameter("compression_method", CompressionMethod)
        .Default(ECompressionMethod::Gzip);
    RegisterParameter("compression_level", CompressionLevel)
        .Default(6);
    RegisterParameter("common_fields", CommonFields)
        .Default();
    RegisterParameter("enable_system_messages", EnableSystemMessages)
        .Alias("enable_control_messages")
        .Default(true);
    RegisterParameter("enable_source_location", EnableSourceLocation)
        .Default(false);

    RegisterPostprocessor([&] () {
        if (Type == EWriterType::File && FileName.empty()) {
            THROW_ERROR_EXCEPTION("Missing \"file_name\" attribute for \"file\" writer");
        } else if (Type != EWriterType::File && !FileName.empty()) {
            THROW_ERROR_EXCEPTION("Unused \"file_name\" attribute for %Qlv writer", Type);
        }

        if (CompressionMethod == ECompressionMethod::Gzip && (CompressionLevel < 0 || CompressionLevel > 9)) {
            THROW_ERROR_EXCEPTION("Invalid \"compression_level\" attribute for \"gzip\" compression method");
        } else if (CompressionMethod == ECompressionMethod::Zstd && CompressionLevel > 22) {
            THROW_ERROR_EXCEPTION("Invalid \"compression_level\" attribute for \"zstd\" compression method");
        }

        // COMPAT(max42).
        if (Format == ELogFormat::Structured) {
            Format = ELogFormat::Json;
        }
    });
}

ELogFamily TWriterConfig::GetFamily() const
{
    return Format == ELogFormat::PlainText ? ELogFamily::PlainText : ELogFamily::Structured;
}

////////////////////////////////////////////////////////////////////////////////

TRuleConfig::TRuleConfig()
{
    RegisterParameter("include_categories", IncludeCategories)
        .Default();
    RegisterParameter("exclude_categories", ExcludeCategories)
        .Default();
    RegisterParameter("min_level", MinLevel)
        .Default(ELogLevel::Minimum);
    RegisterParameter("max_level", MaxLevel)
        .Default(ELogLevel::Maximum);
    RegisterParameter("family", Family)
        .Alias("message_format")
        .Default(ELogFamily::PlainText);
    RegisterParameter("writers", Writers)
        .NonEmpty();
}

bool TRuleConfig::IsApplicable(TStringBuf category, ELogFamily family) const
{
    return
        Family == family &&
        ExcludeCategories.find(category) == ExcludeCategories.end() &&
        (!IncludeCategories || IncludeCategories->find(category) != IncludeCategories->end());
}

bool TRuleConfig::IsApplicable(TStringBuf category, ELogLevel level, ELogFamily family) const
{
    return
        IsApplicable(category, family) &&
        MinLevel <= level && level <= MaxLevel;
}


////////////////////////////////////////////////////////////////////////////////

TLogManagerConfig::TLogManagerConfig()
{
    RegisterParameter("flush_period", FlushPeriod)
        .Default();
    RegisterParameter("watch_period", WatchPeriod)
        .Default();
    RegisterParameter("check_space_period", CheckSpacePeriod)
        .Default();
    RegisterParameter("min_disk_space", MinDiskSpace)
        .GreaterThanOrEqual(1_GB)
        .Default(5_GB);
    RegisterParameter("high_backlog_watermark", HighBacklogWatermark)
        .GreaterThan(0)
        .Default(10'000'000);
    RegisterParameter("low_backlog_watermark", LowBacklogWatermark)
        .GreaterThan(0)
        .Default(1'000'000);
    RegisterParameter("shutdown_grace_timeout", ShutdownGraceTimeout)
        .Default(TDuration::Seconds(1));

    RegisterParameter("writers", Writers);
    RegisterParameter("rules", Rules);
    RegisterParameter("suppressed_messages", SuppressedMessages)
        .Default();
    RegisterParameter("category_rate_limits", CategoryRateLimits)
        .Default();

    RegisterParameter("request_suppression_timeout", RequestSuppressionTimeout)
        .Alias("trace_suppression_timeout")
        .Default(TDuration::Zero());

    RegisterParameter("enable_anchor_profiling", EnableAnchorProfiling)
        .Default(false);
    RegisterParameter("min_logged_message_rate_to_profile", MinLoggedMessageRateToProfile)
        .Default(1.0);

    RegisterParameter("abort_on_alert", AbortOnAlert)
        .Default(false);

    RegisterPostprocessor([&] () {
        for (const auto& [ruleIndex, rule] : Enumerate(Rules)) {
            for (const TString& writer : rule->Writers) {
                auto it = Writers.find(writer);
                if (it == Writers.end()) {
                    THROW_ERROR_EXCEPTION("Unknown writer %Qv", writer);
                }
                if (rule->Family != it->second->GetFamily()) {
                    THROW_ERROR_EXCEPTION("Writer %Qv is from family %Qlv while rule %v is from family %Qlv",
                        writer,
                        it->second->GetFamily(),
                        ruleIndex,
                        rule->Family);
                }
            }
        }
    });
}

TLogManagerConfigPtr TLogManagerConfig::ApplyDynamic(const TLogManagerDynamicConfigPtr& dynamicConfig) const
{
    auto mergedConfig = New<TLogManagerConfig>();
    mergedConfig->FlushPeriod = FlushPeriod;
    mergedConfig->WatchPeriod = WatchPeriod;
    mergedConfig->CheckSpacePeriod = CheckSpacePeriod;
    mergedConfig->MinDiskSpace = dynamicConfig->MinDiskSpace.value_or(MinDiskSpace);
    mergedConfig->HighBacklogWatermark = dynamicConfig->HighBacklogWatermark.value_or(HighBacklogWatermark);
    mergedConfig->LowBacklogWatermark = dynamicConfig->LowBacklogWatermark.value_or(LowBacklogWatermark);
    mergedConfig->ShutdownGraceTimeout = ShutdownGraceTimeout;
    mergedConfig->Rules = CloneYsonSerializables(dynamicConfig->Rules.value_or(Rules));
    mergedConfig->Writers = CloneYsonSerializables(Writers);
    mergedConfig->SuppressedMessages = dynamicConfig->SuppressedMessages.value_or(SuppressedMessages);
    mergedConfig->CategoryRateLimits = dynamicConfig->CategoryRateLimits.value_or(CategoryRateLimits);
    mergedConfig->RequestSuppressionTimeout = dynamicConfig->RequestSuppressionTimeout.value_or(RequestSuppressionTimeout);
    mergedConfig->EnableAnchorProfiling = dynamicConfig->EnableAnchorProfiling.value_or(EnableAnchorProfiling);
    mergedConfig->MinLoggedMessageRateToProfile = dynamicConfig->MinLoggedMessageRateToProfile.value_or(MinLoggedMessageRateToProfile);
    mergedConfig->AbortOnAlert = dynamicConfig->AbortOnAlert.value_or(AbortOnAlert);
    mergedConfig->Postprocess();
    return mergedConfig;
}

TLogManagerConfigPtr TLogManagerConfig::CreateLogFile(const TString& path)
{
    auto rule = New<TRuleConfig>();
    rule->MinLevel = ELogLevel::Trace;
    rule->Writers.push_back("FileWriter");

    auto fileWriterConfig = New<TWriterConfig>();
    fileWriterConfig->Type = EWriterType::File;
    fileWriterConfig->FileName = path;

    auto config = New<TLogManagerConfig>();
    config->Rules.push_back(rule);
    config->Writers.emplace("FileWriter", fileWriterConfig);

    config->MinDiskSpace = 0;
    config->HighBacklogWatermark = 100000;
    config->LowBacklogWatermark = 100000;

    return config;
}

TLogManagerConfigPtr TLogManagerConfig::CreateStderrLogger(ELogLevel logLevel)
{
    auto rule = New<TRuleConfig>();
    rule->MinLevel = logLevel;
    rule->Writers.push_back(TString(DefaultStderrWriterName));

    auto stderrWriterConfig = New<TWriterConfig>();
    stderrWriterConfig->Type = EWriterType::Stderr;

    auto config = New<TLogManagerConfig>();
    config->Rules.push_back(rule);
    config->Writers.emplace(TString(DefaultStderrWriterName), stderrWriterConfig);

    config->MinDiskSpace = 0;
    config->HighBacklogWatermark = 100000;
    config->LowBacklogWatermark = 100000;

    return config;
}

TLogManagerConfigPtr TLogManagerConfig::CreateDefault()
{
    return CreateStderrLogger(DefaultStderrMinLevel);
}

TLogManagerConfigPtr TLogManagerConfig::CreateQuiet()
{
    return CreateStderrLogger(DefaultStderrQuietLevel);
}

TLogManagerConfigPtr TLogManagerConfig::CreateSilent()
{
    auto config = New<TLogManagerConfig>();

    config->MinDiskSpace = 0;
    config->HighBacklogWatermark = 0;
    config->LowBacklogWatermark = 0;

    return config;
}

TLogManagerConfigPtr TLogManagerConfig::CreateYtServer(const TString& componentName, const TString& directory, const THashMap<TString, TString>& structuredCategoryToWriterName)
{
    auto config = New<TLogManagerConfig>();

    std::vector<ELogLevel> levels = {ELogLevel::Debug, ELogLevel::Info, ELogLevel::Error};
    if (getenv("YT_ENABLE_TRACE_LOGGING")) {
        levels.push_back(ELogLevel::Trace);
    }

    for (const auto& logLevel : levels) {
        auto rule = New<TRuleConfig>();
        rule->MinLevel = logLevel;

        // Due to historic reasons, error logs usually contain warning messages.
        if (logLevel == ELogLevel::Error) {
            rule->MinLevel = ELogLevel::Warning;
        }

        rule->Writers.push_back(ToString(logLevel));

        auto fileWriterConfig = New<TWriterConfig>();
        fileWriterConfig->Type = EWriterType::File;
        fileWriterConfig->FileName = Format(
            "%v/%v%v.log",
            directory,
            componentName,
            logLevel == ELogLevel::Info ? "" : "." + FormatEnum(logLevel));

        config->Rules.push_back(rule);
        config->Writers.emplace(ToString(logLevel), fileWriterConfig);
    }

    for (const auto& [category, writerName] : structuredCategoryToWriterName) {
        auto rule = New<TRuleConfig>();
        rule->MinLevel = ELogLevel::Info;
        rule->Writers.emplace_back(writerName);
        rule->Family = ELogFamily::Structured;
        rule->IncludeCategories = {category};

        auto fileWriterConfig = New<TWriterConfig>();
        fileWriterConfig->Type = EWriterType::File;
        fileWriterConfig->FileName = Format(
            "%v/%v.yson.%v.log",
            directory,
            componentName,
            writerName);
        fileWriterConfig->Format = ELogFormat::Yson;

        config->Rules.push_back(rule);
        config->Writers.emplace(writerName, fileWriterConfig);
    }

    return config;
}

TLogManagerConfigPtr TLogManagerConfig::CreateFromFile(const TString& file, const NYPath::TYPath& path)
{
    NYTree::INodePtr node;
    {
        TIFStream stream(file);
        node = NYTree::ConvertToNode(&stream);
    }
    return CreateFromNode(std::move(node), path);
}

TLogManagerConfigPtr TLogManagerConfig::CreateFromNode(NYTree::INodePtr node, const NYPath::TYPath& path)
{
    auto config = New<TLogManagerConfig>();
    config->Load(node, true, true, path);
    return config;
}

TLogManagerConfigPtr TLogManagerConfig::TryCreateFromEnv()
{
    const auto* logLevelStr = getenv("YT_LOG_LEVEL");
    if (!logLevelStr) {
        return nullptr;
    }

    const char* logExcludeCategoriesStr = getenv("YT_LOG_EXCLUDE_CATEGORIES");
    const char* logIncludeCategoriesStr = getenv("YT_LOG_INCLUDE_CATEGORIES");

    const char* const stderrWriterName = "stderr";

    auto rule = New<TRuleConfig>();
    rule->Writers.push_back(stderrWriterName);
    rule->MinLevel = ELogLevel::Fatal;

    if (logLevelStr) {
        TString logLevel = logLevelStr;
        if (!logLevel.empty()) {
            // This handles most typical casings like "DEBUG", "debug", "Debug".
            logLevel.to_title();
            rule->MinLevel = TEnumTraits<ELogLevel>::FromString(logLevel);
        }
    }

    std::vector<TString> logExcludeCategories;
    if (logExcludeCategoriesStr) {
        logExcludeCategories = SplitString(logExcludeCategoriesStr, ",");
    }

    for (const auto& excludeCategory : logExcludeCategories) {
        rule->ExcludeCategories.insert(excludeCategory);
    }

    std::vector<TString> logIncludeCategories;
    if (logIncludeCategoriesStr) {
        logIncludeCategories = SplitString(logIncludeCategoriesStr, ",");
    }

    if (!logIncludeCategories.empty()) {
        rule->IncludeCategories.emplace();
        for (const auto& includeCategory : logIncludeCategories) {
            rule->IncludeCategories->insert(includeCategory);
        }
    }

    auto config = New<TLogManagerConfig>();
    config->Rules.push_back(std::move(rule));

    config->MinDiskSpace = 0;
    config->HighBacklogWatermark = std::numeric_limits<int>::max();
    config->LowBacklogWatermark = 0;

    auto stderrWriter = New<TWriterConfig>();
    stderrWriter->Type = EWriterType::Stderr;

    config->Writers.emplace(stderrWriterName, std::move(stderrWriter));

    return config;
}

////////////////////////////////////////////////////////////////////////////////

TLogManagerDynamicConfig::TLogManagerDynamicConfig()
{
    RegisterParameter("min_disk_space", MinDiskSpace)
        .Optional();
    RegisterParameter("high_backlog_watermark", HighBacklogWatermark)
        .Optional();
    RegisterParameter("low_backlog_watermark", LowBacklogWatermark)
        .Optional();

    RegisterParameter("rules", Rules)
        .Optional();
    RegisterParameter("suppressed_messages", SuppressedMessages)
        .Optional();
    RegisterParameter("category_rate_limits", CategoryRateLimits)
        .Optional();

    RegisterParameter("request_suppression_timeout", RequestSuppressionTimeout)
        .Optional();

    RegisterParameter("enable_anchor_profiling", EnableAnchorProfiling)
        .Optional();
    RegisterParameter("min_logged_message_rate_to_profile", MinLoggedMessageRateToProfile)
        .Optional();

    RegisterParameter("abort_on_alert", AbortOnAlert)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
