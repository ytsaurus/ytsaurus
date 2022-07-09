#include "config.h"
#include "private.h"
#include "file_log_writer.h"
#include "stream_log_writer.h"

#include <util/string/vector.h>
#include <util/system/env.h>

#include <library/cpp/iterator/functools.h>

namespace NYT::NLogging {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

std::optional<ELogLevel> GetLogLevelFromEnv()
{
    auto logLevelStr = GetEnv("YT_LOG_LEVEL");
    if (logLevelStr.empty()) {
        return {};
    }

    // This handles most typical casings like "DEBUG", "debug", "Debug".
    logLevelStr.to_title();
    return TEnumTraits<ELogLevel>::FromString(logLevelStr);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TFileLogWriterConfig::TFileLogWriterConfig()
{
    RegisterParameter("file_name", FileName);
    RegisterParameter("enable_compression", EnableCompression)
        .Default(false);
    RegisterParameter("compression_method", CompressionMethod)
        .Default(ECompressionMethod::Gzip);
    RegisterParameter("compression_level", CompressionLevel)
        .Default(6);

    RegisterPostprocessor([&] {
        if (CompressionMethod == ECompressionMethod::Gzip && (CompressionLevel < 0 || CompressionLevel > 9)) {
            THROW_ERROR_EXCEPTION("Invalid \"compression_level\" attribute for \"gzip\" compression method");
        } else if (CompressionMethod == ECompressionMethod::Zstd && CompressionLevel > 22) {
            THROW_ERROR_EXCEPTION("Invalid \"compression_level\" attribute for \"zstd\" compression method");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

TLogWriterConfig::TLogWriterConfig()
{
    RegisterParameter("type", Type);
    RegisterParameter("format", Format)
        .Alias("accepted_message_format")
        .Default(ELogFormat::PlainText);
    RegisterParameter("rate_limit", RateLimit)
        .Default();
    RegisterParameter("common_fields", CommonFields)
        .Default();
    RegisterParameter("enable_system_messages", EnableSystemMessages)
        .Alias("enable_control_messages")
        .Default();
    RegisterParameter("enable_source_location", EnableSourceLocation)
        .Default(false);
    RegisterParameter("json_format", JsonFormat)
        .Default(nullptr);

    RegisterPostprocessor([&] {
        // COMPAT(max42).
        if (Format == ELogFormat::Structured) {
            Format = ELogFormat::Json;
        }
    });
}

ELogFamily TLogWriterConfig::GetFamily() const
{
    return Format == ELogFormat::PlainText ? ELogFamily::PlainText : ELogFamily::Structured;
}

bool TLogWriterConfig::AreSystemMessagesEnabled() const
{
    return EnableSystemMessages.value_or(GetFamily() == ELogFamily::PlainText);
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
        .GreaterThanOrEqual(0)
        .Default(5_GB);
    RegisterParameter("high_backlog_watermark", HighBacklogWatermark)
        .GreaterThanOrEqual(0)
        .Default(10'000'000);
    RegisterParameter("low_backlog_watermark", LowBacklogWatermark)
        .GreaterThanOrEqual(0)
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

    RegisterParameter("compression_thread_count", CompressionThreadCount)
        .Default(1);

    RegisterPostprocessor([&] {
        THashMap<TString, ELogFamily> writerNameToFamily;
        for (const auto& [writerName, writerConfig] : Writers) {
            try {
                auto typedWriterConfig = ConvertTo<TLogWriterConfigPtr>(writerConfig);
                EmplaceOrCrash(writerNameToFamily, writerName, typedWriterConfig->GetFamily());
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Malformed configuration of writer %Qv",
                    writerName)
                    << ex;
            }
        }

        for (const auto& [ruleIndex, rule] : Enumerate(Rules)) {
            for (const auto& writerName : rule->Writers) {
                auto it = writerNameToFamily.find(writerName);
                if (it == writerNameToFamily.end()) {
                    THROW_ERROR_EXCEPTION("Unknown writer %Qv", writerName);
                }
                if (rule->Family != it->second) {
                    THROW_ERROR_EXCEPTION("Writer %Qv has family %Qlv while rule %v has family %Qlv",
                        writerName,
                        it->second,
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
    mergedConfig->CompressionThreadCount = dynamicConfig->CompressionThreadCount.value_or(CompressionThreadCount);
    mergedConfig->Postprocess();
    return mergedConfig;
}

TLogManagerConfigPtr TLogManagerConfig::CreateLogFile(const TString& path)
{
    auto rule = New<TRuleConfig>();
    rule->MinLevel = ELogLevel::Trace;
    rule->Writers.push_back(TString(DefaultFileWriterName));

    auto writerConfig = New<TLogWriterConfig>();
    writerConfig->Type = TFileLogWriterConfig::Type;

    auto fileWriterConfig = New<TFileLogWriterConfig>();
    fileWriterConfig->FileName = path;

    auto config = New<TLogManagerConfig>();
    config->Rules.push_back(rule);
    EmplaceOrCrash(config->Writers, DefaultFileWriterName, writerConfig->BuildFullConfig(fileWriterConfig));
    config->MinDiskSpace = 0;
    config->HighBacklogWatermark = 100'000;
    config->LowBacklogWatermark = 100'000;

    config->Postprocess();
    return config;
}

TLogManagerConfigPtr TLogManagerConfig::CreateStderrLogger(ELogLevel logLevel)
{
    auto rule = New<TRuleConfig>();
    rule->MinLevel = logLevel;
    rule->Writers.push_back(TString(DefaultStderrWriterName));

    auto writerConfig = New<TLogWriterConfig>();
    writerConfig->Type = TStderrLogWriterConfig::Type;

    auto stderrWriterConfig = New<TStderrLogWriterConfig>();

    auto config = New<TLogManagerConfig>();
    config->Rules.push_back(rule);
    config->Writers.emplace(DefaultStderrWriterName, writerConfig->BuildFullConfig(stderrWriterConfig));
    config->MinDiskSpace = 0;
    config->HighBacklogWatermark = 100'000;
    config->LowBacklogWatermark = 100'000;

    config->Postprocess();
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

    config->Postprocess();
    return config;
}

TLogManagerConfigPtr TLogManagerConfig::CreateYTServer(
    const TString& componentName,
    const TString& directory,
    const THashMap<TString, TString>& structuredCategoryToWriterName)
{
    auto config = New<TLogManagerConfig>();

    auto logLevel = GetLogLevelFromEnv().value_or(ELogLevel::Debug);

    static const std::vector logLevels{
        ELogLevel::Trace,
        ELogLevel::Debug,
        ELogLevel::Info,
        ELogLevel::Error
    };

    for (auto currentLogLevel : logLevels) {
        if (currentLogLevel < logLevel) {
            continue;
        }

        auto rule = New<TRuleConfig>();
        // Due to historical reasons, error logs usually contain warning messages.
        rule->MinLevel = currentLogLevel == ELogLevel::Error ? ELogLevel::Warning : currentLogLevel;
        rule->Writers.push_back(ToString(currentLogLevel));

        auto fileName = Format(
            "%v/%v%v.log",
            directory,
            componentName,
            currentLogLevel == ELogLevel::Info ? "" : "." + FormatEnum(currentLogLevel));

        auto writerConfig = New<TLogWriterConfig>();
        writerConfig->Type = TFileLogWriterConfig::Type;

        auto fileWriterConfig = New<TFileLogWriterConfig>();
        fileWriterConfig->FileName = Format(
            "%v/%v%v.log",
            directory,
            componentName,
            currentLogLevel == ELogLevel::Info ? "" : "." + FormatEnum(currentLogLevel));

        config->Rules.push_back(rule);
        config->Writers.emplace(ToString(currentLogLevel), writerConfig->BuildFullConfig(fileWriterConfig));
    }

    for (const auto& [category, writerName] : structuredCategoryToWriterName) {
        auto rule = New<TRuleConfig>();
        rule->MinLevel = ELogLevel::Info;
        rule->Writers.emplace_back(writerName);
        rule->Family = ELogFamily::Structured;
        rule->IncludeCategories = {category};

        auto writerConfig = New<TLogWriterConfig>();
        writerConfig->Type = TFileLogWriterConfig::Type;
        writerConfig->Format = ELogFormat::Yson;

        auto fileWriterConfig = New<TFileLogWriterConfig>();
        fileWriterConfig->FileName = Format(
            "%v/%v.yson.%v.log",
            directory,
            componentName,
            writerName);

        config->Rules.push_back(rule);
        config->Writers.emplace(writerName, writerConfig->BuildFullConfig(fileWriterConfig));
    }

    config->Postprocess();
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
    auto logLevel = GetLogLevelFromEnv();
    if (!logLevel) {
        return nullptr;
    }

    auto logExcludeCategoriesStr = GetEnv("YT_LOG_EXCLUDE_CATEGORIES");
    auto logIncludeCategoriesStr = GetEnv("YT_LOG_INCLUDE_CATEGORIES");

    auto rule = New<TRuleConfig>();
    rule->Writers.push_back(TString(DefaultStderrWriterName));
    rule->MinLevel = *logLevel;

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

    auto writerConfig = New<TLogWriterConfig>();
    writerConfig->Type = TStderrLogWriterConfig::Type;

    auto stderrWriterConfig = New<TStderrLogWriterConfig>();

    auto config = New<TLogManagerConfig>();
    config->Rules.push_back(std::move(rule));
    config->MinDiskSpace = 0;
    config->HighBacklogWatermark = std::numeric_limits<int>::max();
    config->LowBacklogWatermark = 0;
    EmplaceOrCrash(config->Writers, DefaultStderrWriterName, writerConfig->BuildFullConfig(stderrWriterConfig));

    config->Postprocess();
    return config;
}

void TLogManagerConfig::UpdateWriters(
    const std::function<IMapNodePtr(const IMapNodePtr&)> updater)
{
    THashMap<TString, IMapNodePtr> updatedWriters;
    for (const auto& [name, configNode] : Writers) {
        if (auto updatedConfigNode = updater(configNode)) {
            EmplaceOrCrash(updatedWriters, name, updatedConfigNode);
        }
    }
    Writers = std::move(updatedWriters);
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

    RegisterParameter("compression_thread_count", CompressionThreadCount)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
