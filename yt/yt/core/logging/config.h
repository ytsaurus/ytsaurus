#pragma once

#include "public.h"

#include <yt/yt/core/ytree/public.h>
#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

class TWriterConfig
    : public NYTree::TYsonSerializable
{
public:
    EWriterType Type;
    TString FileName;
    ELogFormat Format;
    std::optional<size_t> RateLimit;
    bool EnableCompression;
    ECompressionMethod CompressionMethod;
    int CompressionLevel;
    THashMap<TString, NYTree::INodePtr> CommonFields;
    bool EnableSystemMessages;
    bool EnableSourceLocation;

    ELogFamily GetFamily() const;

    TWriterConfig();
};

DEFINE_REFCOUNTED_TYPE(TWriterConfig)

////////////////////////////////////////////////////////////////////////////////

class TRuleConfig
    : public NYTree::TYsonSerializable
{
public:
    std::optional<THashSet<TString>> IncludeCategories;
    THashSet<TString> ExcludeCategories;

    ELogLevel MinLevel;
    ELogLevel MaxLevel;

    ELogFamily Family;

    std::vector<TString> Writers;

    TRuleConfig();

    bool IsApplicable(TStringBuf category, ELogFamily family) const;
    bool IsApplicable(TStringBuf category, ELogLevel level, ELogFamily family) const;
};

DEFINE_REFCOUNTED_TYPE(TRuleConfig)

////////////////////////////////////////////////////////////////////////////////

class TLogManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    std::optional<TDuration> FlushPeriod;
    std::optional<TDuration> WatchPeriod;
    std::optional<TDuration> CheckSpacePeriod;

    i64 MinDiskSpace;

    int HighBacklogWatermark;
    int LowBacklogWatermark;

    TDuration ShutdownGraceTimeout;

    std::vector<TRuleConfigPtr> Rules;
    THashMap<TString, TWriterConfigPtr> Writers;
    std::vector<TString> SuppressedMessages;
    THashMap<TString, size_t> CategoryRateLimits;

    TDuration RequestSuppressionTimeout;

    bool AbortOnAlert;

    TLogManagerConfig();

    TLogManagerConfigPtr ApplyDynamic(const TLogManagerDynamicConfigPtr& dynamicConfig) const;

    static TLogManagerConfigPtr CreateStderrLogger(ELogLevel logLevel);
    static TLogManagerConfigPtr CreateLogFile(const TString& path);
    static TLogManagerConfigPtr CreateDefault();
    static TLogManagerConfigPtr CreateQuiet();
    static TLogManagerConfigPtr CreateSilent();
    //! Create logging config a-la YT server config: ./#componentName{,.debug,.error}.log.
    static TLogManagerConfigPtr CreateYtServer(const TString& componentName, const TString& directory = ".");
    static TLogManagerConfigPtr CreateFromFile(const TString& file, const NYPath::TYPath& path = "");
    static TLogManagerConfigPtr CreateFromNode(NYTree::INodePtr node, const NYPath::TYPath& path = "");
    static TLogManagerConfigPtr TryCreateFromEnv();
};

DEFINE_REFCOUNTED_TYPE(TLogManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TLogManagerDynamicConfig
    : public NYTree::TYsonSerializable
{
public:
    std::optional<i64> MinDiskSpace;

    std::optional<int> HighBacklogWatermark;
    std::optional<int> LowBacklogWatermark;

    std::optional<std::vector<TRuleConfigPtr>> Rules;
    std::optional<std::vector<TString>> SuppressedMessages;
    std::optional<THashMap<TString, size_t>> CategoryRateLimits;

    std::optional<TDuration> RequestSuppressionTimeout;

    std::optional<bool> AbortOnAlert;

    TLogManagerDynamicConfig();
};

DEFINE_REFCOUNTED_TYPE(TLogManagerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
