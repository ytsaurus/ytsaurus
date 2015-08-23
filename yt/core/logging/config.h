#pragma once

#include "public.h"

#include <core/ytree/public.h>
#include <core/ytree/yson_serializable.h>

namespace NYT {
namespace NLogging {

////////////////////////////////////////////////////////////////////////////////

class TWriterConfig
    : public NYTree::TYsonSerializable
{
public:
    EWriterType Type;
    Stroka FileName;

    TWriterConfig()
    {
        RegisterParameter("type", Type);
        RegisterParameter("file_name", FileName)
            .Default();

        RegisterValidator([&] () {
            if (Type == EWriterType::File && FileName.empty()) {
                THROW_ERROR_EXCEPTION("Missing \"file_name\" attribute for \"file\" writer");
            } else if (Type != EWriterType::File && !FileName.empty()) {
                THROW_ERROR_EXCEPTION("Unused \"file_name\" attribute for %Qlv writer", Type);
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TWriterConfig)

////////////////////////////////////////////////////////////////////////////////

class TRuleConfig
    : public NYTree::TYsonSerializable
{
public:
    TNullable<yhash_set<Stroka>> IncludeCategories;
    yhash_set<Stroka> ExcludeCategories;
    ELogLevel MinLevel;
    ELogLevel MaxLevel;

    std::vector<Stroka> Writers;

    TRuleConfig()
    {
        RegisterParameter("include_categories", IncludeCategories)
            .Default();
        RegisterParameter("exclude_categories", ExcludeCategories)
            .Default();
        RegisterParameter("min_level", MinLevel)
            .Default(ELogLevel::Minimum);
        RegisterParameter("max_level", MaxLevel)
            .Default(ELogLevel::Maximum);
        RegisterParameter("writers", Writers)
            .NonEmpty();
    }

    bool IsApplicable(const Stroka& category) const;
    bool IsApplicable(const Stroka& category, ELogLevel level) const;
};

DEFINE_REFCOUNTED_TYPE(TRuleConfig)

////////////////////////////////////////////////////////////////////////////////

class TLogConfig
    : public NYTree::TYsonSerializable
{
public:
    TNullable<TDuration> FlushPeriod;
    TNullable<TDuration> WatchPeriod;
    TNullable<TDuration> CheckSpacePeriod;

    i64 MinDiskSpace;

    int HighBacklogWatermark;
    int LowBacklogWatermark;

    TDuration ShutdownGraceTimeout;

    std::vector<TRuleConfigPtr> Rules;
    yhash_map<Stroka, TWriterConfigPtr> WriterConfigs;

    TLogConfig()
    {
        RegisterParameter("flush_period", FlushPeriod)
            .Default();
        RegisterParameter("watch_period", WatchPeriod)
            .Default();
        RegisterParameter("check_space_period", CheckSpacePeriod)
            .Default();
        RegisterParameter("min_disk_space", MinDiskSpace)
            .GreaterThanOrEqual((i64) 1024 * 1024 * 1024)
            .Default((i64) 5 * 1024 * 1024 * 1024);
        RegisterParameter("high_backlog_watermark", HighBacklogWatermark)
            .GreaterThan(0)
            .Default(1000000);
        RegisterParameter("low_backlog_watermark", LowBacklogWatermark)
            .GreaterThan(0)
            .Default(100000);
        RegisterParameter("shutdown_grace_timeout", ShutdownGraceTimeout)
            .Default(TDuration::Seconds(1));

        RegisterParameter("writers", WriterConfigs);
        RegisterParameter("rules", Rules);

        RegisterValidator([&] () {
            for (const auto& rule : Rules) {
                for (const Stroka& writer : rule->Writers) {
                    if (WriterConfigs.find(writer) == WriterConfigs.end()) {
                        THROW_ERROR_EXCEPTION("Unknown writer %Qv", writer);
                    }
                }
            }
        });
    }

    static TLogConfigPtr CreateDefault();
    static TLogConfigPtr CreateQuiet();
    static TLogConfigPtr CreateFromNode(NYTree::INodePtr node, const NYPath::TYPath& path = "");
};

DEFINE_REFCOUNTED_TYPE(TLogConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NLogging
} // namespace NYT
