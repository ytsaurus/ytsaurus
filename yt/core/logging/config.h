#pragma once

#include "public.h"

#include <core/ytree/public.h>
#include <core/ytree/yson_serializable.h>

namespace NYT {
namespace NLog {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EWriterType,
    (File)
    (Stdout)
    (Stderr)
);

struct TWriterConfig
    : public NYTree::TYsonSerializable
{
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

typedef TIntrusivePtr<TWriterConfig> TWriterConfigPtr;

////////////////////////////////////////////////////////////////////////////////

struct TRule
    : public NYTree::TYsonSerializable
{
    TNullable<yhash_set<Stroka>> IncludeCategories;
    yhash_set<Stroka> ExcludeCategories;
    ELogLevel MinLevel;
    ELogLevel MaxLevel;

    std::vector<Stroka> Writers;

    TRule()
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

typedef TIntrusivePtr<TRule> TRulePtr;

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

    std::vector<TRulePtr> Rules;
    yhash_map<Stroka, TWriterConfigPtr> WriterConfigs;

    TLogConfig()
    {
        RegisterParameter("flush_period", FlushPeriod)
            .Default(Null);
        RegisterParameter("watch_period", WatchPeriod)
            .Default(Null);
        RegisterParameter("check_space_period", CheckSpacePeriod)
            .Default(Null);
        RegisterParameter("min_disk_space", MinDiskSpace)
            .GreaterThanOrEqual((i64) 1024 * 1024 * 1024)
            .Default((i64) 5 * 1024 * 1024 * 1024);
        RegisterParameter("high_backlog_watermark", HighBacklogWatermark)
            .GreaterThan(0)
            .Default(1000000);
        RegisterParameter("low_backlog_watermark", LowBacklogWatermark)
            .GreaterThan(0)
            .Default(100000);

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
    static TLogConfigPtr CreateFromNode(NYTree::INodePtr node, const NYPath::TYPath& path = "");
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NLog
} // namespace NYT
