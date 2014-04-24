#pragma once

#include "public.h"

#include <core/ytree/public.h>
#include <core/ytree/yson_serializable.h>

namespace NYT {
namespace NLog {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EType,
    (File)
    (StdOut)
    (StdErr)
);

struct TWriterConfig
    : public TYsonSerializable
{
    EType Type;
    Stroka Pattern;
    Stroka FileName;

    TWriterConfig()
    {
        RegisterParameter("type", Type);
        RegisterParameter("file_name", FileName)
            .Default();

        RegisterValidator([&] () {
            if (Type == EType::File && FileName.empty()) {
                THROW_ERROR_EXCEPTION("FileName is empty while type is File");
            } else if (Type != EType::File && !FileName.empty()) {
                THROW_ERROR_EXCEPTION("FileName is not empty while type is not File");
            }
        });
    }
};

typedef TIntrusivePtr<TWriterConfig> TWriterConfigPtr;

////////////////////////////////////////////////////////////////////////////////

struct TRule
    : public TYsonSerializable
{
    const char* AllCategoriesName = "*";

    bool IncludeAllCategories;
    yhash_set<Stroka> IncludeCategories;
    yhash_set<Stroka> ExcludeCategories;
    ELogLevel MinLevel;
    ELogLevel MaxLevel;

    std::vector<Stroka> Writers;

    TRule()
        : IncludeAllCategories(false)
    {
        RegisterParameter("include_categories", IncludeCategories)
            .NonEmpty();
        RegisterParameter("exclude_categories", ExcludeCategories)
            .Default(yhash_set<Stroka>());
        RegisterParameter("min_level", MinLevel)
            .Default(ELogLevel::Minimum);
        RegisterParameter("max_level", MaxLevel)
            .Default(ELogLevel::Maximum);
        RegisterParameter("writers", Writers)
            .NonEmpty();
    }

    virtual void OnLoaded() override;

    bool IsApplicable(const Stroka& category) const;
    bool IsApplicable(const Stroka& category, ELogLevel level) const;
};

typedef TIntrusivePtr<TRule> TRulePtr;

////////////////////////////////////////////////////////////////////////////////

class TLogConfig
    : public TYsonSerializable
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
            FOREACH (const auto& rule, Rules) {
                FOREACH (const Stroka& writer, rule->Writers) {
                    if (WriterConfigs.find(writer) == WriterConfigs.end()) {
                        THROW_ERROR_EXCEPTION("Unknown writer: %s", ~writer.Quote());
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
