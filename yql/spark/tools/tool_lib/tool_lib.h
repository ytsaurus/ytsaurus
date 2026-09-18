#pragma once

#include <yql/essentials/sql/sql.h>
#include <yql/essentials/tools/yql_facade_run/yql_facade_run.h>

#include <library/cpp/getopt/last_getopt.h>

namespace NSparkTool {

struct TSparkSettings {
    bool ForceSparkSyntax = false;
    TString ParserPath;
    ui16 ParserPort = 0;
};

void AddSparkTranslator(NSQLTranslation::TTranslatorsRegistry& registry, const TSparkSettings& settings);
void AddSparkOptions(NLastGetopt::TOpts& opts, TSparkSettings& settings);
void ValidateSparkSettings(const TSparkSettings& settings);
void ApplySparkSettings(NYql::TFacadeRunOptions& options, const TSparkSettings& settings);

} // namespace NSparkTool
