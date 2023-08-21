#include "category_registry.h"

#include "private.h"

#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NLogging {

using namespace NYson;
using namespace NYTree;
using namespace NTableClient;

static const auto& Logger = CategoryRegistryLogger;

////////////////////////////////////////////////////////////////////////////////

TStructuredCategoryRegistry* TStructuredCategoryRegistry::Get()
{
    auto* topicRegistry = Singleton<TStructuredCategoryRegistry>();
    return topicRegistry;
}

void TStructuredCategoryRegistry::RegisterStructuredCategory(TString name, TTableSchemaPtr schema)
{
    auto guard = Guard(SpinLock_);
    YT_VERIFY(Categories_.insert({name, schema}).second);
}

void TStructuredCategoryRegistry::DumpCategories(IYsonConsumer* consumer)
{
    auto guard = Guard(SpinLock_);
    BuildYsonFluently(consumer)
        .BeginMap()
            .DoFor(Categories_, [] (TFluentMap fluent, const auto& category) {
                fluent.Item(category.first)
                    .BeginMap()
                        .Item("schema").Value(category.second)
                    .EndMap();
            })
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

TLogger CreateSchemafulLogger(TString category, TTableSchemaPtr schema)
{
    TStructuredCategoryRegistry::Get()->RegisterStructuredCategory(category, schema);

    return TLogger(category).WithStructuredValidator([schema, category] (const TYsonString& yson) {
        try {
            YsonToSchemafulRow(yson.ToString(), *schema, /*treatMissingAsNull*/ true, EYsonType::MapFragment, /*validateValues*/ true);
        } catch (const std::exception& ex) {
            YT_LOG_ALERT(ex, "Structured log validation error (LogCategory: %v)", category);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
