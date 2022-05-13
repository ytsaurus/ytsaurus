#pragma once

#include <yt/yt/core/ytree/public.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TFieldsFilter final
{
public:
    TFieldsFilter();
    TFieldsFilter(const NYTree::IAttributeDictionaryPtr& options);

    bool IsFieldSuitable(TStringBuf field) const;

private:
    std::optional<THashSet<TString>> Filter_;

    static std::optional<THashSet<TString>> ParseFilterFromOptions(
        const NYTree::IAttributeDictionaryPtr& options);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

////////////////////////////////////////////////////////////////////////////////

#define ITEM_DO_IF_SUITABLE_FOR_FILTER(filter, field, ...) DoIf(filter.IsFieldSuitable(field), [&] (TFluentMap fluent) { \
        fluent.Item(field).Do(__VA_ARGS__); \
    })

#define ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, field, ...) DoIf(filter.IsFieldSuitable(field), [&] (TFluentMap fluent) { \
        fluent.Item(field).Value(__VA_ARGS__); \
    })
