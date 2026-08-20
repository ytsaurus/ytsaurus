#include "task_data_builder.h"

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/node.h>

#include <util/string/join.h>

namespace NYT::NYqlPlugin {

////////////////////////////////////////////////////////////////////////////////

namespace {

THashMap<TString, TBuilderFactory>& TaskDataBuilders()
{
    static THashMap<TString, TBuilderFactory> builders;
    return builders;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void RegisterTaskDataBuilder(const TString& flavor, TBuilderFactory factory)
{
    bool inserted = TaskDataBuilders().emplace(flavor, std::move(factory)).second;
    YT_VERIFY(inserted);
}

std::unique_ptr<ITaskDataBuilder> CreateTaskDataBuilder(const TString& flavor)
{
    const auto& builders = TaskDataBuilders();
    auto it = builders.find(flavor);
    if (it == builders.end()) {
        TVector<TString> registeredFlavors;
        registeredFlavors.reserve(builders.size());
        for (const auto& builder : builders) {
            registeredFlavors.push_back(builder.first);
        }

        ythrow yexception() << "No task data builder registered for flavor '" << flavor
            << "' (registered flavors: " << JoinSeq(", ", registeredFlavors) << ")";
    }
    return it->second();
}

////////////////////////////////////////////////////////////////////////////////

TBuilderRegistrar::TBuilderRegistrar(const TString& flavor, TBuilderFactory factory)
{
    RegisterTaskDataBuilder(flavor, std::move(factory));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin
