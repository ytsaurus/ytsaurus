#include "yson_struct.h"

#include <yt/yt/core/yson/consumer.h>

#include <yt/yt/core/ytree/node.h>
#include <yt/yt/core/ytree/ypath_detail.h>

#include <yt/yt/core/misc/singleton.h>

#include <util/generic/algorithm.h>

namespace NYT::NYTree {

using namespace NYPath;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

IMapNodePtr TYsonStructBase::GetLocalUnrecognized() const
{
    return LocalUnrecognized_;
}

IMapNodePtr TYsonStructBase::GetRecursiveUnrecognized() const
{
    return Meta_->GetRecursiveUnrecognized(this);
}

void TYsonStructBase::SetUnrecognizedStrategy(EUnrecognizedStrategy strategy)
{
    InstanceUnrecognizedStrategy_ = strategy;
}

THashSet<TString> TYsonStructBase::GetRegisteredKeys() const
{
    return Meta_->GetRegisteredKeys();
}

void TYsonStructBase::Load(
    INodePtr node,
    bool postprocess,
    bool setDefaults,
    const TYPath& path)
{
    Meta_->LoadStruct(this, node, postprocess, setDefaults, path);
}

void TYsonStructBase::Save(
    IYsonConsumer* consumer,
    bool stable) const
{
    consumer->OnBeginMap();
    for (const auto& [name, parameter] : Meta_->GetParameterSortedList()) {
        if (!parameter->CanOmitValue(this)) {
            consumer->OnKeyedItem(name);
            parameter->Save(this, consumer);
        }
    }

    if (LocalUnrecognized_) {
        auto unrecognizedList = LocalUnrecognized_->GetChildren();
        if (stable) {
            std::sort(
                unrecognizedList.begin(),
                unrecognizedList.end(),
                [] (const auto& x, const auto& y) {
                    return x.first < y.first;
                });
        }
        for (const auto& [key, child] : unrecognizedList) {
            consumer->OnKeyedItem(key);
            Serialize(child, consumer);
        }
    }

    consumer->OnEndMap();
}

void TYsonStructBase::Postprocess(const TYPath& path)
{
    Meta_->Postprocess(this, path);
}

void TYsonStructBase::SetDefaults()
{
    Meta_->SetDefaultsOfInitializedStruct(this);
}

void TYsonStructBase::SaveParameter(const TString& key, IYsonConsumer* consumer) const
{
    Meta_->GetParameter(key)->Save(this, consumer);
}

void TYsonStructBase::LoadParameter(const TString& key, const NYTree::INodePtr& node, EMergeStrategy mergeStrategy)
{
    Meta_->LoadParameter(this, key, node, mergeStrategy);
}

void TYsonStructBase::ResetParameter(const TString& key)
{
    Meta_->GetParameter(key)->SetDefaultsInitialized(this);
}

int TYsonStructBase::GetParameterCount() const
{
    return Meta_->GetParameterMap().size();
}

std::vector<TString> TYsonStructBase::GetAllParameterAliases(const TString& key) const
{
    auto parameter = Meta_->GetParameter(key);
    auto result = parameter->GetAliases();
    result.push_back(parameter->GetKey());
    return result;
}

TYsonStructBase::TForbidCachedDynamicCastGuard::TForbidCachedDynamicCastGuard(TYsonStructBase* target)
    : Target_(target)
{
    Target_->CachedDynamicCastAllowed_ = false;
};

TYsonStructBase::TForbidCachedDynamicCastGuard::~TForbidCachedDynamicCastGuard()
{
    Target_->CachedDynamicCastAllowed_ = true;
}

////////////////////////////////////////////////////////////////////////////////

void TYsonStruct::InitializeRefCounted()
{
    if (!TYsonStructRegistry::InitializationInProgress()) {
        Meta_->SetDefaultsOfInitializedStruct(this);
    }
}

////////////////////////////////////////////////////////////////////////////////

TYsonStructRegistry* TYsonStructRegistry::Get()
{
    return LeakySingleton<TYsonStructRegistry>();
}

bool TYsonStructRegistry::InitializationInProgress()
{
    return CurrentlyInitializingMeta_ != nullptr;
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TYsonStructBase& value, IYsonConsumer* consumer)
{
    value.Save(consumer);
}

void Deserialize(TYsonStructBase& value, INodePtr node)
{
    value.Load(node);
}

TYsonString ConvertToYsonStringStable(const TYsonStructBase& value)
{
    TStringStream output;
    TBufferedBinaryYsonWriter writer(&output);
    value.Save(
        &writer,
        true); // truth matters :)
    writer.Flush();
    return TYsonString(output.Str());
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TYsonStruct);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree


namespace NYT {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TBinaryYsonStructSerializer::Save(TStreamSaveContext& context, const TYsonStructBase& obj)
{
    auto str = ConvertToYsonStringStable(obj);
    NYT::Save(context, str);
}

void TBinaryYsonStructSerializer::Load(TStreamLoadContext& context, TYsonStructBase& obj)
{
    auto str = NYT::Load<TYsonString>(context);
    auto node = ConvertTo<INodePtr>(str);
    obj.Load(node);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
