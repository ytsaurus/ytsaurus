#include "attribute_profiler.h"
#include "attribute_schema.h"
#include "private.h"

#include <yt/yt_proto/yt/orm/client/proto/object.pb.h>

#include <yt/yt/orm/server/objects/object_reflection.h>

#include <yt/yt/orm/client/objects/registry.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/library/syncmap/map.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

namespace {

TString ExtractSensorValue(NYTree::INodePtr node)
{
    switch (node->GetType()) {
        case NYTree::ENodeType::String:
            return node->AsString()->GetValue();
        case NYTree::ENodeType::Int64:
            return ToString(node->AsInt64()->GetValue());
        case NYTree::ENodeType::Uint64:
            return ToString(node->AsUint64()->GetValue());
        case NYTree::ENodeType::Double:
            return ToString(node->AsDouble()->GetValue());
        case NYTree::ENodeType::Boolean:
            return ToString(node->AsBoolean()->GetValue());
        case NYTree::ENodeType::Entity:
            return "#";
        default:
            THROW_ERROR_EXCEPTION("Unable extract sensor value from node of type %Qlv", node->GetType());
    }
}

class TAttributeValueAttributeProfiler
    : public IAttributeProfiler
{
public:
    TAttributeValueAttributeProfiler(IObjectTypeHandler* typeHandler, const NYPath::TYPath& path)
        : Profiler_(NObjects::Profiler
            .WithSparse()
            .WithRequiredTag("object_type",
                NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(typeHandler->GetType()))
            .WithRequiredTag("attribute", path))
        , ValueTransform_(ExtractSensorValue)
    {
        auto resolveResult = ResolveAttribute(typeHandler, path, {});
        Attribute_ = resolveResult.Attribute->TryAsScalar();
        YT_VERIFY(Attribute_);
        SuffixPath_ = std::move(resolveResult.SuffixPath);
    }

    void Profile(TTransaction* transaction, const TObject* object) override
    {
        if (!object->IsStoreScheduled()) {
            return;
        }

        bool created = object->GetState() == EObjectState::Created;
        if (!created && Attribute_->HasChangedGetter() && !Attribute_->RunChangedGetter(object, SuffixPath_))
        {
            return;
        }
        auto value = RunConsumedValueGetter(Attribute_, transaction, object, SuffixPath_);
        auto valueStr = ValueTransform_(std::move(value));
        auto [counter, _] = Counters_.FindOrInsert(valueStr, [this, &valueStr, created] {
            return Profiler_
                .WithRequiredTag("value", std::move(valueStr))
                .WithRequiredTag("state", created ? "created" : "updated")
                .Counter("/attribute_value");
        });
        counter->Increment();
    }

    void SetValueTransform(TValueTransform transform) override
    {
        ValueTransform_ = std::move(transform);
    }

private:
    NProfiling::TProfiler Profiler_;
    NConcurrency::TSyncMap<TString, NProfiling::TCounter> Counters_;

    const TScalarAttributeSchema* Attribute_;
    NYPath::TYPath SuffixPath_;

    TValueTransform ValueTransform_;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

IAttributeProfilerPtr CreateAttributeProfiler(
    IObjectTypeHandler* typeHandler,
    const NYPath::TYPath& path,
    NClient::NProto::EAttributeSensorPolicy policy)
{
    switch (policy) {
        case NClient::NProto::ASP_ATTRIBUTE_VALUE_AS_TAG:
            return New<TAttributeValueAttributeProfiler>(typeHandler, path);
        default: {
            static const auto* enumType = NYson::ReflectProtobufEnumType(
                google::protobuf::GetEnumDescriptor<NClient::NProto::EAttributeSensorPolicy>());
            THROW_ERROR_EXCEPTION("Unsupported attribute sensor policy %v",
                NYson::FindProtobufEnumLiteralByValue(enumType, policy));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
