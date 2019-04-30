#include "logical_type.h"

#include <yt/client/table_client/proto/chunk_meta.pb.h>

#include <yt/core/misc/error.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/node.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

TLogicalType::TLogicalType(ELogicalMetatype type)
    : Metatype_(type)
{}

ELogicalMetatype TLogicalType::GetMetatype() const
{
    return Metatype_;
}

const TSimpleLogicalType& TLogicalType::AsSimpleTypeRef() const
{
    return dynamic_cast<const TSimpleLogicalType&>(*this);
}

const TOptionalLogicalType& TLogicalType::AsOptionalTypeRef() const
{
    return dynamic_cast<const TOptionalLogicalType&>(*this);
}

const TListLogicalType& TLogicalType::AsListTypeRef() const
{
    return dynamic_cast<const TListLogicalType&>(*this);
}

TString ToString(const TLogicalType& logicalType)
{

    switch (logicalType.GetMetatype()) {
        case ELogicalMetatype::Simple:
            return CamelCaseToUnderscoreCase(ToString(logicalType.AsSimpleTypeRef().GetElement()));
        case ELogicalMetatype::Optional:
            return Format("optional<%v>", *logicalType.AsOptionalTypeRef().GetElement());
        case ELogicalMetatype::List:
            return Format("list<%v>", *logicalType.AsListTypeRef().GetElement());
    }
    Y_UNREACHABLE();
}

////////////////////////////////////////////////////////////////////////////////

TOptionalLogicalType::TOptionalLogicalType(TLogicalTypePtr element)
    : TLogicalType(ELogicalMetatype::Optional)
    , Element_(element)
{ }

const TLogicalTypePtr& TOptionalLogicalType::GetElement() const
{
    return Element_;
}

std::optional<ESimpleLogicalValueType> TOptionalLogicalType::Simplify() const
{
    if (GetElement()->GetMetatype() == ELogicalMetatype::Simple) {
        return GetElement()->AsSimpleTypeRef().GetElement();
    } else {
        return std::nullopt;
    }
}

size_t TOptionalLogicalType::GetMemoryUsage() const
{
    if (Element_->GetMetatype() == ELogicalMetatype::Simple) {
        // All optionals of simple logical types are signletons and therefore we assume they use no space.
        return 0;
    } else {
        return sizeof(*this) + Element_->GetMemoryUsage();
    }
}

int TOptionalLogicalType::GetTypeComplexity() const
{
    if (Element_->GetMetatype() == ELogicalMetatype::Simple) {
        return 1;
    } else {
        return 1 + Element_->GetTypeComplexity();
    }
}

////////////////////////////////////////////////////////////////////////////////

TSimpleLogicalType::TSimpleLogicalType(ESimpleLogicalValueType element)
    : TLogicalType(ELogicalMetatype::Simple)
    , Element_(element)
{ }

ESimpleLogicalValueType TSimpleLogicalType::GetElement() const
{
    return Element_;
}

size_t TSimpleLogicalType::GetMemoryUsage() const
{
    // All simple logical types are signletons and therefore we assume they use no space.
    return 0;
}

int TSimpleLogicalType::GetTypeComplexity() const
{
    return 1;
}

////////////////////////////////////////////////////////////////////////////////

TListLogicalType::TListLogicalType(TLogicalTypePtr element)
    : TLogicalType(ELogicalMetatype::List)
    , Element_(element)
{ }

const TLogicalTypePtr& TListLogicalType::GetElement() const
{
    return Element_;
}

size_t TListLogicalType::GetMemoryUsage() const
{
    return sizeof(*this) + Element_->GetMemoryUsage();
}

int TListLogicalType::GetTypeComplexity() const
{
    return 1 + Element_->GetTypeComplexity();
}

////////////////////////////////////////////////////////////////////////////////

std::pair<std::optional<ESimpleLogicalValueType>, bool> SimplifyLogicalType(const TLogicalTypePtr& logicalType)
{
    switch (logicalType->GetMetatype()) {
        case ELogicalMetatype::Simple:
            return std::make_tuple(std::make_optional(logicalType->AsSimpleTypeRef().GetElement()), true);
        case ELogicalMetatype::Optional:
            return std::make_pair(logicalType->AsOptionalTypeRef().Simplify(), false);
        case ELogicalMetatype::List:
            return std::make_pair(std::nullopt, true);
    }
    Y_UNREACHABLE();
}

bool operator == (const TLogicalType& lhs, const TLogicalType& rhs)
{
    if (&lhs == &rhs) {
        return true;
    }

    if (lhs.GetMetatype() != rhs.GetMetatype()) {
        return false;
    }

    switch (lhs.GetMetatype()) {
        case ELogicalMetatype::Simple:
            return lhs.AsSimpleTypeRef().GetElement() == rhs.AsSimpleTypeRef().GetElement();
        case ELogicalMetatype::Optional:
            return *lhs.AsOptionalTypeRef().GetElement() == *rhs.AsOptionalTypeRef().GetElement();
        case ELogicalMetatype::List:
            return *lhs.AsListTypeRef().GetElement() == *rhs.AsListTypeRef().GetElement();
    }
    Y_UNREACHABLE();
}

void ValidateAlterType(const TLogicalTypePtr& oldType, const TLogicalTypePtr& newType)
{
    if (*oldType == *newType) {
        return;
    }
    const auto& [simplifiedOldLogicalType, oldRequired] = SimplifyLogicalType(oldType);
    const auto& [simplifiedNewLogicalType, newRequired] = SimplifyLogicalType(newType);
    if (simplifiedOldLogicalType != simplifiedNewLogicalType ||
        !simplifiedOldLogicalType || // NB. types are different (we already checked this) and are complex
        !oldRequired && newRequired)
    {
        THROW_ERROR_EXCEPTION("Cannot alter type %Qv to type %Qv",
            *oldType,
            *newType);
    }
}

static bool IsSubtypeOf(ESimpleLogicalValueType lhs, ESimpleLogicalValueType rhs)
{
    if (lhs == rhs) {
        return true;
    }
    if (rhs == ESimpleLogicalValueType::Any) {
        return true;
    }

    auto leftPhysicalType = GetPhysicalType(lhs);
    auto rightPhysicalType = GetPhysicalType(rhs);
    if (leftPhysicalType != rightPhysicalType) {
        return false;
    }

    if (leftPhysicalType == EValueType::Uint64 || leftPhysicalType == EValueType::Int64) {
        static const std::vector<ESimpleLogicalValueType> order = {
            ESimpleLogicalValueType::Uint8,
            ESimpleLogicalValueType::Int8,
            ESimpleLogicalValueType::Uint16,
            ESimpleLogicalValueType::Int16,
            ESimpleLogicalValueType::Uint32,
            ESimpleLogicalValueType::Int32,
            ESimpleLogicalValueType::Uint64,
            ESimpleLogicalValueType::Int64,
        };

        auto lit = std::find(order.begin(), order.end(), lhs);
        auto rit = std::find(order.begin(), order.end(), rhs);
        Y_ASSERT(lit != order.end());
        Y_ASSERT(rit != order.end());

        return lit <= rit;
    }

    if (leftPhysicalType == EValueType::String) {
        static const std::vector<ESimpleLogicalValueType> order = {
            ESimpleLogicalValueType::Utf8,
            ESimpleLogicalValueType::String,
        };
        auto lit = std::find(order.begin(), order.end(), lhs);
        auto rit = std::find(order.begin(), order.end(), rhs);
        Y_ASSERT(lit != order.end());
        Y_ASSERT(rit != order.end());
        return lit <= rit;
    }

    return false;
}

bool IsSubtypeOf(const TLogicalTypePtr& lhs, const TLogicalTypePtr& rhs)
{
    if (*lhs == *rhs) {
        return true;
    }

    const auto& [lhsSimplifiedLogicalType, lhsRequired] = SimplifyLogicalType(lhs);
    const auto& [rhsSimplifiedLogicalType, rhsRequired] = SimplifyLogicalType(rhs);

    if (!lhsSimplifiedLogicalType || !rhsSimplifiedLogicalType) {
        return false;
    }
    if (rhsRequired && !lhsRequired) {
        return false;
    }
    return IsSubtypeOf(*lhsSimplifiedLogicalType, *rhsSimplifiedLogicalType);
}

void ToProto(NProto::TLogicalType* protoLogicalType, const TLogicalTypePtr& logicalType)
{
    switch (logicalType->GetMetatype()) {
        case ELogicalMetatype::Simple:
            protoLogicalType->set_simple(static_cast<int>(logicalType->AsSimpleTypeRef().GetElement()));
            return;
        case ELogicalMetatype::Optional:
            ToProto(protoLogicalType->mutable_optional(), logicalType->AsOptionalTypeRef().GetElement());
            return;
        case ELogicalMetatype::List:
            ToProto(protoLogicalType->mutable_list(), logicalType->AsListTypeRef().GetElement());
            return;
    }
    Y_UNREACHABLE();
}

void FromProto(TLogicalTypePtr* logicalType, const NProto::TLogicalType& protoLogicalType)
{
    switch (protoLogicalType.type_case()) {
        case NProto::TLogicalType::TypeCase::kSimple:
            *logicalType = SimpleLogicalType(static_cast<ESimpleLogicalValueType>(protoLogicalType.simple()), true);
            return;
        case NProto::TLogicalType::TypeCase::kOptional: {
            TLogicalTypePtr element;
            FromProto(&element, protoLogicalType.optional());
            *logicalType = OptionalLogicalType(element);
            return;
        }
        case NProto::TLogicalType::TypeCase::kList: {
            TLogicalTypePtr element;
            FromProto(&element, protoLogicalType.list());
            *logicalType = ListLogicalType(element);
            return;
        }
        case NProto::TLogicalType::TypeCase::TYPE_NOT_SET:
            break;
    }
    Y_UNREACHABLE();
}

void Serialize(const TLogicalTypePtr& logicalType, NYson::IYsonConsumer* consumer)
{
    const auto metatype = logicalType->GetMetatype();
    switch (metatype) {
        case ELogicalMetatype::Simple:
            NYTree::BuildYsonFluently(consumer)
                .Value(logicalType->AsSimpleTypeRef().GetElement());
            return;
        case ELogicalMetatype::Optional:
            NYTree::BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("metatype").Value(metatype)
                    .Item("element").Value(logicalType->AsOptionalTypeRef().GetElement())
                .EndMap();
            return;
        case ELogicalMetatype::List:
            NYTree::BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("metatype").Value(metatype)
                    .Item("element").Value(logicalType->AsListTypeRef().GetElement())
                .EndMap();
            return;
    }
    Y_UNREACHABLE();
}

void Deserialize(TLogicalTypePtr& logicalType, NYTree::INodePtr node)
{
    if (node->GetType() == NYTree::ENodeType::String) {
        auto simpleLogicalType = NYTree::ConvertTo<ESimpleLogicalValueType>(node);
        logicalType = SimpleLogicalType(simpleLogicalType, true);
        return;
    }
    if (node->GetType() != NYTree::ENodeType::Map) {
        THROW_ERROR_EXCEPTION("Error parsing logical type: expected %Qlv or %Qlv, actual %Qlv",
            NYTree::ENodeType::Map,
            NYTree::ENodeType::String,
            node->GetType());
    }
    auto mapNode = node->AsMap();

    ELogicalMetatype metatype;
    {
        auto metatypeNode = mapNode->GetChild("metatype");
        metatype = NYTree::ConvertTo<ELogicalMetatype>(metatypeNode);
    }
    switch (metatype) {
        case ELogicalMetatype::Simple: {
            THROW_ERROR_EXCEPTION("Error parsing logical type: cannot parse simple type from %Qv",
                NYTree::ENodeType::Map);
        }
        case ELogicalMetatype::Optional: {
            auto elementNode = mapNode->GetChild("element");
            auto element = NYTree::ConvertTo<TLogicalTypePtr>(elementNode);
            logicalType = OptionalLogicalType(std::move(element));
            return;
        }
        case ELogicalMetatype::List: {
            auto elementNode = mapNode->GetChild("element");
            auto element = NYTree::ConvertTo<TLogicalTypePtr>(elementNode);
            logicalType = ListLogicalType(std::move(element));
            return;
        }
    }
    Y_UNREACHABLE();
}

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TSimpleTypeStore
{
public:
    TSimpleTypeStore()
    {
        for (auto simpleLogicalType : TEnumTraits<ESimpleLogicalValueType>::GetDomainValues()) {
            auto logicalType = New<TSimpleLogicalType>(simpleLogicalType);
            SimpleTypeMap[simpleLogicalType] = logicalType;
            OptionalTypeMap[simpleLogicalType] = New<TOptionalLogicalType>(logicalType);
        }
    }

    const TLogicalTypePtr& GetSimpleType(ESimpleLogicalValueType type)
    {
        auto it = SimpleTypeMap.find(type);
        YCHECK(it != SimpleTypeMap.end());
        return it->second;
    }

    const TLogicalTypePtr& GetOptionalType(ESimpleLogicalValueType type)
    {
        auto it = OptionalTypeMap.find(type);
        YCHECK(it != OptionalTypeMap.end());
        return it->second;
    }

private:
    THashMap<ESimpleLogicalValueType, TLogicalTypePtr> SimpleTypeMap;
    THashMap<ESimpleLogicalValueType, TLogicalTypePtr> OptionalTypeMap;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TLogicalTypePtr OptionalLogicalType(TLogicalTypePtr element)
{
    if (element->GetMetatype() == ELogicalMetatype::Simple) {
        auto simpleLogicalType = element->AsSimpleTypeRef().GetElement();
        if (element.Get() == Singleton<TSimpleTypeStore>()->GetSimpleType(simpleLogicalType).Get()) {
            return Singleton<TSimpleTypeStore>()->GetOptionalType(simpleLogicalType);
        }
    }
    return New<TOptionalLogicalType>(std::move(element));
}

TLogicalTypePtr SimpleLogicalType(ESimpleLogicalValueType element, bool required)
{
    if (required) {
        return Singleton<TSimpleTypeStore>()->GetSimpleType(element);
    } else {
        return Singleton<TSimpleTypeStore>()->GetOptionalType(element);
    }
}

TLogicalTypePtr ListLogicalType(TLogicalTypePtr element)
{
    return New<TListLogicalType>(element);
}

TLogicalTypePtr NullLogicalType = Singleton<TSimpleTypeStore>()->GetSimpleType(ESimpleLogicalValueType::Null);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

size_t THash<NYT::NTableClient::TLogicalType>::operator()(const NYT::NTableClient::TLogicalType& logicalType) const
{
    using namespace NYT::NTableClient;
    const size_t typeHash = static_cast<size_t>(logicalType.GetMetatype());
    switch (logicalType.GetMetatype()) {
        case ELogicalMetatype::Simple:
            return CombineHashes(static_cast<size_t>(logicalType.AsSimpleTypeRef().GetElement()), typeHash);
        case ELogicalMetatype::Optional:
            return CombineHashes((*this)(*logicalType.AsOptionalTypeRef().GetElement()), typeHash);
        case ELogicalMetatype::List:
            return CombineHashes((*this)(*logicalType.AsListTypeRef().GetElement()), typeHash);
    }
    Y_UNREACHABLE();
}
