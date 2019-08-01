#include "logical_type.h"

#include <yt/client/table_client/proto/chunk_meta.pb.h>

#include <yt/core/misc/error.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/node.h>

#include <util/charset/utf8.h>

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

const TStructLogicalType& TLogicalType::AsStructTypeRef() const
{
    return dynamic_cast<const TStructLogicalType&>(*this);
}

const TTupleLogicalType& TLogicalType::AsTupleTypeRef() const
{
    return dynamic_cast<const TTupleLogicalType&>(*this);
}

const TVariantTupleLogicalType& TLogicalType::AsVariantTupleTypeRef() const
{
    return dynamic_cast<const TVariantTupleLogicalType&>(*this);
}

const TVariantStructLogicalType& TLogicalType::AsVariantStructTypeRef() const
{
    return dynamic_cast<const TVariantStructLogicalType&>(*this);
}

static bool operator == (const TStructField& lhs, const TStructField& rhs)
{
    return (lhs.Name == rhs.Name) && (*lhs.Type == *rhs.Type);
}

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TLogicalType& logicalType)
{

    switch (logicalType.GetMetatype()) {
        case ELogicalMetatype::Simple:
            return CamelCaseToUnderscoreCase(ToString(logicalType.AsSimpleTypeRef().GetElement()));
        case ELogicalMetatype::Optional:
            return Format("optional<%v>", *logicalType.AsOptionalTypeRef().GetElement());
        case ELogicalMetatype::List:
            return Format("list<%v>", *logicalType.AsListTypeRef().GetElement());
        case ELogicalMetatype::Struct: {
            TStringStream out;
            out << "struct<";
            bool first = true;
            for (const auto& structItem : logicalType.AsStructTypeRef().GetFields()) {
                if (first) {
                    first = false;
                } else {
                    out << ';';
                }
                out << structItem.Name << '=' << ToString(*structItem.Type);
            }
            out << '>';
            return out.Str();
        }
        case ELogicalMetatype::Tuple: {
            TStringStream out;
            out << "tuple<";
            bool first = true;
            for (const auto& element : logicalType.AsTupleTypeRef().GetElements()) {
                if (first) {
                    first = false;
                } else {
                    out << ';';
                }
                out << ToString(*element);
            }
            out << '>';
            return out.Str();
        }
        case ELogicalMetatype::VariantTuple: {
            TStringStream out;
            out << "variant<";
            bool first = true;
            for (const auto& element : logicalType.AsVariantTupleTypeRef().GetElements()) {
                if (first) {
                    first = false;
                } else {
                    out << ';';
                }
                out << ToString(*element);
            }
            out << '>';
            return out.Str();
        }
        case ELogicalMetatype::VariantStruct: {
            TStringStream out;
            out << "named_variant<";
            bool first = true;
            for (const auto& field : logicalType.AsVariantStructTypeRef().GetFields()) {
                if (first) {
                    first = false;
                } else {
                    out << ';';
                }
                out << field.Name << '=' << ToString(*field.Type);
            }
            out << '>';
            return out.Str();

        }
    }
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

TOptionalLogicalType::TOptionalLogicalType(TLogicalTypePtr element)
    : TLogicalType(ELogicalMetatype::Optional)
    , Element_(element)
    , ElementIsNullable_(Element_->IsNullable())
{ }

const TLogicalTypePtr& TOptionalLogicalType::GetElement() const
{
    return Element_;
}

std::optional<ESimpleLogicalValueType> TOptionalLogicalType::Simplify() const
{
    if (!IsElementNullable() && GetElement()->GetMetatype() == ELogicalMetatype::Simple) {
        return GetElement()->AsSimpleTypeRef().GetElement();
    } else {
        return std::nullopt;
    }
}

bool TOptionalLogicalType::IsElementNullable() const
{
    return ElementIsNullable_;
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

void TOptionalLogicalType::ValidateNode() const
{ }

bool TOptionalLogicalType::IsNullable() const
{
    return true;
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

void TSimpleLogicalType::ValidateNode() const
{ }

bool TSimpleLogicalType::IsNullable() const
{
    return Element_ == ESimpleLogicalValueType::Null;
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

void TListLogicalType::ValidateNode() const
{ }

bool TListLogicalType::IsNullable() const
{
    return false;
}

////////////////////////////////////////////////////////////////////////////////

TComplexTypeFieldDescriptor::TComplexTypeFieldDescriptor(TLogicalTypePtr type)
    : Type_(std::move(type))
{ }

TComplexTypeFieldDescriptor::TComplexTypeFieldDescriptor(TString columnName, TLogicalTypePtr type)
    : Descriptor_(std::move(columnName))
    , Type_(std::move(type))
{ }

TComplexTypeFieldDescriptor TComplexTypeFieldDescriptor::OptionalElement() const
{
    return TComplexTypeFieldDescriptor(Descriptor_ + ".<optional-element>", Type_->AsOptionalTypeRef().GetElement());
}

TComplexTypeFieldDescriptor TComplexTypeFieldDescriptor::ListElement() const
{
    return TComplexTypeFieldDescriptor(Descriptor_ + ".<list-element>", Type_->AsListTypeRef().GetElement());
}

TComplexTypeFieldDescriptor TComplexTypeFieldDescriptor::StructField(size_t i) const
{
    const auto& fields = Type_->AsStructTypeRef().GetFields();
    YT_VERIFY(i < fields.size());
    const auto& field = fields[i];
    return TComplexTypeFieldDescriptor(Descriptor_ + "." + field.Name, field.Type);
}

TComplexTypeFieldDescriptor TComplexTypeFieldDescriptor::TupleElement(size_t i) const
{
    const auto& elements = Type_->AsTupleTypeRef().GetElements();
    YT_VERIFY(i < elements.size());
    return TComplexTypeFieldDescriptor(Descriptor_ + Format(".<tuple-element-%v>", i), elements[i]);
}

TComplexTypeFieldDescriptor TComplexTypeFieldDescriptor::VariantTupleElement(size_t i) const
{
    const auto& elements = Type_->AsVariantTupleTypeRef().GetElements();
    YT_VERIFY(i < elements.size());
    return TComplexTypeFieldDescriptor(Descriptor_ + Format(".<variant-element-%v>", i), elements[i]);
}

TComplexTypeFieldDescriptor TComplexTypeFieldDescriptor::VariantStructField(size_t i) const
{
    const auto& fields = Type_->AsVariantStructTypeRef().GetFields();
    YT_VERIFY(i < fields.size());
    const auto& field = fields[i];
    return TComplexTypeFieldDescriptor(Descriptor_ + "." + field.Name, field.Type);
}

const TString& TComplexTypeFieldDescriptor::GetDescription() const
{
    return Descriptor_;
}

const TLogicalTypePtr& TComplexTypeFieldDescriptor::GetType() const
{
    return Type_;
}

void TComplexTypeFieldDescriptor::Walk(std::function<void(const TComplexTypeFieldDescriptor&)> onElement) const
{
    onElement(*this);
    const auto metatype = GetType()->GetMetatype();
    switch (metatype) {
        case ELogicalMetatype::Simple:
            return;
        case ELogicalMetatype::Optional:
            OptionalElement().Walk(onElement);
            return;
        case ELogicalMetatype::List:
            ListElement().Walk(onElement);
            return;
        case ELogicalMetatype::Struct:
            for (size_t i = 0; i < GetType()->AsStructTypeRef().GetFields().size(); ++i) {
                StructField(i).Walk(onElement);
            }
            return;
        case ELogicalMetatype::Tuple:
            for (size_t i = 0; i < GetType()->AsTupleTypeRef().GetElements().size(); ++i) {
                TupleElement(i).Walk(onElement);
            }
            return;
        case ELogicalMetatype::VariantStruct: {
            for (size_t i = 0; i < GetType()->AsVariantStructTypeRef().GetFields().size(); ++i) {
                VariantStructField(i).Walk(onElement);
            }
            return;
        }
        case ELogicalMetatype::VariantTuple:
            for (size_t i = 0; i < GetType()->AsVariantTupleTypeRef().GetElements().size(); ++i) {
                VariantTupleElement(i).Walk(onElement);
            }
            return;
    }
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

TStructLogicalTypeBase::TStructLogicalTypeBase(ELogicalMetatype metatype, std::vector<TStructField> fields)
    : TLogicalType(metatype)
    , Fields_(std::move(fields))
{ }

const std::vector<TStructField>& TStructLogicalTypeBase::GetFields() const
{
    return Fields_;
}

size_t TStructLogicalTypeBase::GetMemoryUsage() const
{
    size_t result = sizeof(*this);
    result += sizeof(TStructField) * Fields_.size();
    for (const auto& field : Fields_) {
        result += field.Type->GetMemoryUsage();
    }
    return result;
}

int TStructLogicalTypeBase::GetTypeComplexity() const
{
    int result = 1;
    for (const auto& field : Fields_) {
        result += field.Type->GetTypeComplexity();
    }
    return result;
}

void TStructLogicalTypeBase::ValidateNode() const
{
    THashSet<TStringBuf> usedNames;
    for (size_t i = 0; i < Fields_.size(); ++i) {
        const auto& field = Fields_[i];
        if (field.Name.empty()) {
            THROW_ERROR_EXCEPTION("Name of struct field #%v is empty",
                i);
        }
        if (usedNames.contains(field.Name)) {
            THROW_ERROR_EXCEPTION("Struct field name %Qv is used twice",
                field.Name);
        }
        usedNames.emplace(field.Name);
        if (field.Name.size() > MaxColumnNameLength) {
            THROW_ERROR_EXCEPTION("Name of struct field #%v exceeds limit: %v > %v",
                i,
                field.Name.size(),
                MaxColumnNameLength);
        }
        if (!IsUtf(field.Name)) {
            THROW_ERROR_EXCEPTION("Name of struct field #%v is not valid utf8",
                i);
        }
    }
}

bool TStructLogicalTypeBase::IsNullable() const
{
    return false;
}

////////////////////////////////////////////////////////////////////////////////

TTupleLogicalTypeBase::TTupleLogicalTypeBase(ELogicalMetatype metatype, std::vector<NYT::NTableClient::TLogicalTypePtr> elements)
    : TLogicalType(metatype)
    , Elements_(std::move(elements))
{ }

const std::vector<TLogicalTypePtr>& TTupleLogicalTypeBase::GetElements() const
{
    return Elements_;
}

size_t TTupleLogicalTypeBase::GetMemoryUsage() const
{
    size_t result = sizeof(*this);
    result += sizeof(TLogicalTypePtr) * Elements_.size();
    for (const auto& element : Elements_) {
        result += element->GetMemoryUsage();
    }
    return result;
}

int TTupleLogicalTypeBase::GetTypeComplexity() const
{
    int result = 1;
    for (const auto& element : Elements_) {
        result += element->GetTypeComplexity();
    }
    return result;
}

void TTupleLogicalTypeBase::ValidateNode() const
{ }

bool TTupleLogicalTypeBase::IsNullable() const
{
    return false;
}

////////////////////////////////////////////////////////////////////////////////

TStructLogicalType::TStructLogicalType(std::vector<NYT::NTableClient::TStructField> fields)
    : TStructLogicalTypeBase(ELogicalMetatype::Struct, std::move(fields))
{ }

////////////////////////////////////////////////////////////////////////////////

TTupleLogicalType::TTupleLogicalType(std::vector<NYT::NTableClient::TLogicalTypePtr> elements)
    : TTupleLogicalTypeBase(ELogicalMetatype::Tuple, std::move(elements))
{ }

////////////////////////////////////////////////////////////////////////////////

TVariantStructLogicalType::TVariantStructLogicalType(std::vector<NYT::NTableClient::TStructField> fields)
    : TStructLogicalTypeBase(ELogicalMetatype::VariantStruct, std::move(fields))
{ }

////////////////////////////////////////////////////////////////////////////////

TVariantTupleLogicalType::TVariantTupleLogicalType(std::vector<NYT::NTableClient::TLogicalTypePtr> elements)
    : TTupleLogicalTypeBase(ELogicalMetatype::VariantTuple, std::move(elements))
{ }

////////////////////////////////////////////////////////////////////////////////

std::pair<std::optional<ESimpleLogicalValueType>, bool> SimplifyLogicalType(const TLogicalTypePtr& logicalType)
{
    switch (logicalType->GetMetatype()) {
        case ELogicalMetatype::Simple:
            return std::make_pair(std::make_optional(logicalType->AsSimpleTypeRef().GetElement()), !logicalType->IsNullable());
        case ELogicalMetatype::Optional:
            return std::make_pair(logicalType->AsOptionalTypeRef().Simplify(), false);
        case ELogicalMetatype::List:
        case ELogicalMetatype::Struct:
        case ELogicalMetatype::Tuple:
        case ELogicalMetatype::VariantStruct:
        case ELogicalMetatype::VariantTuple:
            return std::make_pair(std::nullopt, true);
    }
    YT_ABORT();
}

bool operator != (const TLogicalType& lhs, const TLogicalType& rhs)
{
    return !(lhs == rhs);
}

bool operator == (const std::vector<TLogicalTypePtr>& lhs, const std::vector<TLogicalTypePtr>& rhs)
{
    if (lhs.size() != rhs.size()) {
        return false;
    }
    for (size_t i = 0; i < lhs.size(); ++i) {
        if (*lhs[i] != *rhs[i]) {
            return false;
        }
    }
    return true;
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
        case ELogicalMetatype::Struct:
            return lhs.AsStructTypeRef().GetFields() == rhs.AsStructTypeRef().GetFields();
        case ELogicalMetatype::Tuple:
            return lhs.AsTupleTypeRef().GetElements() == rhs.AsTupleTypeRef().GetElements();
        case ELogicalMetatype::VariantStruct:
            return lhs.AsVariantStructTypeRef().GetFields() == rhs.AsVariantStructTypeRef().GetFields();
        case ELogicalMetatype::VariantTuple:
            return lhs.AsVariantTupleTypeRef().GetElements() == rhs.AsVariantTupleTypeRef().GetElements();
    }
    YT_ABORT();
}

void ValidateLogicalType(const TComplexTypeFieldDescriptor& descriptor)
{
    descriptor.Walk([] (const TComplexTypeFieldDescriptor& descriptor) {
        try {
            descriptor.GetType()->ValidateNode();
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("%v has bad type",
                descriptor.GetDescription())
            << ex;
        }
    });
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
        YT_ASSERT(lit != order.end());
        YT_ASSERT(rit != order.end());

        return lit <= rit;
    }

    if (leftPhysicalType == EValueType::String) {
        static const std::vector<ESimpleLogicalValueType> order = {
            ESimpleLogicalValueType::Utf8,
            ESimpleLogicalValueType::String,
        };
        auto lit = std::find(order.begin(), order.end(), lhs);
        auto rit = std::find(order.begin(), order.end(), rhs);
        YT_ASSERT(lit != order.end());
        YT_ASSERT(rit != order.end());
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
        case ELogicalMetatype::Struct: {
            auto protoStruct = protoLogicalType->mutable_struct_();
            for (const auto& structField : logicalType->AsStructTypeRef().GetFields()) {
                auto protoStructField = protoStruct->add_fields();
                protoStructField->set_name(structField.Name);
                ToProto(protoStructField->mutable_type(), structField.Type);
            }
            return;
        }
        case ELogicalMetatype::Tuple: {
            auto protoTuple = protoLogicalType->mutable_tuple();
            for (const auto& element : logicalType->AsTupleTypeRef().GetElements()) {
                auto protoElement = protoTuple->add_elements();
                ToProto(protoElement, element);
            }
            return;
        }
        case ELogicalMetatype::VariantStruct: {
            auto protoVariantStruct = protoLogicalType->mutable_variant_struct();
            for (const auto& field : logicalType->AsVariantStructTypeRef().GetFields()) {
                auto protoField = protoVariantStruct->add_fields();
                protoField->set_name(field.Name);
                ToProto(protoField->mutable_type(), field.Type);
            }
            return;
        }
        case ELogicalMetatype::VariantTuple: {
            auto protoVariantTuple = protoLogicalType->mutable_variant_tuple();
            for (const auto& element : logicalType->AsVariantTupleTypeRef().GetElements()) {
                auto protoElement = protoVariantTuple->add_elements();
                ToProto(protoElement, element);
            }
            return;
        }
    }
    YT_ABORT();
}

void FromProto(TLogicalTypePtr* logicalType, const NProto::TLogicalType& protoLogicalType)
{
    switch (protoLogicalType.type_case()) {
        case NProto::TLogicalType::TypeCase::kSimple:
            *logicalType = SimpleLogicalType(static_cast<ESimpleLogicalValueType>(protoLogicalType.simple()));
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
        case NProto::TLogicalType::TypeCase::kStruct: {
            std::vector<TStructField> fields;
            for (const auto& protoField : protoLogicalType.struct_().fields()) {
                TLogicalTypePtr fieldType;
                FromProto(&fieldType, protoField.type());
                fields.emplace_back(TStructField{protoField.name(), std::move(fieldType)});
            }
            *logicalType = StructLogicalType(std::move(fields));
            return;
        }
        case NProto::TLogicalType::TypeCase::kTuple: {
            std::vector<TLogicalTypePtr> elements;
            for (const auto& protoField : protoLogicalType.tuple().elements()) {
                elements.emplace_back();
                FromProto(&elements.back(), protoField);
            }
            *logicalType = TupleLogicalType(std::move(elements));
            return;
        }
        case NProto::TLogicalType::TypeCase::kVariantTuple: {
            std::vector<TLogicalTypePtr> elements;
            for (const auto& protoElement : protoLogicalType.variant_tuple().elements()) {
                elements.emplace_back();
                FromProto(&elements.back(), protoElement);
            }
            *logicalType = VariantTupleLogicalType(std::move(elements));
            return;
        }
        case NProto::TLogicalType::TypeCase::kVariantStruct: {
            std::vector<TStructField> fields;
            for (const auto& protoField : protoLogicalType.variant_struct().fields()) {
                TLogicalTypePtr fieldType;
                FromProto(&fieldType, protoField.type());
                fields.emplace_back(TStructField{protoField.name(), std::move(fieldType)});
            }
            *logicalType = VariantStructLogicalType(std::move(fields));
            return;
        }
        case NProto::TLogicalType::TypeCase::TYPE_NOT_SET:
            break;
    }
    YT_ABORT();
}

void Serialize(const TStructField& structElement, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer).BeginMap()
        .Item("name").Value(structElement.Name)
        .Item("type").Value(structElement.Type)
    .EndMap();
}

void Deserialize(TStructField& structElement, NYTree::INodePtr node)
{
    const auto& mapNode = node->AsMap();
    structElement.Name = NYTree::ConvertTo<TString>(mapNode->GetChild("name"));
    structElement.Type = NYTree::ConvertTo<TLogicalTypePtr>(mapNode->GetChild("type"));
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
        case ELogicalMetatype::Struct:
        case ELogicalMetatype::VariantStruct: {
            const auto& fields =
                metatype == ELogicalMetatype::Struct ?
                logicalType->AsStructTypeRef().GetFields() :
                logicalType->AsVariantStructTypeRef().GetFields();
            NYTree::BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("metatype").Value(metatype)
                    .Item("fields").Value(fields)
                .EndMap();
            return;
        }
        case ELogicalMetatype::Tuple:
        case ELogicalMetatype::VariantTuple: {
            const auto& elements =
                metatype == ELogicalMetatype::Tuple ?
                logicalType->AsTupleTypeRef().GetElements() :
                logicalType->AsVariantTupleTypeRef().GetElements();
            NYTree::BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("metatype").Value(metatype)
                    .Item("elements").Value(elements)
                .EndMap();
            return;
        }
    }
    YT_ABORT();
}

void Deserialize(TLogicalTypePtr& logicalType, NYTree::INodePtr node)
{
    if (node->GetType() == NYTree::ENodeType::String) {
        auto simpleLogicalType = NYTree::ConvertTo<ESimpleLogicalValueType>(node);
        logicalType = SimpleLogicalType(simpleLogicalType);
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
        case ELogicalMetatype::Struct: {
            auto fieldsNode = mapNode->GetChild("fields");
            auto fields = NYTree::ConvertTo<std::vector<TStructField>>(fieldsNode);
            logicalType = StructLogicalType(std::move(fields));
            return;
        }
        case ELogicalMetatype::Tuple: {
            auto elementsNode = mapNode->GetChild("elements");
            auto elements = NYTree::ConvertTo<std::vector<TLogicalTypePtr>>(elementsNode);
            logicalType = TupleLogicalType(std::move(elements));
            return;
        }
        case ELogicalMetatype::VariantStruct: {
            auto fieldsNode = mapNode->GetChild("fields");
            auto fields = NYTree::ConvertTo<std::vector<TStructField>>(fieldsNode);
            logicalType = VariantStructLogicalType(std::move(fields));
            return;
        }
        case ELogicalMetatype::VariantTuple: {
            auto elementsNode = mapNode->GetChild("elements");
            auto elements = NYTree::ConvertTo<std::vector<TLogicalTypePtr>>(elementsNode);
            logicalType = VariantTupleLogicalType(std::move(elements));
            return;
        }
    }
    YT_ABORT();
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
        YT_VERIFY(it != SimpleTypeMap.end());
        return it->second;
    }

    const TLogicalTypePtr& GetOptionalType(ESimpleLogicalValueType type)
    {
        auto it = OptionalTypeMap.find(type);
        YT_VERIFY(it != OptionalTypeMap.end());
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

TLogicalTypePtr MakeOptionalIfNot(TLogicalTypePtr element)
{
    if (element->GetMetatype() == ELogicalMetatype::Optional) {
        return element;
    }
    return OptionalLogicalType(std::move(element));
}

TLogicalTypePtr SimpleLogicalType(ESimpleLogicalValueType element)
{
    return Singleton<TSimpleTypeStore>()->GetSimpleType(element);
}

TLogicalTypePtr MakeLogicalType(ESimpleLogicalValueType element, bool required)
{
    if (element == ESimpleLogicalValueType::Null) {
        if (required) {
            THROW_ERROR_EXCEPTION("Null type cannot be required");
        }
        return Singleton<TSimpleTypeStore>()->GetSimpleType(element);
    } else if (required) {
        return Singleton<TSimpleTypeStore>()->GetSimpleType(element);
    } else {
        return Singleton<TSimpleTypeStore>()->GetOptionalType(element);
    }
}

TLogicalTypePtr ListLogicalType(TLogicalTypePtr element)
{
    return New<TListLogicalType>(element);
}

TLogicalTypePtr StructLogicalType(std::vector<TStructField> fields)
{
    return New<TStructLogicalType>(std::move(fields));
}

TLogicalTypePtr TupleLogicalType(std::vector<TLogicalTypePtr> elements)
{
    return New<TTupleLogicalType>(std::move(elements));
}

TLogicalTypePtr VariantTupleLogicalType(std::vector<TLogicalTypePtr> elements)
{
    return New<TVariantTupleLogicalType>(std::move(elements));
}

TLogicalTypePtr VariantStructLogicalType(std::vector<TStructField> fields)
{
    return New<TVariantStructLogicalType>(std::move(fields));
}

TLogicalTypePtr NullLogicalType = Singleton<TSimpleTypeStore>()->GetSimpleType(ESimpleLogicalValueType::Null);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

static inline size_t GetHash(
    const THash<NYT::NTableClient::TLogicalType>& hasher,
    const std::vector<NYT::NTableClient::TLogicalTypePtr>& elements)
{
    size_t result = 0;
    for (const auto& element : elements) {
        result = CombineHashes(result, hasher(*element));
    }
    return result;
}

static inline size_t GetHash(
    const THash<NYT::NTableClient::TLogicalType>& hasher,
    const std::vector<NYT::NTableClient::TStructField>& fields)
{
    size_t result = 0;
    for (const auto& field : fields) {
        result = CombineHashes(result, THash<TString>{}(field.Name));
        result = CombineHashes(result, hasher(*field.Type));
    }
    return result;
}

size_t THash<NYT::NTableClient::TLogicalType>::operator()(const NYT::NTableClient::TLogicalType& logicalType) const
{
    using namespace NYT::NTableClient;
    const auto typeHash = static_cast<size_t>(logicalType.GetMetatype());
    switch (logicalType.GetMetatype()) {
        case ELogicalMetatype::Simple:
            return CombineHashes(static_cast<size_t>(logicalType.AsSimpleTypeRef().GetElement()), typeHash);
        case ELogicalMetatype::Optional:
            return CombineHashes((*this)(*logicalType.AsOptionalTypeRef().GetElement()), typeHash);
        case ELogicalMetatype::List:
            return CombineHashes((*this)(*logicalType.AsListTypeRef().GetElement()), typeHash);
        case ELogicalMetatype::Struct:
            return CombineHashes(GetHash(*this, logicalType.AsStructTypeRef().GetFields()), typeHash);
        case ELogicalMetatype::Tuple:
            return CombineHashes(GetHash(*this, logicalType.AsTupleTypeRef().GetElements()), typeHash);
        case ELogicalMetatype::VariantStruct:
            return CombineHashes(GetHash(*this, logicalType.AsVariantStructTypeRef().GetFields()), typeHash);
        case ELogicalMetatype::VariantTuple:
            return CombineHashes(GetHash(*this, logicalType.AsVariantTupleTypeRef().GetElements()), typeHash);
    }
    YT_ABORT();
}
