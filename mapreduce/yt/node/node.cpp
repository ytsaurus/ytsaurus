#include "node.h"

#include "node_io.h"

#include <library/yson/writer.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

bool TNode::TNull::operator==(const TNull&) const {
    return true;
}

////////////////////////////////////////////////////////////////////////////////

bool TNode::TUndefined::operator==(const TUndefined&) const {
    return true;
}

////////////////////////////////////////////////////////////////////////////////

TNode::TNode()
    : Value_(TVariantTypeTag<TUndefined>())
{ }

TNode::TNode(const char* s)
    : Value_(TVariantTypeTag<TString>(), TString(s))
{ }

TNode::TNode(const TStringBuf& s)
    : Value_(TVariantTypeTag<TString>(), TString(s))
{ }

TNode::TNode(TString s)
    : Value_(TVariantTypeTag<TString>(), std::move(s))
{ }

TNode::TNode(int i)
    : Value_(TVariantTypeTag<i64>(), i)
{ }


TNode::TNode(unsigned int ui)
    : Value_(TVariantTypeTag<ui64>(), ui)
{ }

TNode::TNode(long i)
    : Value_(TVariantTypeTag<i64>(), i)
{ }

TNode::TNode(unsigned long ui)
    : Value_(TVariantTypeTag<ui64>(), ui)
{ }

TNode::TNode(long long i)
    : Value_(TVariantTypeTag<i64>(), i)
{ }

TNode::TNode(unsigned long long ui)
    : Value_(TVariantTypeTag<ui64>(), ui)
{ }

TNode::TNode(double d)
    : Value_(TVariantTypeTag<double>(), d)
{ }

TNode::TNode(bool b)
    : Value_(TVariantTypeTag<bool>(), b)
{ }

TNode::TNode(TMapType map)
    : Value_(TVariantTypeTag<TMapType>(), std::move(map))
{ }

TNode::TNode(const TNode& rhs)
    : TNode()
{
    Copy(rhs);
}

TNode& TNode::operator=(const TNode& rhs)
{
    if (this != &rhs) {
        Clear();
        Copy(rhs);
    }
    return *this;
}

TNode::TNode(TNode&& rhs)
    : TNode()
{
    if (this != &rhs) {
        Move(std::move(rhs));
    }
}

TNode& TNode::operator=(TNode&& rhs)
{
    if (this != &rhs) {
        Move(std::move(rhs));
    }
    return *this;
}

TNode::~TNode() = default;

void TNode::Clear()
{
    ClearAttributes();
    Value_ = TUndefined();
}

bool TNode::IsString() const
{
    return HoldsAlternative<TString>(Value_);
}

bool TNode::IsInt64() const
{
    return HoldsAlternative<i64>(Value_);
}

bool TNode::IsUint64() const
{
    return HoldsAlternative<ui64>(Value_);
}

bool TNode::IsDouble() const
{
    return HoldsAlternative<double>(Value_);
}

bool TNode::IsBool() const
{
    return HoldsAlternative<bool>(Value_);
}

bool TNode::IsList() const
{
    return HoldsAlternative<TListType>(Value_);
}

bool TNode::IsMap() const
{
    return HoldsAlternative<TMapType>(Value_);
}

bool TNode::IsEntity() const
{
    return IsNull();
}

bool TNode::IsNull() const
{
    return HoldsAlternative<TNull>(Value_);
}

bool TNode::IsUndefined() const
{
    return HoldsAlternative<TUndefined>(Value_);
}

bool TNode::Empty() const
{
    switch (GetType()) {
        case String:
            return Get<TString>(Value_).empty();
        case List:
            return Get<TListType>(Value_).empty();
        case Map:
            return Get<TMapType>(Value_).empty();
        default:
            ythrow TTypeError() << "Empty() called for type " << GetType();
    }
}

size_t TNode::Size() const
{
    switch (GetType()) {
        case String:
            return Get<TString>(Value_).size();
        case List:
            return Get<TListType>(Value_).size();
        case Map:
            return Get<TMapType>(Value_).size();
        default:
            ythrow TTypeError() << "Size() called for type " << GetType();
    }
}

TNode::EType TNode::GetType() const
{
    switch (Value_.index()) {
        case TValue::TagOf<TUndefined>():
            return Undefined;
        case TValue::TagOf<TString>():
            return String;
        case TValue::TagOf<i64>():
            return Int64;
        case TValue::TagOf<ui64>():
            return Uint64;
        case TValue::TagOf<double>():
            return Double;
        case TValue::TagOf<bool>():
            return Bool;
        case TValue::TagOf<TListType>():
            return List;
        case TValue::TagOf<TMapType>():
            return Map;
        case TValue::TagOf<TNull>():
            return Null;
    }
    Y_UNREACHABLE();
}

const TString& TNode::AsString() const
{
    CheckType(String);
    return Get<TString>(Value_);
}

i64 TNode::AsInt64() const
{
    CheckType(Int64);
    return Get<i64>(Value_);
}

ui64 TNode::AsUint64() const
{
    CheckType(Uint64);
    return Get<ui64>(Value_);
}

double TNode::AsDouble() const
{
    CheckType(Double);
    return Get<double>(Value_);
}

bool TNode::AsBool() const
{
    CheckType(Bool);
    return Get<bool>(Value_);
}

const TNode::TListType& TNode::AsList() const
{
    CheckType(List);
    return Get<TListType>(Value_);
}

const TNode::TMapType& TNode::AsMap() const
{
    CheckType(Map);
    return Get<TMapType>(Value_);
}

TNode::TListType& TNode::AsList()
{
    CheckType(List);
    return Get<TListType>(Value_);
}

TNode::TMapType& TNode::AsMap()
{
    CheckType(Map);
    return Get<TMapType>(Value_);
}

const TString& TNode::UncheckedAsString() const noexcept
{
    return Get<TString>(Value_);
}

i64 TNode::UncheckedAsInt64() const noexcept
{
    return Get<i64>(Value_);
}

ui64 TNode::UncheckedAsUint64() const noexcept
{
    return Get<ui64>(Value_);
}

double TNode::UncheckedAsDouble() const noexcept
{
    return Get<double>(Value_);
}

bool TNode::UncheckedAsBool() const noexcept
{
    return Get<bool>(Value_);
}

const TNode::TListType& TNode::UncheckedAsList() const noexcept
{
    return Get<TListType>(Value_);
}

const TNode::TMapType& TNode::UncheckedAsMap() const noexcept
{
    return Get<TMapType>(Value_);
}

TNode::TListType& TNode::UncheckedAsList() noexcept
{
    return Get<TListType>(Value_);
}

TNode::TMapType& TNode::UncheckedAsMap() noexcept
{
    return Get<TMapType>(Value_);
}

TNode TNode::CreateList()
{
    TNode node;
    node.Value_ = TValue(TVariantTypeTag<TListType>());
    return node;
}

TNode TNode::CreateMap()
{
    TNode node;
    node.Value_ = TValue(TVariantTypeTag<TMapType>());
    return node;
}

TNode TNode::CreateEntity()
{
    TNode node;
    node.Value_ = TValue(TVariantTypeTag<TNull>());
    return node;
}

const TNode& TNode::operator[](size_t index) const
{
    CheckType(List);
    return Get<TListType>(Value_)[index];
}

TNode& TNode::operator[](size_t index)
{
    CheckType(List);
    return Get<TListType>(Value_)[index];
}

TNode& TNode::Add() &
{
    AssureList();
    return Get<TListType>(Value_).emplace_back();
}

TNode TNode::Add() &&
{
    return std::move(Add());
}

TNode& TNode::Add(const TNode& node) &
{
    AssureList();
    Get<TListType>(Value_).emplace_back(node);
    return *this;
}

TNode TNode::Add(const TNode& node) &&
{
    return std::move(Add(node));
}

TNode& TNode::Add(TNode&& node) &
{
    AssureList();
    Get<TListType>(Value_).emplace_back(std::move(node));
    return *this;
}

TNode TNode::Add(TNode&& node) &&
{
    return std::move(Add(std::move(node)));
}

bool TNode::HasKey(const TStringBuf key) const
{
    CheckType(Map);
    return Get<TMapType>(Value_).contains(key);
}

TNode& TNode::operator()(const TString& key, const TNode& value) &
{
    AssureMap();
    Get<TMapType>(Value_)[key] = value;
    return *this;
}

TNode TNode::operator()(const TString& key, const TNode& value) &&
{
    return std::move(operator()(key, value));
}

TNode& TNode::operator()(const TString& key, TNode&& value) &
{
    AssureMap();
    Get<TMapType>(Value_)[key] = std::move(value);
    return *this;
}

TNode TNode::operator()(const TString& key, TNode&& value) &&
{
    return std::move(operator()(key, std::move(value)));
}

const TNode& TNode::operator[](const TStringBuf key) const
{
    CheckType(Map);
    static TNode notFound;
    const auto& map = Get<TMapType>(Value_);
    TMapType::const_iterator i = map.find(key);
    if (i == map.end()) {
        return notFound;
    } else {
        return i->second;
    }
}

TNode& TNode::operator[](const TStringBuf key)
{
    AssureMap();
    return Get<TMapType>(Value_)[key];
}

const TNode& TNode::At(const TStringBuf key) const {
    CheckType(Map);
    const auto& map = Get<TMapType>(Value_);
    TMapType::const_iterator i = map.find(key);
    if (i == map.end()) {
        throw yexception() << "Cannot find key " << key;
    } else {
        return i->second;
    }
}

bool TNode::HasAttributes() const
{
    return Attributes_ && !Attributes_->Empty();
}

void TNode::ClearAttributes()
{
    if (Attributes_) {
        Attributes_.Destroy();
    }
}

const TNode& TNode::GetAttributes() const
{
    static TNode notFound = TNode::CreateMap();
    if (!Attributes_) {
        return notFound;
    }
    return *Attributes_;
}

TNode& TNode::Attributes()
{
    if (!Attributes_) {
        CreateAttributes();
    }
    return *Attributes_;
}

void TNode::MoveWithoutAttributes(TNode&& rhs)
{
    Value_ = std::move(rhs.Value_);
    rhs.Clear();
}

void TNode::Copy(const TNode& rhs)
{
    if (rhs.Attributes_) {
        if (!Attributes_) {
            CreateAttributes();
        }
        *Attributes_ = *rhs.Attributes_;
    }

    Value_ = rhs.Value_;
}

void TNode::Move(TNode&& rhs)
{
    Value_ = std::move(rhs.Value_);
    Attributes_ = std::move(rhs.Attributes_);
}

void TNode::CheckType(EType type) const
{
    Y_ENSURE_EX(GetType() == type,
        TTypeError() << "TNode type " << type <<  " expected, actual type " << GetType();
    );
}

void TNode::AssureMap()
{
    if (HoldsAlternative<TUndefined>(Value_)) {
        Value_ = TMapType();
    } else {
        CheckType(Map);
    }
}

void TNode::AssureList()
{
    if (HoldsAlternative<TUndefined>(Value_)) {
        Value_ = TListType();
    } else {
        CheckType(List);
    }
}

void TNode::CreateAttributes()
{
    Attributes_ = new TNode;
    Attributes_->Value_ = TMapType();
}

void TNode::Save(IOutputStream* out) const
{
    NodeToYsonStream(*this, out, YF_BINARY);
}

void TNode::Load(IInputStream* in)
{
    Clear();
    *this = NodeFromYsonStream(in, YT_NODE);
}

////////////////////////////////////////////////////////////////////////////////

bool operator==(const TNode& lhs, const TNode& rhs)
{
    if (HoldsAlternative<TNode::TUndefined>(lhs.Value_) ||
        HoldsAlternative<TNode::TUndefined>(rhs.Value_))
    {
        // TODO: should try to remove this behaviour if nobody uses it.
        return false;
    }

    if (lhs.GetType() != rhs.GetType()) {
        return false;
    }

    if (lhs.Attributes_) {
        if (rhs.Attributes_) {
            if (*lhs.Attributes_ != *rhs.Attributes_) {
                return false;
            }
        } else {
            return false;
        }
    } else {
        if (rhs.Attributes_) {
            return false;
        }
    }

    return rhs.Value_ == lhs.Value_;
}

bool operator!=(const TNode& lhs, const TNode& rhs)
{
    return !(lhs == rhs);
}

bool GetBool(const TNode& node)
{
    if (node.IsBool()) {
        return node.AsBool();
    } else if (node.IsString()) {
        return node.AsString() == "true";
    } else {
        ythrow TNode::TTypeError()
            << "GetBool(): not a boolean or string type";
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
