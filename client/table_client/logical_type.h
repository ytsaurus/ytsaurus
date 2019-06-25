#pragma once

#include "public.h"

#include <yt/client/table_client/row_base.h>

#include <yt/core/yson/public.h>
#include <yt/core/ytree/public.h>

#include <yt/core/misc/enum.h>
#include <yt/core/misc/ref_counted.h>

#include <util/generic/hash.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ELogicalMetatype,
    (Simple)
    (Optional)
    (List)
    (Struct)
    (Tuple)
    // In the future there will be Variant, Map, etc
);

class TLogicalType
    : public virtual TIntrinsicRefCounted
{
public:
    explicit TLogicalType(ELogicalMetatype type);
    ELogicalMetatype GetMetatype() const;

    const TSimpleLogicalType& AsSimpleTypeRef() const;
    const TOptionalLogicalType& AsOptionalTypeRef() const;
    const TListLogicalType& AsListTypeRef() const;
    const TStructLogicalType& AsStructTypeRef() const;
    const TTupleLogicalType& AsTupleTypeRef() const;

    virtual size_t GetMemoryUsage() const = 0;
    virtual int GetTypeComplexity() const = 0;

    // This function doesn't validate children of current node.
    // Users should use ValidateLogicalType function.
    virtual void ValidateNode() const = 0;

private:
    const ELogicalMetatype Metatype_;
};

DEFINE_REFCOUNTED_TYPE(TLogicalType)

TString ToString(const TLogicalType& logicalType);

bool operator == (const TLogicalType& lhs, const TLogicalType& rhs);
bool operator != (const TLogicalType& lhs, const TLogicalType& rhs);
bool operator == (const TLogicalTypePtr& lhs, const TLogicalTypePtr& rhs) = delete;

void ValidateAlterType(const TLogicalTypePtr& oldType, const TLogicalTypePtr& newType);

void ValidateLogicalType(const TComplexTypeFieldDescriptor& descriptor);

//! Returns true if #lhs type is subtype of #rhs type.
//! We say that #lhs type is subtype of #rhs type
//! iff every value that belongs to #lhs type also belongs to #rhs type.
bool IsSubtypeOf(const TLogicalTypePtr& lhs, const TLogicalTypePtr& rhs);

// Function converts new type to old typesystem if possible.
// The first element of result is ESimpleLogicalValue type corresponding to logicalType
// if logicalType is either T or optional<T> and T is simple. Otherwise the first element of result is nullopt.
// The second element of resut is false if logicalType is optional<A> where A is any type otherwise it's true.
std::pair<std::optional<ESimpleLogicalValueType>, bool> SimplifyLogicalType(const TLogicalTypePtr& logicalType);

void ToProto(NProto::TLogicalType* protoLogicalType, const TLogicalTypePtr& logicalType);
void FromProto(TLogicalTypePtr* logicalType, const NProto::TLogicalType& protoLogicalType);

void Serialize(const TLogicalTypePtr& schema, NYson::IYsonConsumer* consumer);
void Deserialize(TLogicalTypePtr& schema, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

class TOptionalLogicalType
    : public TLogicalType
{
public:
    explicit TOptionalLogicalType(TLogicalTypePtr element);

    const TLogicalTypePtr& GetElement() const;

    std::optional<ESimpleLogicalValueType> Simplify() const;

    virtual size_t GetMemoryUsage() const override;
    virtual int GetTypeComplexity() const override;
    virtual void ValidateNode() const override;

private:
    const TLogicalTypePtr Element_;
};
DEFINE_REFCOUNTED_TYPE(TOptionalLogicalType);

////////////////////////////////////////////////////////////////////////////////

class TSimpleLogicalType
    : public TLogicalType
{
public:
    explicit TSimpleLogicalType(ESimpleLogicalValueType element);

    ESimpleLogicalValueType GetElement() const;

    virtual size_t GetMemoryUsage() const override;
    virtual int GetTypeComplexity() const override;
    virtual void ValidateNode() const override;

private:
    ESimpleLogicalValueType Element_;
};
DEFINE_REFCOUNTED_TYPE(TSimpleLogicalType);

////////////////////////////////////////////////////////////////////////////////

class TListLogicalType
    : public TLogicalType
{
public:
    explicit TListLogicalType(TLogicalTypePtr element);

    const TLogicalTypePtr& GetElement() const;

    virtual size_t GetMemoryUsage() const override;
    virtual int GetTypeComplexity() const override;
    virtual void ValidateNode() const override;

private:
    TLogicalTypePtr Element_;
};
DEFINE_REFCOUNTED_TYPE(TListLogicalType);

////////////////////////////////////////////////////////////////////////////////

// Class builds descriptors of a complex value field.
// Such descriptors are useful for generating error messages when working with complex types.
class TComplexTypeFieldDescriptor
{
public:
    explicit TComplexTypeFieldDescriptor(TLogicalTypePtr type);
    TComplexTypeFieldDescriptor(TString columnName, TLogicalTypePtr type);

    TComplexTypeFieldDescriptor OptionalElement() const;
    TComplexTypeFieldDescriptor ListElement() const;
    TComplexTypeFieldDescriptor StructField(size_t i) const;
    TComplexTypeFieldDescriptor TupleElement(size_t i) const;

    const TString& GetDescription() const;
    const TLogicalTypePtr& GetType() const;

    void Walk(std::function<void(const TComplexTypeFieldDescriptor&)> onElement) const;

private:
    TString Descriptor_;
    TLogicalTypePtr Type_;
};

////////////////////////////////////////////////////////////////////////////////

class TStructLogicalType
    : public TLogicalType
{
public:
    struct TField
    {
        TString Name;
        TLogicalTypePtr Type;
    };

public:
    explicit TStructLogicalType(std::vector<TField> fields);
    const std::vector<TField>& GetFields() const;

    virtual size_t GetMemoryUsage() const override;
    virtual int GetTypeComplexity() const override;
    virtual void ValidateNode() const override;

private:
    std::vector<TField> Fields_;
};
DEFINE_REFCOUNTED_TYPE(TStructLogicalType);

////////////////////////////////////////////////////////////////////////////////

class TTupleLogicalType
    : public TLogicalType
{
public:
    explicit TTupleLogicalType(std::vector<TLogicalTypePtr> elements);

    const std::vector<TLogicalTypePtr>& GetElements() const;

    virtual size_t GetMemoryUsage() const override;
    virtual int GetTypeComplexity() const override;
    virtual void ValidateNode() const override;

private:
    std::vector<TLogicalTypePtr> Elements_;
};
DEFINE_REFCOUNTED_TYPE(TTupleLogicalType);

////////////////////////////////////////////////////////////////////////////////

extern TLogicalTypePtr NullLogicalType;

////////////////////////////////////////////////////////////////////////////////

TLogicalTypePtr SimpleLogicalType(ESimpleLogicalValueType element, bool required = true);
TLogicalTypePtr OptionalLogicalType(TLogicalTypePtr element);
TLogicalTypePtr ListLogicalType(TLogicalTypePtr element);
TLogicalTypePtr StructLogicalType(std::vector<TStructLogicalType::TField> fields);
TLogicalTypePtr TupleLogicalType(std::vector<TLogicalTypePtr> fields);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

template <>
struct THash<NYT::NTableClient::TLogicalType>
{
    size_t operator() (const NYT::NTableClient::TLogicalType& logicalType) const;
};
