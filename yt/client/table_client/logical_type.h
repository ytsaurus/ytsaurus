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

    // Variant with named elements.
    (VariantStruct)

    // Variant with unnamed elements.
    (VariantTuple)

    // Dict is effectively list of pairs. We have restrictions on the key type.
    // YT doesn't check uniqueness of the keys.
    (Dict)
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
    const TVariantTupleLogicalType& AsVariantTupleTypeRef() const;
    const TVariantStructLogicalType& AsVariantStructTypeRef() const;
    const TDictLogicalType& AsDictTypeRef() const;

    virtual size_t GetMemoryUsage() const = 0;
    virtual int GetTypeComplexity() const = 0;

    // This function doesn't validate children of current node.
    // Users should use ValidateLogicalType function.
    virtual void ValidateNode() const = 0;

    // Whether or not element might be null.
    virtual bool IsNullable() const = 0;

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
// The second element of resut is false if logicalType is Null or it is optional<A> where A is any type otherwise it's true.
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

    // Cached value of GetElement()->IsNullable(), useful for performance reasons.
    bool IsElementNullable() const;

    virtual size_t GetMemoryUsage() const override;
    virtual int GetTypeComplexity() const override;
    virtual void ValidateNode() const override;
    virtual bool IsNullable() const override;

private:
    const TLogicalTypePtr Element_;
    const bool ElementIsNullable_;
};

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
    virtual bool IsNullable() const override;

private:
    ESimpleLogicalValueType Element_;
};

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
    virtual bool IsNullable() const override;

private:
    TLogicalTypePtr Element_;
};

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
    TComplexTypeFieldDescriptor VariantTupleElement(size_t i) const;
    TComplexTypeFieldDescriptor VariantStructField(size_t i) const;
    TComplexTypeFieldDescriptor DictKey() const;
    TComplexTypeFieldDescriptor DictValue() const;

    const TString& GetDescription() const;
    const TLogicalTypePtr& GetType() const;

    void Walk(std::function<void(const TComplexTypeFieldDescriptor&)> onElement) const;

private:
    TString Descriptor_;
    TLogicalTypePtr Type_;
};

////////////////////////////////////////////////////////////////////////////////

struct TStructField
{
    TString Name;
    TLogicalTypePtr Type;
};

////////////////////////////////////////////////////////////////////////////////

// Base class for struct and named variant.
class TStructLogicalTypeBase
    : public TLogicalType
{
public:

public:
    TStructLogicalTypeBase(ELogicalMetatype metatype, std::vector<TStructField> fields);
    const std::vector<TStructField>& GetFields() const;

    virtual size_t GetMemoryUsage() const override;
    virtual int GetTypeComplexity() const override;
    virtual void ValidateNode() const override;
    virtual bool IsNullable() const override;

private:
    std::vector<TStructField> Fields_;
};

////////////////////////////////////////////////////////////////////////////////

class TTupleLogicalTypeBase
    : public TLogicalType
{
public:
    explicit TTupleLogicalTypeBase(ELogicalMetatype metatype, std::vector<TLogicalTypePtr> elements);

    const std::vector<TLogicalTypePtr>& GetElements() const;

    virtual size_t GetMemoryUsage() const override;
    virtual int GetTypeComplexity() const override;
    virtual void ValidateNode() const override;
    virtual bool IsNullable() const override;

private:
    std::vector<TLogicalTypePtr> Elements_;
};

////////////////////////////////////////////////////////////////////////////////

class TStructLogicalType
    : public TStructLogicalTypeBase
{
public:
    TStructLogicalType(std::vector<TStructField> fields);
};

////////////////////////////////////////////////////////////////////////////////

class TTupleLogicalType
    : public TTupleLogicalTypeBase
{
public:
    TTupleLogicalType(std::vector<TLogicalTypePtr> elements);
};

////////////////////////////////////////////////////////////////////////////////

class TVariantStructLogicalType
    : public TStructLogicalTypeBase
{
public:
    explicit TVariantStructLogicalType(std::vector<TStructField> fields);
};

////////////////////////////////////////////////////////////////////////////////

class TVariantTupleLogicalType
    : public TTupleLogicalTypeBase
{
public:
    explicit TVariantTupleLogicalType(std::vector<TLogicalTypePtr> elements);
};

////////////////////////////////////////////////////////////////////////////////

class TDictLogicalType
    : public TLogicalType
{
public:
    TDictLogicalType(TLogicalTypePtr key, TLogicalTypePtr value);

    const TLogicalTypePtr& GetKey() const;
    const TLogicalTypePtr& GetValue() const;

    virtual size_t GetMemoryUsage() const override;
    virtual int GetTypeComplexity() const override;
    virtual void ValidateNode() const override;
    virtual bool IsNullable() const override;

private:
    TLogicalTypePtr Key_;
    TLogicalTypePtr Value_;
};

////////////////////////////////////////////////////////////////////////////////

extern const TLogicalTypePtr NullLogicalType;

////////////////////////////////////////////////////////////////////////////////

TLogicalTypePtr SimpleLogicalType(ESimpleLogicalValueType element);
TLogicalTypePtr OptionalLogicalType(TLogicalTypePtr element);
TLogicalTypePtr ListLogicalType(TLogicalTypePtr element);
TLogicalTypePtr StructLogicalType(std::vector<TStructField> fields);
TLogicalTypePtr TupleLogicalType(std::vector<TLogicalTypePtr> elements);
TLogicalTypePtr VariantStructLogicalType(std::vector<TStructField> fields);
TLogicalTypePtr VariantTupleLogicalType(std::vector<TLogicalTypePtr> elements);
TLogicalTypePtr DictLogicalType(TLogicalTypePtr key, TLogicalTypePtr value);

TLogicalTypePtr MakeOptionalIfNot(TLogicalTypePtr element);

// Creates logical type from legacy schema fields.
// IMPORTANT: Only used for compatibility reasons.
// In modern code, one should use OptionalLogicalType + SimpleLogicalType instead.
TLogicalTypePtr MakeLogicalType(ESimpleLogicalValueType type, bool required);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

template <>
struct THash<NYT::NTableClient::TLogicalType>
{
    size_t operator() (const NYT::NTableClient::TLogicalType& logicalType) const;
};
