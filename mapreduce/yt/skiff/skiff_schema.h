#pragma once

#include "public.h"
#include "wire_type.h"

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/cast.h>

////////////////////////////////////////////////////////////////////////////////

template <>
struct THash<NSkiff::TSkiffSchema>
{
    size_t operator()(const NSkiff::TSkiffSchema& schema) const;
};

////////////////////////////////////////////////////////////////////////////////

namespace NSkiff {

////////////////////////////////////////////////////////////////////////////////

class TSimpleTypeSchema;
using TSimpleTypeSchemaPtr = TIntrusivePtr<TSimpleTypeSchema>;

template <EWireType WireType>
class TComplexSchema;

using TSkiffSchemaList = TVector<TSkiffSchemaPtr>;

using TTupleSchema = TComplexSchema<EWireType::Tuple>;
using TTupleSchemaPtr = TIntrusivePtr<TTupleSchema>;
using TVariant8Schema = TComplexSchema<EWireType::Variant8>;
using TVariant8SchemaPtr = TIntrusivePtr<TVariant8Schema>;
using TVariant16Schema = TComplexSchema<EWireType::Variant16>;
using TVariant16SchemaPtr = TIntrusivePtr<TVariant16Schema>;
using TRepeatedVariant16Schema = TComplexSchema<EWireType::RepeatedVariant16>;
using TRepeatedVariant16SchemaPtr = TIntrusivePtr<TRepeatedVariant16Schema>;

////////////////////////////////////////////////////////////////////////////////

class TSkiffSchema
    : public TThrRefBase
{
public:
    EWireType GetWireType() const;
    TSkiffSchemaPtr SetName(TString name);
    const TString& GetName() const;

    virtual const TSkiffSchemaList& GetChildren() const;

protected:
    explicit TSkiffSchema(EWireType type);

private:
    const EWireType Type_;
    TString Name_;
};

bool operator==(const TSkiffSchema& lhs, const TSkiffSchema& rhs);

////////////////////////////////////////////////////////////////////////////////

class TSimpleTypeSchema
    : public TSkiffSchema
{
public:
    explicit TSimpleTypeSchema(EWireType type);
};

////////////////////////////////////////////////////////////////////////////////

template <EWireType WireType>
class TComplexSchema
    : public TSkiffSchema
{
public:
    explicit TComplexSchema(TSkiffSchemaList elements);

    virtual const TSkiffSchemaList& GetChildren() const override;

private:
    const TSkiffSchemaList Elements_;
};

////////////////////////////////////////////////////////////////////////////////

bool IsSimpleType(EWireType type);
TString GetShortDebugString(const TSkiffSchema& schema);
TString GetShortDebugString(const TSkiffSchemaPtr& schema);
void PrintShortDebugString(const TSkiffSchema& schema, IOutputStream* out);
void PrintShortDebugString(const TSkiffSchemaPtr& schema, IOutputStream* out);

TSimpleTypeSchemaPtr CreateSimpleTypeSchema(EWireType type);
TTupleSchemaPtr CreateTupleSchema(TSkiffSchemaList children);
TVariant8SchemaPtr CreateVariant8Schema(TSkiffSchemaList children);
TVariant16SchemaPtr CreateVariant16Schema(TSkiffSchemaList children);
TRepeatedVariant16SchemaPtr CreateRepeatedVariant16Schema(TSkiffSchemaList children);

////////////////////////////////////////////////////////////////////////////////

struct TSkiffSchemaPtrHasher
{
    size_t operator()(const NSkiff::TSkiffSchemaPtr& schema) const
    {
        return THash<NSkiff::TSkiffSchema>()(*schema);
    }
};

struct TSkiffSchemaPtrEqual
{
    size_t operator()(const NSkiff::TSkiffSchemaPtr& lhs, const NSkiff::TSkiffSchemaPtr& rhs) const
    {
        return *lhs == *rhs;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkiff

#define SKIFF_SCHEMA_H
#include "skiff_schema-inl.h"
#undef SKIFF_SCHEMA_H
