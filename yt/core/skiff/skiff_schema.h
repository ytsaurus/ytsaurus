#pragma once

#include "public.h"

#include <yt/core/misc/error.h>

#include <util/generic/string.h>
#include <util/string/cast.h>

namespace NYT::NSkiff {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TSimpleTypeSchema);

template <EWireType WireType>
class TComplexSchema;

using TSkiffSchemaList = std::vector<TSkiffSchemaPtr>;

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
    : public TIntrinsicRefCounted
{
public:
    EWireType GetWireType() const;
    TSkiffSchemaPtr SetName(TString name);
    const TString& GetName() const;

    virtual TSkiffSchemaList GetChildren() const;

protected:
    explicit TSkiffSchema(EWireType type);

private:
    const EWireType Type_;
    TString Name_;
};

DEFINE_REFCOUNTED_TYPE(TSkiffSchema);

////////////////////////////////////////////////////////////////////////////////

class TSimpleTypeSchema
    : public TSkiffSchema
{
public:
    explicit TSimpleTypeSchema(EWireType type);
};

DEFINE_REFCOUNTED_TYPE(TSimpleTypeSchema);

////////////////////////////////////////////////////////////////////////////////

template <EWireType WireType>
class TComplexSchema
    : public TSkiffSchema
{
public:
    explicit TComplexSchema(TSkiffSchemaList elements);

    virtual TSkiffSchemaList GetChildren() const override;

private:
    const TSkiffSchemaList Elements_;
};

////////////////////////////////////////////////////////////////////////////////

bool IsSimpleType(EWireType type);
TString GetShortDebugString(const TSkiffSchemaPtr& schema);
void PrintShortDebugString(const TSkiffSchemaPtr& schema, IOutputStream* out);

TSimpleTypeSchemaPtr CreateSimpleTypeSchema(EWireType type);
TTupleSchemaPtr CreateTupleSchema(TSkiffSchemaList children);
TVariant8SchemaPtr CreateVariant8Schema(TSkiffSchemaList children);
TVariant16SchemaPtr CreateVariant16Schema(TSkiffSchemaList children);
TRepeatedVariant16SchemaPtr CreateRepeatedVariant16Schema(TSkiffSchemaList children);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSkiff

#define SKIFF_SCHEMA_H
#include "skiff_schema-inl.h"
#undef SKIFF_SCHEMA_H
