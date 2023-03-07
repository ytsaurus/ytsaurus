#pragma once

#include "config.h"
#include "private.h"

#include <contrib/libs/protobuf/descriptor.h>
#include <contrib/libs/protobuf/google/protobuf/descriptor.pb.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

class TEnumerationDescription
{
public:
    explicit TEnumerationDescription(const TString& name);

    const TString& GetEnumerationName() const;
    const TString& GetValueName(i32 value) const;
    i32 GetValue(TStringBuf name) const;
    void Add(TString name, i32 value);

private:
    THashMap<TString, i32> NameToValue_;
    THashMap<i32, TString> ValueToName_;
    TString Name_;
};

////////////////////////////////////////////////////////////////////////////////

struct TProtobufFieldDescriptionBase
{
    TString Name;
    EProtobufType Type;
    ui64 WireTag;
    size_t TagSize;

    const TEnumerationDescription* EnumerationDescription;

    // Number of fields in struct in schema (only for |Type == StructuredMessage|).
    int StructElementCount;

    // Index of field inside struct (for fields corresponding to struct fields in schema).
    int StructElementIndex;

    // Is field repeated?
    bool Repeated;

    // Is a repeated field packed (i.e. it is encoded as `<tag> <length> <value1> ... <valueK>`)?
    bool Packed;

    // Is the corresponding type in schema optional?
    bool Optional;

    // Extracts field number from |WireTag|.
    ui32 GetFieldNumber() const;
};

struct TProtobufFieldDescription
    : public TProtobufFieldDescriptionBase
{
    std::vector<TProtobufFieldDescription> Children;
};

////////////////////////////////////////////////////////////////////////////////


struct TProtobufTableDescription
{
    THashMap<TString, TProtobufFieldDescription> Columns;
};

////////////////////////////////////////////////////////////////////////////////

class TProtobufFormatDescription
    : public TRefCounted
{
public:
    TProtobufFormatDescription() = default;

    void Init(
        const TProtobufFormatConfigPtr& config,
        const std::vector<NTableClient::TTableSchema>& schemas,
        bool validateMissingFieldsOptionality);
    const TProtobufTableDescription& GetTableDescription(ui32 tableIndex) const;
    size_t GetTableCount() const;

private:
    void InitFromFileDescriptors(const TProtobufFormatConfigPtr& config);
    void InitFromProtobufSchema(
        const TProtobufFormatConfigPtr& config,
        const std::vector<NTableClient::TTableSchema>& schemas,
        bool validateMissingFieldsOptionality);

    // Initialize field description from column config and logical type from schema.
    // Matching of the config and type is performed by the following rules:
    //  * Field of simple type matches simple type T "naturally"
    //  * Repeated field matches List<T> iff corresponding non-repeated field matches T and T is not Optional<...>
    //  * Non-repeated field matches Optional<T> iff it matches T and T is not Optional<...>
    //  * StructuredMessage field matches Struct<Name1: Type1, ..., NameN: TypeN> iff
    //      - the field has subfields whose names are in set {Name1, ..., NameN}
    //      - the subfield with name NameK matches TypeK
    //      - if |validateMissingFieldsOptionality| is |true|,
    //        for each name NameK missing from subfields TypeK is Optional<...>
    void InitField(
        TProtobufFieldDescription* field,
        const TProtobufColumnConfigPtr& columnConfig,
        NTableClient::TLogicalTypePtr logicalType,
        int elementIndex,
        bool validateMissingFieldsOptionality);

    // Initialize field description from column config without matching it with logical type.
    // StructuredMessage and repeated fields must _not_ be initialized solely by this method.
    void InitSchemalessField(
        TProtobufFieldDescription* field,
        const TProtobufColumnConfigPtr& columnConfig);

private:
    std::vector<TProtobufTableDescription> Tables_;
    THashMap<TString, TEnumerationDescription> EnumerationDescriptionMap_;
};

DEFINE_REFCOUNTED_TYPE(TProtobufFormatDescription)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
