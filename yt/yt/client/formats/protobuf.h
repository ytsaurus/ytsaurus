#pragma once

#include "config.h"
#include "private.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>

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

class TProtobufFieldDescriptionBase
{
public:
    TString Name;
    
    ui64 WireTag = 0;
    size_t TagSize = 0;

    // Index of field inside struct (for fields corresponding to struct fields in schema).
    int StructFieldIndex = 0;

    // Is field repeated?
    bool Repeated = false;

    // Is a repeated field packed (i.e. it is encoded as `<tag> <length> <value1> ... <valueK>`)?
    bool Packed = false;

    // Extracts field number from |WireTag|.
    ui32 GetFieldNumber() const;
};

class TProtobufTypeBase
    : public TRefCounted
{
public:
    EProtobufType ProtoType = EProtobufType::Int64;

    // Is the corresponding type in schema optional?
    bool Optional = true;

    // Number of fields in struct in schema (only for |Type == StructuredMessage|).
    int StructFieldCount = 0;

    const TEnumerationDescription* EnumerationDescription = nullptr;
};

class TProtobufWriterType
    : public TProtobufTypeBase
{
public:
    void AddChild(
        const std::optional<NTableClient::TComplexTypeFieldDescriptor>& descriptor,
        std::unique_ptr<TProtobufWriterFieldDescription> childType,
        std::optional<int> fieldIndex);

    void IgnoreChild(
        const std::optional<NTableClient::TComplexTypeFieldDescriptor>& descriptor,
        int fieldNumber);

    const TProtobufWriterFieldDescription* FindAlternative(int alternativeIndex) const;

public:
    std::vector<std::unique_ptr<TProtobufWriterFieldDescription>> Children;

private:
    std::vector<int> AlternativeToChildIndex_;
    static constexpr int InvalidChildIndex = -1;
};

DEFINE_REFCOUNTED_TYPE(TProtobufWriterType)

class TProtobufWriterFieldDescription
    : public TProtobufFieldDescriptionBase
{
public:
    TProtobufWriterTypePtr Type;
};

////////////////////////////////////////////////////////////////////////////////

class TProtobufParserType
    : public TProtobufTypeBase
{
public:
    void AddChild(
        const std::optional<NTableClient::TComplexTypeFieldDescriptor>& descriptor,
        std::unique_ptr<TProtobufParserFieldDescription> childType,
        std::optional<int> fieldIndex);

    void IgnoreChild(
        const std::optional<NTableClient::TComplexTypeFieldDescriptor>& descriptor,
        int fieldNumber);

    // Returns std::nullopt iff the field is ignored (missing from table schema).
    // Throws an exception iff the field number is unknown.
    std::optional<int> FieldNumberToChildIndex(int fieldNumber) const;

private:
    void SetChildIndex(
        const std::optional<NTableClient::TComplexTypeFieldDescriptor>& descriptor,
        int fieldNumber,
        int childIndex);

public:
    // For oneof types -- corresponding oneof description.
    const TProtobufParserFieldDescription* Field = nullptr;

    std::vector<std::unique_ptr<TProtobufParserFieldDescription>> Children;
    
private:
    static constexpr int InvalidChildIndex = -1;
    static constexpr int IgnoredChildIndex = -2;
    static constexpr int MaxFieldNumberVectorSize = 256;

    std::vector<int> IgnoredChildFieldNumbers_;
    std::vector<std::unique_ptr<TProtobufParserFieldDescription>> OneofDescriptions_;
    std::vector<int> FieldNumberToChildIndexVector_;
    THashMap<int, int> FieldNumberToChildIndexMap_;
};

DEFINE_REFCOUNTED_TYPE(TProtobufParserType)


class TProtobufParserFieldDescription
    : public TProtobufFieldDescriptionBase
{
public:
    bool IsOneofAlternative() const
    {
        return AlternativeIndex.has_value();
    }

public:
    TProtobufParserTypePtr Type;

    // For oneof members -- index of alternative.
    std::optional<int> AlternativeIndex;
    // For oneof members -- containing oneof type.
    const TProtobufParserType* ContainingOneof;
};

template <typename TType>
class TProtobufTypeBuilder
{
public:
    static constexpr bool IsWriter = std::is_same_v<TType, TProtobufWriterType>;
    static constexpr bool AreNonOptionalMissingFieldsAllowed = IsWriter;

    using TTypePtr = NYT::TIntrusivePtr<TType>;
    using TField = std::conditional_t<IsWriter, TProtobufWriterFieldDescription, TProtobufParserFieldDescription>;
    using TFieldPtr = std::unique_ptr<TField>;

    TProtobufTypeBuilder(const THashMap<TString, TEnumerationDescription>& enumerations);

    TFieldPtr CreateField(
        int structFieldIndex,
        const TProtobufColumnConfigPtr& columnConfig,
        std::optional<NTableClient::TComplexTypeFieldDescriptor> maybeDescriptor,
        bool allowOtherColumns = false);

private:
    const THashMap<TString, TEnumerationDescription>& Enumerations_;

private:
    // Traverse type config, matching it with type descriptor from schema.
    //
    // Matching of the type config and type descriptor is performed by the following rules:
    //  * Field of simple type matches simple type T "naturally"
    //  * Repeated field matches List<T> iff corresponding non-repeated field matches T and T is not Optional<...>
    //    (List<Optional<Any>> is allowed as an exception)
    //  * Non-repeated field matches Optional<T> iff it matches T and T is not Optional<...>
    //  * StructuredMessage field matches Struct<Name1: Type1, ..., NameN: TypeN> iff
    //      - the field has subfields whose names are in set {Name1, ..., NameN}
    //      - the subfield with name NameK matches TypeK
    //      - if |NonOptionalMissingFieldsAllowed()| is |false|,
    //        for each name NameK missing from subfields TypeK is Optional<...>
    TTypePtr FindOrCreateType(
        const TProtobufTypeConfigPtr& typeConfig,
        std::optional<NTableClient::TComplexTypeFieldDescriptor> maybeDescriptor,
        bool optional,
        bool repeated);

    void VisitStruct(
        const TTypePtr& type,
        const TProtobufTypeConfigPtr& typeConfig,
        NTableClient::TComplexTypeFieldDescriptor descriptor);

    void VisitDict(
        const TTypePtr& type,
        const TProtobufTypeConfigPtr& typeConfig,
        NTableClient::TComplexTypeFieldDescriptor descriptor);
};

////////////////////////////////////////////////////////////////////////////////

template <typename TType>
class TProtobufFormatDescriptionBase
    : public TRefCounted
{
protected:
    void DoInit(
        const TProtobufFormatConfigPtr& config,
        const std::vector<NTableClient::TTableSchemaPtr>& schemas);

private:
    THashMap<TString, TEnumerationDescription> EnumerationDescriptionMap_;

private:
    virtual void AddTable(NYT::TIntrusivePtr<TType> tableType) = 0;

    void InitFromFileDescriptorsLegacy(const TProtobufFormatConfigPtr& config);

    void InitFromFileDescriptors(
        const TProtobufFormatConfigPtr& config,
        const std::vector<NTableClient::TTableSchemaPtr>& schemas);

    void InitFromProtobufSchema(
        const TProtobufFormatConfigPtr& config,
        const std::vector<NTableClient::TTableSchemaPtr>& schemas);
};

////////////////////////////////////////////////////////////////////////////////

class TProtobufWriterFormatDescription
    : public TProtobufFormatDescriptionBase<TProtobufWriterType>
{
public:
    void Init(
        const TProtobufFormatConfigPtr& config,
        const std::vector<NTableClient::TTableSchemaPtr>& schemas);

    const TProtobufWriterFieldDescription* FindField(
        int tableIndex,
        int fieldIndex,
        const NTableClient::TNameTablePtr& nameTable) const;

    int GetTableCount() const;

    const TProtobufWriterFieldDescription* FindOtherColumnsField(int tableIndex) const;

private:
    virtual void AddTable(TProtobufWriterTypePtr tableType) override;

private:
    struct TTableDescription
    {
        TProtobufWriterTypePtr Type;
        THashMap<TString, const TProtobufWriterFieldDescription*> Columns;

        // Cached data.
        mutable std::vector<const TProtobufWriterFieldDescription*> FieldIndexToDescription;
        mutable const TProtobufWriterFieldDescription* OtherColumnsField = nullptr;
    };

private:
    const TTableDescription& GetTableDescription(int tableIndex) const;

private:
    std::vector<TTableDescription> Tables_;
};

DEFINE_REFCOUNTED_TYPE(TProtobufWriterFormatDescription)

////////////////////////////////////////////////////////////////////////////////

class TProtobufParserFormatDescription
    : public TProtobufFormatDescriptionBase<TProtobufParserType>
{
public:
    void Init(
        const TProtobufFormatConfigPtr& config,
        const std::vector<NTableClient::TTableSchemaPtr>& schemas);

    const TProtobufParserTypePtr& GetTableType() const;
    std::vector<ui16> CreateRootChildColumnIds(const NTableClient::TNameTablePtr& nameTable) const;

private:
    virtual void AddTable(TProtobufParserTypePtr tableType) override;

private:
    TProtobufParserTypePtr TableType_;
};

DEFINE_REFCOUNTED_TYPE(TProtobufParserFormatDescription)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
