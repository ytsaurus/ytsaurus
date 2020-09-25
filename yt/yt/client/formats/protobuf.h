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

struct TProtobufFieldDescriptionBase
{
    TString Name;
    EProtobufType Type = EProtobufType::Int64;
    ui64 WireTag = 0;
    size_t TagSize = 0;

    const TEnumerationDescription* EnumerationDescription = nullptr;

    // Index of field inside struct (for fields corresponding to struct fields in schema).
    int StructFieldIndex = 0;

    // Number of fields in struct in schema (only for |Type == StructuredMessage|).
    int StructFieldCount = 0;

    // Is field repeated?
    bool Repeated = false;

    // Is a repeated field packed (i.e. it is encoded as `<tag> <length> <value1> ... <valueK>`)?
    bool Packed = false;

    // Is the corresponding type in schema optional?
    bool Optional = true;

    // Extracts field number from |WireTag|.
    ui32 GetFieldNumber() const;
};

class TProtobufWriterFieldDescription
    : public TProtobufFieldDescriptionBase
{
public:
    TProtobufWriterFieldDescription* AddChild(int fieldIndex);
    const TProtobufWriterFieldDescription* FindAlternative(int alternativeIndex) const;

public:
    std::vector<std::unique_ptr<TProtobufWriterFieldDescription>> Children;

private:
    std::vector<int> AlternativeToChildIndex_;
    static constexpr int InvalidChildIndex = -1;
};

class TProtobufParserFieldDescription
    : public TProtobufFieldDescriptionBase
{
public:
    TProtobufParserFieldDescription* AddChild(int fieldNumber);
    void IgnoreChild(int fieldNumber);

    // Returns std::nullopt iff the field is ignored (missing from table schema).
    // Throws an exception iff the field number is unknown.
    std::optional<int> FieldNumberToChildIndex(int fieldNumber) const;

    // Return concise human-readable description of the path to the field.
    TString GetDebugString() const;

    bool IsOneofAlternative() const
    {
        return AlternativeIndex.has_value();
    }

private:
    void SetChildIndex(int fieldNumber, int childIndex);

public:
    std::vector<std::unique_ptr<TProtobufParserFieldDescription>> Children;
    std::optional<int> AlternativeIndex;

    // For non-oneof members Parent points to the description
    // containing this as a child.
    // For oneof members Parent points to corresponding oneof description.
    TProtobufParserFieldDescription* Parent = nullptr;

private:
    static constexpr int InvalidChildIndex = -1;
    static constexpr int IgnoredChildIndex = -2;
    static constexpr int MaxFieldNumberVectorSize = 256;

    std::vector<int> FieldNumberToChildIndexVector_;
    THashMap<int, int> FieldNumberToChildIndexMap_;
};

////////////////////////////////////////////////////////////////////////////////

class TProtobufFormatDescriptionBase
    : public TRefCounted
{
public:
    void Init(
        const TProtobufFormatConfigPtr& config,
        const std::vector<NTableClient::TTableSchemaPtr>& schemas);

protected:
    virtual bool NonOptionalMissingFieldsAllowed() const = 0;

    virtual void AddTable() = 0;

    virtual void IgnoreField(
        int tableIndex,
        TProtobufFieldDescriptionBase* parent,
        const TProtobufColumnConfigPtr& columnConfig) = 0;

    virtual TProtobufFieldDescriptionBase* AddField(
        int tableIndex,
        TProtobufFieldDescriptionBase* parent,
        int fieldIndex,
        const TProtobufColumnConfigPtr& columnConfig) = 0;

    // Returns |field| for convenience.
    TProtobufFieldDescriptionBase* InitField(
        TProtobufFieldDescriptionBase* field,
        int structFieldIndex,
        const TProtobufColumnConfigPtr& columnConfig);

protected:
    THashMap<TString, TEnumerationDescription> EnumerationDescriptionMap_;

private:
    void InitFromFileDescriptors(const TProtobufFormatConfigPtr& config);
    void InitFromProtobufSchema(
        const TProtobufFormatConfigPtr& config,
        const std::vector<NTableClient::TTableSchemaPtr>& schemas);

    // Traverse column config, matching it with column schema and
    // calling |AddField| and |AddOneofField| for corresponding subfields.
    //
    // Matching of the config and type is performed by the following rules:
    //  * Field of simple type matches simple type T "naturally"
    //  * Repeated field matches List<T> iff corresponding non-repeated field matches T and T is not Optional<...>
    //    (List<Optional<Any>> is allowed as an exception)
    //  * Non-repeated field matches Optional<T> iff it matches T and T is not Optional<...>
    //  * StructuredMessage field matches Struct<Name1: Type1, ..., NameN: TypeN> iff
    //      - the field has subfields whose names are in set {Name1, ..., NameN}
    //      - the subfield with name NameK matches TypeK
    //      - if |NonOptionalMissingFieldsAllowed()| is |false|,
    //        for each name NameK missing from subfields TypeK is Optional<...>
    void Traverse(
        int tableIndex,
        TProtobufFieldDescriptionBase* field,
        const TProtobufColumnConfigPtr& columnConfig,
        NTableClient::TComplexTypeFieldDescriptor descriptor);

    void TraverseStruct(
        int tableIndex,
        TProtobufFieldDescriptionBase* parent,
        const TProtobufColumnConfigPtr& columnConfig,
        NTableClient::TComplexTypeFieldDescriptor descriptor);

    void TraverseDict(
        int tableIndex,
        TProtobufFieldDescriptionBase* field,
        const TProtobufColumnConfigPtr& columnConfig,
        NTableClient::TComplexTypeFieldDescriptor descriptor);
};

////////////////////////////////////////////////////////////////////////////////

class TProtobufWriterFormatDescription
    : public TProtobufFormatDescriptionBase
{
public:
    const TProtobufWriterFieldDescription* FindField(
        int tableIndex,
        int fieldIndex,
        const NTableClient::TNameTablePtr& nameTable) const;

    int GetTableCount() const;

    const TProtobufWriterFieldDescription* FindOtherColumnsField(int tableIndex) const;

private:
    virtual bool NonOptionalMissingFieldsAllowed() const override;

    virtual void AddTable() override;

    virtual void IgnoreField(
        int tableIndex,
        TProtobufFieldDescriptionBase* parent,
        const TProtobufColumnConfigPtr& columnConfig) override;

    virtual TProtobufFieldDescriptionBase* AddField(
        int tableIndex,
        TProtobufFieldDescriptionBase* parent,
        int fieldIndex,
        const TProtobufColumnConfigPtr& columnConfig) override;

private:
    struct TTableDescription
    {
        THashMap<TString, TProtobufWriterFieldDescription> Columns;

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
    : public TProtobufFormatDescriptionBase
{
public:
    TProtobufParserFormatDescription();
    const TProtobufParserFieldDescription& GetRootDescription() const;
    std::vector<ui16> CreateRootChildColumnIds(const NTableClient::TNameTablePtr& nameTable) const;

private:
    virtual bool NonOptionalMissingFieldsAllowed() const override;

    virtual void AddTable() override;

    virtual void IgnoreField(
        int tableIndex,
        TProtobufFieldDescriptionBase* parent,
        const TProtobufColumnConfigPtr& columnConfig) override;

    virtual TProtobufFieldDescriptionBase* AddField(
        int tableIndex,
        TProtobufFieldDescriptionBase* parent,
        int fieldIndex,
        const TProtobufColumnConfigPtr& columnConfig) override;

private:
    TProtobufParserFieldDescription* ResolveField(TProtobufFieldDescriptionBase* parent);

private:
    TProtobufParserFieldDescription RootDescription_;

    // Storage of oneof descriptions.
    std::vector<std::unique_ptr<TProtobufParserFieldDescription>> OneofDescriptions_;
};

DEFINE_REFCOUNTED_TYPE(TProtobufParserFormatDescription)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
