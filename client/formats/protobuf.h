#pragma once

#include "config.h"

#include <contrib/libs/protobuf/descriptor.h>
#include <contrib/libs/protobuf/google/protobuf/descriptor.pb.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

class TEnumerationDescription
{
public:
    TEnumerationDescription(const TString& name);

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

struct TProtobufFieldDescription
{
    TString Name;
    EProtobufType Type;
    ui64 WireTag;
    size_t TagSize;
    const TEnumerationDescription* EnumerationDescription;

    ui32 GetFieldNumber() const;
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

    void Init(const TProtobufFormatConfigPtr& config);
    const TProtobufTableDescription& GetTableDescription(ui32 tableIndex) const;
    size_t GetTableCount() const;

private:
    void InitFromFileDescriptors(const TProtobufFormatConfigPtr& config);
    void InitFromProtobufSchema(const TProtobufFormatConfigPtr& config);

private:
    std::vector<TProtobufTableDescription> Tables_;
    THashMap<TString, TEnumerationDescription> EnumerationDescriptionMap_;
};

DEFINE_REFCOUNTED_TYPE(TProtobufFormatDescription)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
