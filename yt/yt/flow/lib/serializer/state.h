#pragma once

#include "public.h"

#include <yt/yt/flow/lib/delta_codecs/public.h>
#include <yt/yt/flow/lib/delta_codecs/state.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/client/table_client/unversioned_row.h>

namespace NYT::NFlow::NYsonSerializer {

////////////////////////////////////////////////////////////////////////////////

TString GetYsonStatePatchField(TStringBuf name);

////////////////////////////////////////////////////////////////////////////////

struct TStateSchema
    : public TRefCounted
{
    NTableClient::TTableSchemaPtr YsonSchema;
    NTableClient::TTableSchemaPtr TableSchema;
    std::vector<std::pair<i64, i64>> YsonToTableMapping;
    // table schema info
    THashSet<i64> CompressedColumns;
    THashSet<i64> CompressedPatchColumns;
    std::optional<i64> FormatColumn;

    std::function<NYTree::TYsonStructPtr()> MakeYsonStruct;
    std::function<bool(const NYTree::TYsonStructPtr&)> IsEmpty;
};

DEFINE_REFCOUNTED_TYPE(TStateSchema);

////////////////////////////////////////////////////////////////////////////////

struct TFormat
    : public virtual NYTree::TYsonStruct
{
    NCompression::ECodec Compression;
    NCompression::ECodec PatchCompression;
    NDeltaCodecs::ECodec Delta;

    REGISTER_YSON_STRUCT(TFormat);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFormat);

////////////////////////////////////////////////////////////////////////////////

namespace NPrivate {

TStateSchemaPtr BuildYsonStateSchema(std::function<NYTree::TYsonStructPtr()> ctor);

} // namespace NPrivate

////////////////////////////////////////////////////////////////////////////////

template <class T>
TStateSchemaPtr GetYsonStateSchema();

////////////////////////////////////////////////////////////////////////////////

struct TEraseMutation
{ };

struct TEmptyMutation
{ };

using TUpdateMutation = NTableClient::TUnversionedOwningRow;
using TStateMutation = std::variant<TEraseMutation, TUpdateMutation, TEmptyMutation>;

////////////////////////////////////////////////////////////////////////////////

class TState
    : public TRefCounted
{
public:
    TState(TStateSchemaPtr schema);

    void Init(const std::optional<NTableClient::TUnversionedRow>& tableRow = std::nullopt);

    i64 GetSize() const;

    TFormatPtr GetFormat() const;

    template <class TYsonStruct>
    TIntrusivePtr<TYsonStruct> GetValueAs()
    {
        auto result = DynamicPointerCast<TYsonStruct>(GetValue());
        YT_VERIFY(result);
        return result;
    }

    NYTree::TYsonStructPtr GetValue() const;
    void SetValue(NYTree::TYsonStructPtr value);

    void SetFormat(TFormatPtr format);
    void ForceRewrite();

    TStateMutation FlushMutation();
    std::optional<NTableClient::TUnversionedOwningRow> GetTableRow() const;
    NTableClient::TTableSchemaPtr GetTableSchema() const;
private:
    const TStateSchemaPtr Schema_;
    std::optional<std::vector<NTableClient::TUnversionedOwningValue>> TableRow_;
    TFormatPtr Format_;

    THashMap<i64, std::optional<NDeltaCodecs::TState>> UncompressedPackableTableColumns_;

    NTableClient::TUnversionedOwningRow YsonRow_;
    THashSet<int> ModifiedYsonRowColumns_;
    bool Rewrite_ = false;

    void ParseTableRow();
    bool IsEmptyYsonRow();

    static TSharedRef Compress(NCompression::ICodec* codec, const TSharedRef& value);
    static TSharedRef Decompress(NCompression::ICodec* codec, const TSharedRef& value);
};

DEFINE_REFCOUNTED_TYPE(TState);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NYsonSerializer

#define STATE_INL_H_
#include "state-inl.h"
#undef STATE_INL_H_
