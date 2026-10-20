#include "state.h"

#include <yt/yt/flow/library/cpp/serializer/state.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/ytree/attributes.h>
#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow::NTables {

////////////////////////////////////////////////////////////////////////////////

void TInternalState::Register(TRegistrar registrar)
{
    registrar.Parameter("state", &TThis::State)
        .AddOption(NYsonSerializer::EYsonStateValueType::Simple)
        .Default();
    registrar.Parameter("compressed", &TThis::Compressed)
        .AddOption(NYsonSerializer::EYsonStateValueType::Packable)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

namespace {

bool IsNullColumn(const NYTree::INodePtr& value, TStringBuf name)
{
    if (!value) {
        return true;
    }
    if (value->GetType() != NYTree::ENodeType::Entity) {
        return false;
    }
    if (value->Attributes().Get<NTableClient::EValueType>("type", NTableClient::EValueType::Null) != NTableClient::EValueType::Null) {
        THROW_ERROR_EXCEPTION("Unexpected entity type in column %Qv", name);
    }
    return true;
}

TSharedRef GetPackedColumn(const NYTree::IMapNodePtr& row, TStringBuf name)
{
    auto value = row->FindChild(name);
    if (IsNullColumn(value, name)) {
        return TSharedRef::MakeEmpty();
    }
    if (value->GetType() != NYTree::ENodeType::String) {
        THROW_ERROR_EXCEPTION("Column %Qv must be a string or null", name);
    }
    return TSharedRef(
        TRef::FromString(value->AsString()->GetValue()),
        MakeSharedRangeHolder(value));
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

NYTree::INodePtr UnpackInternalState(const NYTree::IMapNodePtr& row)
{
    auto value = row->FindChild("state");
    if (!IsNullColumn(value, "state")) {
        return value;
    }

    auto formatNode = row->FindChild("format");
    auto format = IsNullColumn(formatNode, "format")
        ? nullptr
        : NYTree::ConvertTo<NYsonSerializer::TFormatPtr>(formatNode);
    auto unpacked = NYsonSerializer::UnpackState(
        GetPackedColumn(row, "compressed"),
        GetPackedColumn(row, "compressed_patch"),
        format);
    return NYTree::ConvertToNode(unpacked.Empty()
            ? NYson::TYsonStringBuf("#")
            : NYson::TYsonStringBuf(unpacked.ToStringBuf()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTables
