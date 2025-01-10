#pragma once

#include <yql/essentials/public/result_format/yql_result_format_type.h>

#include <yt/yt/client/table_client/logical_type.h>

#include <stack>

namespace NYT::NYqlAgent {

////////////////////////////////////////////////////////////////////////////////

class TTypeBuilder
    : public NYql::NResult::ITypeVisitor
{
public:
    TTypeBuilder();

    NTableClient::TLogicalTypePtr PullResult();

private:
    void OnVoid() final;
    void OnNull() final;
    void OnEmptyList() final;
    void OnEmptyDict() final;
    void OnBool() final;
    void OnInt8() final;
    void OnUint8() final;
    void OnInt16() final;
    void OnUint16() final;
    void OnInt32() final;
    void OnUint32() final;
    void OnInt64() final;
    void OnUint64() final;
    void OnFloat() final;
    void OnDouble() final;
    void OnString() final;
    void OnUtf8() final;
    void OnYson() final;
    void OnJson() final;
    void OnJsonDocument() final;
    void OnUuid() final;
    void OnDyNumber() final;
    void OnDate() final;
    void OnDatetime() final;
    void OnTimestamp() final;
    void OnTzDate() final;
    void OnTzDatetime() final;
    void OnTzTimestamp() final;
    void OnInterval() final;
    void OnDate32() final;
    void OnDatetime64() final;
    void OnTimestamp64() final;
    void OnTzDate32() final;
    void OnTzDatetime64() final;
    void OnTzTimestamp64() final;
    void OnInterval64() final;
    void OnDecimal(ui32 precision, ui32 scale) final;
    void OnBeginOptional() final;
    void OnEndOptional() final;
    void OnBeginList() final;
    void OnEndList() final;
    void OnBeginTuple() final;
    void OnTupleItem() final;
    void OnEndTuple() final;
    void OnBeginStruct() final;
    void OnStructItem(TStringBuf member) final;
    void OnEndStruct() final;
    void OnBeginDict() final;
    void OnDictKey() final;
    void OnDictPayload() final;
    void OnEndDict() final;
    void OnBeginVariant() final;
    void OnEndVariant() final;
    void OnBeginTagged(TStringBuf tag) final;
    void OnEndTagged() final;
    void OnPg(TStringBuf name, TStringBuf category) final;

    void Push(NTableClient::TLogicalTypePtr type);

    template<class T = NTableClient::TLogicalTypePtr>
    T Pop();

    using TItemType = NTableClient::TLogicalTypePtr;
    using TElements = std::vector<TItemType>;
    using TMembers = std::vector<NTableClient::TStructField>;
    using TTagAndType = std::pair<TString, TItemType>;

    struct TKeyAndPayload {
        TItemType Key, Payload;
        std::optional<bool> Switch;

        void Set(TItemType type) {
            (*Switch ? Key : Payload) = std::move(type);
        }
    };

    using TItem = std::variant<TItemType, TElements, TMembers, TKeyAndPayload, TTagAndType>;

    std::stack<TItem> ItemsStack;
    std::stack<TString> MemberNames;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlAgent
