#include "identifier.h"

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/unversioned_value.h>

#include <yt/yt/core/yson/pull_parser_deserialize.h>

#include <yt/yt/core/ytree/serialize.h>

#include <library/cpp/yt/string/format.h>
#include <library/cpp/yt/yson/consumer.h>

#include <util/ysaveload.h>

#include <cstring>
#include <new>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

static size_t BufferSize(size_t strLen) noexcept
{
    return (strLen <= NDetail::ShortLengthMax ? NDetail::ShortHeaderSize : NDetail::LongHeaderSize) + strLen;
}

void TIdentifierStringData::operator delete(void* ptr) noexcept
{
    ::operator delete(ptr);
}

::TIntrusivePtr<TIdentifierStringData> TIdentifierStringData::Make(std::string_view str)
{
    const size_t strLen = str.size();
    const size_t bufSize = BufferSize(strLen);
    const size_t allocSize = sizeof(TIdentifierStringData) + bufSize;

    void* raw = ::operator new(allocSize);
    auto* obj = ::new (raw) TIdentifierStringData();
    char* buf = obj->ExtraPtr();

    if (strLen <= NDetail::ShortLengthMax) {
        NDetail::TShortSize shortLen = static_cast<NDetail::TShortSize>(strLen);
        std::memcpy(buf, &shortLen, sizeof(shortLen));
        std::memcpy(buf + NDetail::ShortHeaderSize, str.data(), strLen);
    } else {
        NDetail::TShortSize marker = 0;
        NDetail::TLongSize longLen = static_cast<NDetail::TLongSize>(strLen);
        std::memcpy(buf, &marker, sizeof(marker));
        std::memcpy(buf + NDetail::ShortHeaderSize, &longLen, sizeof(longLen));
        std::memcpy(buf + NDetail::LongHeaderSize, str.data(), strLen);
    }

    return ::TIntrusivePtr<TIdentifierStringData>(obj);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

////////////////////////////////////////////////////////////////////////////////

namespace NYT::NFlow::NDetail {

////////////////////////////////////////////////////////////////////////////////

void IdentifierSave(IOutputStream* out, std::string_view data)
{
    ::Save(out, TString(data));
}

void IdentifierLoad(IInputStream* in, std::string& data)
{
    ::Load(in, data);
}

void IdentifierSerialize(std::string_view data, NYson::IYsonConsumer* consumer)
{
    consumer->OnStringScalar(data);
}

void IdentifierDeserializeFromNode(std::string& data, const NYTree::INodePtr& node)
{
    Deserialize(data, node);
}

void IdentifierDeserializeFromCursor(std::string& data, NYson::TYsonPullParserCursor* cursor)
{
    Deserialize(data, cursor);
}

void IdentifierFormat(TStringBuilderBase* builder, std::string_view data, TStringBuf spec)
{
    FormatValue(builder, data, spec);
}

void IdentifierToUnversionedValue(
    NTableClient::TUnversionedValue* unversionedValue,
    std::string_view data,
    const NTableClient::TRowBufferPtr& rowBuffer,
    int id,
    NTableClient::EValueFlags flags)
{
    NTableClient::ToUnversionedValue(unversionedValue, data, rowBuffer, id, flags);
}

void IdentifierFromUnversionedValue(std::string& data, const NTableClient::TUnversionedValue& unversionedValue)
{
    NTableClient::FromUnversionedValue(&data, unversionedValue);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDetail
