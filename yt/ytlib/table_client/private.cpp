#include "stdafx.h"
#include "private.h"

namespace NYT {
namespace NTableClient {

using namespace NVersionedTableClient;

////////////////////////////////////////////////////////////////////////////////

NLog::TLogger TableReaderLogger("TableReader");
NLog::TLogger TableWriterLogger("TableWriter");

const int FormatVersion = 1;

const size_t MaxKeySize = 4 * 1024;

////////////////////////////////////////////////////////////////////////////////

TUnversionedValue MakeKeyPart(const TStringBuf& yson, NYson::TStatelessLexer& lexer)
{
    NYson::TToken token;
    lexer.GetToken(yson, &token);
    YCHECK(!token.IsEmpty());

    switch (token.GetType()) {
        case NYson::ETokenType::Integer:
            return MakeUnversionedIntegerValue(token.GetIntegerValue());

        case NYson::ETokenType::Double:
            return MakeUnversionedDoubleValue(token.GetDoubleValue());

        case NYson::ETokenType::String:
            return MakeUnversionedStringValue(token.GetStringValue());

        default:
            return MakeUnversionedAnyValue(yson);
    }
}

////////////////////////////////////////////////////////////////////////////////

namespace {

int CompareKeyParts(const NChunkClient::NProto::TKeyPart& lhs, const NChunkClient::NProto::TKeyPart& rhs)
{
    if (lhs.type() != rhs.type()) {
        return lhs.type() - rhs.type();
    }

    if (lhs.has_double_value()) {
        if (lhs.double_value() > rhs.double_value())
            return 1;
        if (lhs.double_value() < rhs.double_value())
            return -1;
        return 0;
    }

    if (lhs.has_int_value()) {

        if (lhs.int_value() > rhs.int_value())
            return 1;
        if (lhs.int_value() < rhs.int_value())
            return -1;
        return 0;
    }

    if (lhs.has_str_value()) {
        return lhs.str_value().compare(rhs.str_value());
    }

    return 0;
}

} // namespace

NChunkClient::NProto::TKey GetKeySuccessor(const NChunkClient::NProto::TKey& key)
{
    NChunkClient::NProto::TKey result;
    result.CopyFrom(key);
    auto* sentinelPart = result.add_parts();
    sentinelPart->set_type(EKeyPartType::MinSentinel);
    return result;
}

int CompareKeys(
    const NChunkClient::NProto::TKey& lhs, 
    const NChunkClient::NProto::TKey& rhs, 
    int prefixLength)
{
    int lhsSize = std::min(lhs.parts_size(), prefixLength);
    int rhsSize = std::min(rhs.parts_size(), prefixLength);
    int minSize = std::min(lhsSize, rhsSize);
    for (int index = 0; index < minSize; ++index) {
        int result = CompareKeyParts(lhs.parts(index), rhs.parts(index));
        if (result != 0) {
            return result;
        }
    }
    return lhsSize - rhsSize;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

