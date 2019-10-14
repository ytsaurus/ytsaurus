#include "check_yson_token.h"

#include <yt/client/table_client/logical_type.h>

namespace NYT::NComplexTypes {

////////////////////////////////////////////////////////////////////////////////

void ThrowUnexpectedYsonTokenException(
    const NTableClient::TComplexTypeFieldDescriptor& descriptor,
    NYson::EYsonItemType actual,
    const std::vector<NYson::EYsonItemType>& expected)
{
    YT_VERIFY(expected.size() > 0);
    TString expectedString;
    if (expected.size() > 1) {
        TStringStream out;
        out << "one of the tokens {";
        for (const auto& token : expected) {
            out << Format("%Qlv, ", token);
        }
        out << "}";
        expectedString = out.Str();
    } else {
        expectedString = Format("%Qlv", expected[0]);
    }

    THROW_ERROR_EXCEPTION("Cannot parse %Qv; expected: %v; actual: %Qlv",
        descriptor.GetDescription(),
        expectedString,
        actual);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NComplexTypes
