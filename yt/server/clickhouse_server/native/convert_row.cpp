#include "convert_row.h"

#include <yt/client/table_client/unversioned_row.h>
#include <yt/client/table_client/unversioned_value.h>

namespace NYT {
namespace NClickHouseServer {
namespace NNative {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

void ConvertValue(TValue& dst, const TUnversionedValue& src)
{
    switch (src.Type) {
        case NTableClient::EValueType::Null:
            dst.SetNull();
            return;

        case NTableClient::EValueType::Int64:
            dst.SetInt(src.Data.Int64);
            return;

        case NTableClient::EValueType::Uint64:
            dst.SetUInt(src.Data.Uint64);
            return;

        case NTableClient::EValueType::Double:
            dst.SetFloat(src.Data.Double);
            return;

        case NTableClient::EValueType::Boolean:
            dst.SetBoolean(src.Data.Boolean);
            return;

        case NTableClient::EValueType::String:
            dst.SetString(src.Data.String, src.Length);
            return;

        case NTableClient::EValueType::Any:
        case NTableClient::EValueType::Min:
        case NTableClient::EValueType::Max:
        case NTableClient::EValueType::TheBottom:
            break;
    }

    THROW_ERROR_EXCEPTION("Invalid column type")
        << TErrorAttribute("type", static_cast<int>(src.Type));
}

TRow ConvertRow(const TUnversionedRow& src)
{
    TRow dst;
    dst.resize(src.GetCount());

    for (size_t i = 0; i < src.GetCount(); ++i) {
        ConvertValue(dst[i], src[i]);
    }

    return dst;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNative
} // namespace NClickHouseServer
} // namespace NYT
