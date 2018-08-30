#include "convert_row.h"

#include <yt/client/table_client/unversioned_row.h>
#include <yt/client/table_client/unversioned_value.h>

namespace NYT {
namespace NClickHouse {

using namespace NYT::NTableClient;

////////////////////////////////////////////////////////////////////////////////

void ConvertValue(NInterop::TValue& dst, const TUnversionedValue& src)
{
    switch (src.Type) {
        case EValueType::Null:
            dst.SetNull();
            return;

        case EValueType::Int64:
            dst.SetInt(src.Data.Int64);
            return;

        case EValueType::Uint64:
            dst.SetUInt(src.Data.Uint64);
            return;

        case EValueType::Double:
            dst.SetFloat(src.Data.Double);
            return;

        case EValueType::Boolean:
            dst.SetBoolean(src.Data.Boolean);
            return;

        case EValueType::String:
            dst.SetString(src.Data.String, src.Length);
            return;

        case EValueType::Any:
        case EValueType::Min:
        case EValueType::Max:
        case EValueType::TheBottom:
            break;
    }

    THROW_ERROR_EXCEPTION("Invalid column type")
        << TErrorAttribute("type", static_cast<int>(src.Type));
}

NInterop::TRow ConvertRow(const TUnversionedRow& src)
{
    NInterop::TRow dst;
    dst.resize(src.GetCount());

    for (size_t i = 0; i < src.GetCount(); ++i) {
        ConvertValue(dst[i], src[i]);
    }

    return dst;
}

}   // namespace NClickHouse
}   // namespace NYT
