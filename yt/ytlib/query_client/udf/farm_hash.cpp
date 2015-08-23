#include <yt/ytlib/table_client/unversioned_value.h>
#include <yt_udf_cpp.h>

extern "C" void farm_hash(
    TExecutionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* args,
    int args_len)
{
    auto argsValue = (NYT::NTableClient::TUnversionedValue*)args;
    result->Data.Uint64 = NYT::NTableClient::GetFarmFingerprint(argsValue, argsValue + args_len);
    result->Type = Uint64;
}
