#include <contrib/libs/farmhash/farmhash.cc>
#include <yt_udf_cpp.h>

extern "C" uint64_t FarmHash(
    const TUnversionedValue* begin,
    const TUnversionedValue* end);

extern "C" void farm_hash(
    TExecutionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* args,
    int args_len)
{
    uint64_t hash = 0xdeadc0de;
    for (int i = 0; i < args_len; i++) {
        auto argument = args[i];
        uint64_t argHash;
        switch (argument.Type) {
            case EValueType::String:
                argHash = NFarmHashPrivate::Fingerprint64(
                    argument.Data.String,
                    argument.Length);
                break;

            case EValueType::Int64:
            case EValueType::Uint64:
            case EValueType::Double:
                // These types are aliased.
                argHash = NFarmHashPrivate::Fingerprint(argument.Data.Int64);
                break;

            case EValueType::Boolean:
                argHash = NFarmHashPrivate::Fingerprint(argument.Data.Boolean);
                break;

            case EValueType::Null:
                argHash = NFarmHashPrivate::Fingerprint(0);
                break;
        }
        hash = NFarmHashPrivate::Fingerprint(NFarmHashPrivate::Uint128(hash, argHash));
    }

    result->Data.Uint64 = hash ^ args_len;
    result->Type = Uint64;
}
