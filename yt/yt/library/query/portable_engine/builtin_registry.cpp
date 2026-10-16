#include "builtin_registry.h"

#include "registry.h"

#include <yt/yt/library/query/base/query_common.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/actions/bind.h>

#include <library/cpp/yt/memory/leaky_singleton.h>

namespace NYT::NQueryClient::NPortable {
namespace {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

void EvaluateFarmHash(
    TValue* result,
    TRange<TValue> arguments,
    const TRowBufferPtr& /*rowBuffer*/)
{
    *result = MakeUnversionedUint64Value(GetFarmFingerprint(arguments));
}

void EvaluateIntegerModulo(
    TValue* result,
    TRange<TValue> arguments,
    const TRowBufferPtr& /*rowBuffer*/)
{
    *result = EvaluateModulo(arguments[0], arguments[1]);
}

void ConvertInt64ToUint64(
    TValue* result,
    TRange<TValue> arguments,
    const TRowBufferPtr& /*rowBuffer*/)
{
    *result = CastValueWithCheck(arguments[0], EValueType::Uint64);
}

TExpressionRegistry CreateBuiltinExpressionRegistry()
{
    TExpressionRegistryBuilder builder;
    builder.RegisterVariadicFunction(
        "farm_hash",
        {
            .AllowedArgumentTypes = TTypeSet{
                EValueType::Int64,
                EValueType::Uint64,
                EValueType::Boolean,
                EValueType::String,
            },
            .ResultType = EValueType::Uint64,
            .Implementation = {
                .NullPolicy = EOperationNullPolicy::PassToCallback,
                .Callback = BIND_NO_PROPAGATE(&EvaluateFarmHash),
            },
        });

    for (auto type : {EValueType::Int64, EValueType::Uint64}) {
        builder.RegisterBinary(
            EBinaryOp::Modulo,
            {
                .ArgumentTypes = {type, type},
                .ResultType = type,
                .Implementation = {
                    .NullPolicy = EOperationNullPolicy::Propagate,
                    .Callback = BIND_NO_PROPAGATE(&EvaluateIntegerModulo),
                },
            });
    }

    builder.RegisterFunction(
        "uint64",
        {
            .ArgumentTypes = {EValueType::Int64},
            .ResultType = EValueType::Uint64,
            .Implementation = {
                .NullPolicy = EOperationNullPolicy::Propagate,
                .Callback = BIND_NO_PROPAGATE(&ConvertInt64ToUint64),
            },
        });

    return builder.Build();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

const TExpressionRegistry& GetBuiltinExpressionRegistry()
{
    struct TStorage
    {
        const TExpressionRegistry Registry = CreateBuiltinExpressionRegistry();
    };

    return LeakySingleton<TStorage>()->Registry;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient::NPortable
