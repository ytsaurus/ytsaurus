#pragma once

#include <contrib/ydb/library/yql/minikql/mkql_function_metadata.h>

namespace NKikimr {
namespace NMiniKQL {

IBuiltinFunctionRegistry::TPtr CreateBuiltinRegistry();

} // namspace NMiniKQL
} // namspace NKikimr
