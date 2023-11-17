#pragma once
#include <contrib/ydb/library/yql/public/issue/yql_issue.h>
#include <contrib/ydb/library/yql/minikql/mkql_node.h>

namespace NKikimr {
namespace NMiniKQL {

NYql::TPosition ExtractPosition(TCallable& callable);

}
}
