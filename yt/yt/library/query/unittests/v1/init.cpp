#include <yt/yt/library/query/unittests/evaluate/ql_helpers.h>

#include <library/cpp/testing/hook/hook.h>

Y_TEST_HOOK_BEFORE_INIT(SET_DEFAULT_QL_EXPRESSION_BUILDER)
{
    NYT::NQueryClient::DefaultExpressionBuilderVersion = 1;
}

