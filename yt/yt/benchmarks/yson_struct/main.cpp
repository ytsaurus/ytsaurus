#include <yt/yt/core/ytree/yson_struct.h>

#include <benchmark/benchmark.h>
#include <library/cpp/testing/gbenchmark/benchmark.h>

namespace NYT::NYTree {
namespace {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TBinaryTree);

class TBinaryTree
    : public virtual TYsonStruct
{
public:
    i64 Value;
    TBinaryTreePtr Left;
    TBinaryTreePtr Right;

    REGISTER_YSON_STRUCT(TBinaryTree);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("value", &TBinaryTree::Value)
            .Default(0);
        registrar.Parameter("left", &TBinaryTree::Left)
            .Default();
        registrar.Parameter("right", &TBinaryTree::Right)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TBinaryTree);

////////////////////////////////////////////////////////////////////////////////

void BM_Parse(benchmark::State& state)
{
    auto ysonData = NYson::TYsonString(TStringBuf(R""""({
        "value" = 10;
        "left" = {
            "value" = 5;
            "left" = {
                "value" = 3;
                "left" = {};
                "right" = {};
            };
            "right" = {
                "value" = 6;
                "left" = {};
                "right" = {
                    "value" = 9;
                    "left" = {
                        "value" = 8;
                        "left" = {
                            "value" = 7;
                            "left" = {};
                            "right" = {};
                        };
                        "right" = {};
                    };
                    "right" = {};
                };
            };
        };
        "right" = {
            "value" = 15;
            "left" = {};
            "right" = {};
        };
    })""""));
    while (state.KeepRunning()) {
        ConvertTo<TBinaryTreePtr>(ysonData);
    }
}

////////////////////////////////////////////////////////////////////////////////

BENCHMARK(BM_Parse);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYTree

BENCHMARK_MAIN();
