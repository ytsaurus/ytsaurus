#include <yt/cpp/roren/interface/roren.h>
#include <yt/cpp/roren/local/local.h>
#include <yt/cpp/roren/local/vector_io.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYT;
using namespace NRoren;

struct X {
    void Load(void*) {}
    void Save(void*) const {}
};

class TStatefulDo
    : public IStatefulDoFn<TKV<TString, X>, TString, int>
{
public:
    void Do(const TInputRow& input, TOutput<TOutputRow>& output, TState& state)
    {
        ++state;
        output.Add(input.Key());
    }
};

void ConstructPipeline(NRoren::TPipeline& pipeline, const std::vector<TString>& input, std::vector<TString>& output, THashMap<TString, int>& stateStorage)
{
    using namespace NRoren;
    pipeline
        | VectorRead(input)
        | ParDo([](const TString& input)
            {
                return TKV<TString, X>(input, {});
            }
        )
        | MakeStatefulParDo<TStatefulDo>(NRoren::MakeLocalPState(stateStorage))
        | VectorWrite(&output);
}

Y_UNIT_TEST_SUITE(RorenLocal) {
    Y_UNIT_TEST(StatefulPardo)
    {
    const std::vector<TString> input = {"d", "a" , "b", "c", "b", "d", "c", "d", "c", "d"};

    std::vector<TString> output;
    THashMap<TString, int> stateStorage;

    auto pipeline = MakeLocalPipeline();
    ConstructPipeline(pipeline, input, output, stateStorage);
    pipeline.Run();

    THashMap<TString, int> expectedState;
    expectedState["a"] = 1;
    expectedState["b"] = 2;
    expectedState["c"] = 3;
    expectedState["d"] = 4;
    UNIT_ASSERT_VALUES_EQUAL(input, output);
    UNIT_ASSERT_VALUES_EQUAL(stateStorage, expectedState);
    }
}
