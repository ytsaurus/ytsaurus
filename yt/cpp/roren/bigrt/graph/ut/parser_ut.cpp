#include <yt/cpp/roren/bigrt/graph/parser.h>
#include <yt/cpp/roren/interface/roren.h>
#include <yt/cpp/roren/interface/executor.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NRoren;
using namespace NRoren::NPrivate;

////////////////////////////////////////////////////////////////////////////////

// Dummy pipeline. Pipeline that is never meant to run graphs but only construct them.

class TDummyExecutor
    : public NRoren::IExecutor
{
public:
    void Run(const TPipeline&) override
    {
        Y_FAIL("Test executor is not meant to execute pipelines");
    }
};

NRoren::TPipeline MakeDummyPipeline()
{
    return NRoren::NPrivate::MakePipeline(MakeIntrusive<TDummyExecutor>());
}

////////////////////////////////////////////////////////////////////////////////

struct TMyState
{
    inline void Save(IOutputStream*) const
    { }

    inline void Load(IInputStream*)
    { }
};

////////////////////////////////////////////////////////////////////////////////

TEST(GraphParser, ParseResharderSimpleMap) {
    auto pipeline = MakeDummyPipeline();
    auto input = pipeline | DummyRead<TString>();

    auto kv = input | ParDo([] (const TString& input) {
        return input + "foo";
    });

    kv | DummyWrite<TString>();
}
