#include <yt/cpp/roren/interface/easy_init_par_do/easy_init_par_do.h>
#include <yt/cpp/roren/interface/roren.h>
#include <yt/cpp/roren/local/local.h>
#include <yt/cpp/roren/local/vector_io.h>
#include <yt/cpp/roren/transforms/sum.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <limits>

using namespace NRoren;

DEFINE_EASY_INIT_PAR_DO(TNoDefaultConstructorParDo, int64_t, int64_t) : public IDoFn<int32_t, int64_t>
{
public:
    TNoDefaultConstructorParDo(int64_t value1, int64_t value2)
        : Value1_(value1)
        , Value2_(value2)
    { }

    void Start(TOutput<int64_t>& output) override
    {
        output.Add(Value1_);
    }

    void Do(const int& input, TOutput<int64_t>& output) override
    {
        output.Add(input * Value1_ + Value2_);
    }

    void Finish(TOutput<int64_t>& output) override
    {
        output.Add(Value2_);
    }

private:
    int64_t Value1_;
    int64_t Value2_;
};

TEST(EasyInitParDo, EasyInitParDo)
{
    TNoDefaultConstructorParDo pardo;

    TStringStream ss;
    ::SaveMany(&ss, 1LL * INT_MAX, 42LL);

    pardo.Load(&ss);

    auto test = [&](int64_t value1, int64_t value2){
        const std::vector<int32_t> input = {
            2, 3, 4
        };
        std::vector<int64_t> output;
        std::vector<int64_t> expected {
            value1, value1 * 2 + value2, value1 * 3 + value2, value1 * 4 + value2, value2
        };
        TPipeline pipeline = MakeLocalPipeline();
        pipeline
            | VectorRead(input)
            | MakeParDo<TNoDefaultConstructorParDo>(pardo)
            | VectorWrite(&output);
        pipeline.Run();

        ASSERT_EQ(output, expected);
    };
    test(INT_MAX, 42);

    pardo.Save(&ss);
    pardo.Load(&ss);
    test(INT_MAX, 42);

    pardo = TNoDefaultConstructorParDo(INT_MIN, -42);
    test(INT_MIN, -42);
}

DEFINE_EASY_INIT_PAR_DO(TExecutionContextPushParDo, std::string) : public IDoFn<std::string, std::string>
{
public:
    TExecutionContextPushParDo(std::string value)
        : Value_(std::move(value))
    { }

    void Do(const std::string& input, TOutput<std::string>& output) override
    {
        output.Add(input + GetExecutionContext()->GetExecutorName() + Value_);
    }

private:
    std::string Value_;
};

TEST(EasyInitParDo, ExecutionContextForwarding)
{
    TExecutionContextPushParDo pardo("__");
    const std::vector<std::string> input = {
        "a", "b", "c"
    };
    std::vector<std::string> output;
    std::vector<std::string> expected {
        "alocal__", "blocal__", "clocal__"
    };
    TPipeline pipeline = MakeLocalPipeline();
    pipeline
        | VectorRead(input)
        | MakeParDo<TExecutionContextPushParDo>(pardo)
        | VectorWrite(&output);
    pipeline.Run();

    ASSERT_EQ(output, expected);
}
