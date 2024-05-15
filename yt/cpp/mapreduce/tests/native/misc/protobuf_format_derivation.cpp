#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/tests/native/proto_lib/row.pb.h>

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/errors.h>
#include <yt/cpp/mapreduce/interface/config.h>

#include <library/cpp/testing/gtest/gtest.h>

using ::google::protobuf::Message;

using namespace NYT;
using namespace NYT::NTesting;

class TProtoFormatDerivationFixture
    : public TTestFixture
{
public:
    TProtoFormatDerivationFixture()
    {
        // Fill some data.
        {
            auto writer = GetClient()->CreateTableWriter<TNode>(GetWorkingDir() + "/urls1");
            writer->AddRow(TNode()("Host", "http://www.example.com")("Path", "/")("HttpCode", 302));
            writer->AddRow(TNode()("Host", "http://www.example.com")("Path", "/index.php")("HttpCode", 200));
            writer->Finish();
        }
        {
            auto writer = GetClient()->CreateTableWriter<TNode>(GetWorkingDir() + "/urls2");
            writer->AddRow(TNode()("Host", "http://www.example.com")("Path", "/index.htm")("HttpCode", 404));
            writer->AddRow(TNode()("Host", "http://www.other-example.com")("Path", "/")("HttpCode", 200));
            writer->Finish();
        }
        {
            auto writer = GetClient()->CreateTableWriter<TNode>(GetWorkingDir() + "/empty");
            writer->Finish();
        }
    }
};

template<class TRow>
const TRow& GetRow(TTableReader<TRow>* reader)
{
    return reader->GetRow();
}

template<class TRow>
const TRow& GetRow(TTableReader<Message>* reader)
{
    return reader->GetRow<TRow>();
}

template <class TInputRow, class TOutputRow>
class TMapper
    : public IMapper<TTableReader<TInputRow>, TTableWriter<TOutputRow>>
{
public:
    void Do(TTableReader<TInputRow>* reader, TTableWriter<TOutputRow>* writer) override
    {
        for (; reader->IsValid(); reader->Next()) {
            TUrlRow out = GetRow<TUrlRow>(reader);
            out.SetHttpCode(-out.GetHttpCode());
            writer->AddRow(out);
        }
    }
};

template <class TInputRow, class TOutputRow>
class TReduceCombiner
    : public IReducer<TTableReader<TInputRow>, TTableWriter<TOutputRow>>
{
public:
    void Do(TTableReader<TInputRow>* reader, TTableWriter<TOutputRow>* writer) override
    {
        const TString host = GetRow<TUrlRow>(reader).GetHost();
        ui64 httpCodeTotal = 0;
        for (; reader->IsValid(); reader->Next()) {
            const auto& row = GetRow<TUrlRow>(reader);
            httpCodeTotal += row.GetHttpCode();
        }
        TUrlRow urlRow;
        urlRow.SetHost(host);
        urlRow.SetHttpCode(httpCodeTotal);
        writer->AddRow(urlRow);
    }
};

template <class TInputRow, class TOutputRow>
class TReducer : public IReducer<TTableReader<TInputRow>, TTableWriter<TOutputRow>>
{
public:

    void Do(TTableReader<TInputRow>* reader, TTableWriter<TOutputRow>* writer) override
    {
        const TString host = GetRow<TUrlRow>(reader).GetHost();
        ui64 httpCodeTotal = 0;
        for (; reader->IsValid(); reader->Next()) {
            const auto& row = GetRow<TUrlRow>(reader);
            httpCodeTotal += row.GetHttpCode();
        }
        THostRow hostRow;
        hostRow.SetHost(host);
        hostRow.SetHttpCodeTotal(httpCodeTotal);
        writer->AddRow(hostRow);
    }
};

using TUnspecifiedInputMapper = TMapper<Message, TUrlRow>;
REGISTER_MAPPER(TUnspecifiedInputMapper)
using TUnspecifiedOutputMapper = TMapper<TUrlRow, Message>;
REGISTER_MAPPER(TUnspecifiedOutputMapper)
using TEverythingSpecifiedMapper = TMapper<TUrlRow, TUrlRow>;
REGISTER_MAPPER(TEverythingSpecifiedMapper)

using TUnspecifiedInputReduceCombiner = TReduceCombiner<Message, TUrlRow>;
REGISTER_REDUCER(TUnspecifiedInputReduceCombiner)
using TUnspecifiedOutputReduceCombiner = TReduceCombiner<TUrlRow, Message>;
REGISTER_REDUCER(TUnspecifiedOutputReduceCombiner)
using TEverythingSpecifiedReduceCombiner = TReduceCombiner<TUrlRow, TUrlRow>;
REGISTER_REDUCER(TEverythingSpecifiedReduceCombiner)

using TUnspecifiedInputReducer = TReducer<Message, THostRow>;
REGISTER_REDUCER(TUnspecifiedInputReducer)
using TEverythingSpecifiedReducer = TReducer<TUrlRow, THostRow>;
REGISTER_REDUCER(TEverythingSpecifiedReducer)


TEST(ProtoFormatDerivation, DifferentTypesMapperInput)
{
    TProtoFormatDerivationFixture fixture;

    fixture.GetClient()->MapReduce(
        TMapReduceOperationSpec()
        .ReduceBy("Host")
        .AddInput<TUrlRow>(fixture.GetWorkingDir() + "/urls1")
        .AddInput<TUrlRow>(fixture.GetWorkingDir() + "/urls2")
        //the only way to add different types' table is to make it empty
        .AddInput<THostRow>(fixture.GetWorkingDir() + "/empty")
        .AddOutput<THostRow>(fixture.GetWorkingDir() + "/host"),
        new TUnspecifiedInputMapper,
        new TEverythingSpecifiedReducer,
        TOperationOptions().Spec(TNode()("max_failed_job_count", 1)));
}

TEST(ProtoFormatDerivation, DifferentTypesNoMapperInput)
{
    TProtoFormatDerivationFixture fixture;

    try {
        fixture.GetClient()->MapReduce(
            TMapReduceOperationSpec()
                .ReduceBy("Host")
                .AddInput<TUrlRow>(fixture.GetWorkingDir() + "/urls1")
                .AddInput<THostRow>(fixture.GetWorkingDir() + "/urls2")
                .AddOutput<THostRow>(fixture.GetWorkingDir() + "/host"),
            nullptr,
            new TEverythingSpecifiedReducer,
            TOperationOptions().Spec(TNode()("max_failed_job_count", 1)));
        FAIL() << "operation was expected to fail";
    } catch (const TApiUsageError&) {
    }
}

TEST(ProtoFormatDerivation, UnspecifiedMapperOutput)
{
    TProtoFormatDerivationFixture fixture;

    try {
        fixture.GetClient()->MapReduce(
            TMapReduceOperationSpec()
                .ReduceBy("Host")
                .AddInput<TUrlRow>(fixture.GetWorkingDir() + "/urls1")
                .AddInput<TUrlRow>(fixture.GetWorkingDir() + "/urls2")
                .AddOutput<THostRow>(fixture.GetWorkingDir() + "/host"),
            new TUnspecifiedOutputMapper,
            new TUnspecifiedInputReducer,
            TOperationOptions().Spec(TNode()("max_failed_job_count", 1)));
        FAIL() << "operation was expected to fail";
    } catch (const TApiUsageError&) {
    }
}

TEST(ProtoFormatDerivation, HintedMapperOutput)
{
    TProtoFormatDerivationFixture fixture;

    fixture.GetClient()->MapReduce(
        TMapReduceOperationSpec()
        .ReduceBy("Host")
        .HintMapOutput<TUrlRow>()
        .AddInput<TUrlRow>(fixture.GetWorkingDir() + "/urls1")
        .AddInput<TUrlRow>(fixture.GetWorkingDir() + "/urls2")
        .AddOutput<THostRow>(fixture.GetWorkingDir() + "/host"),
        new TUnspecifiedOutputMapper,
        new TEverythingSpecifiedReducer,
        TOperationOptions().Spec(TNode()("max_failed_job_count", 1)));
}

TEST(ProtoFormatDerivation, UnspecifiedInputReduceCombiner)
{
    TProtoFormatDerivationFixture fixture;

    try {
        fixture.GetClient()->MapReduce(
            TMapReduceOperationSpec()
            .ReduceBy("Host")
            .AddInput<TUrlRow>(fixture.GetWorkingDir() + "/urls1")
            .AddInput<TUrlRow>(fixture.GetWorkingDir() + "/urls2")
            .AddOutput<THostRow>(fixture.GetWorkingDir() + "/host"),
            new TEverythingSpecifiedMapper,
            new TUnspecifiedInputReduceCombiner,
            new TEverythingSpecifiedReducer,
            TOperationOptions().Spec(TNode()("max_failed_job_count", 1)));
        FAIL() << "operation was expected to fail";
    } catch (const TApiUsageError&) {
    }
}

TEST(ProtoFormatDerivation, HintedInputReduceCombiner)
{
    TProtoFormatDerivationFixture fixture;

    fixture.GetClient()->MapReduce(
        TMapReduceOperationSpec()
        .ReduceBy("Host")
        .AddInput<TUrlRow>(fixture.GetWorkingDir() + "/urls1")
        .AddInput<TUrlRow>(fixture.GetWorkingDir() + "/urls2")
        .AddOutput<THostRow>(fixture.GetWorkingDir() + "/host")
        .HintReduceCombinerInput<TUrlRow>(),
        new TEverythingSpecifiedMapper,
        new TUnspecifiedInputReduceCombiner,
        new TEverythingSpecifiedReducer,
        TOperationOptions().Spec(TNode()("max_failed_job_count", 1)));
}

TEST(ProtoFormatDerivation, UnspecifiedOutputReduceCombiner)
{
    TProtoFormatDerivationFixture fixture;

    try {
        fixture.GetClient()->MapReduce(
            TMapReduceOperationSpec()
            .ReduceBy("Host")
            .AddInput<TUrlRow>(fixture.GetWorkingDir() + "/urls1")
            .AddInput<TUrlRow>(fixture.GetWorkingDir() + "/urls2")
            .AddOutput<THostRow>(fixture.GetWorkingDir() + "/host"),
            new TEverythingSpecifiedMapper,
            new TUnspecifiedOutputReduceCombiner,
            new TEverythingSpecifiedReducer,
            TOperationOptions().Spec(TNode()("max_failed_job_count", 1)));
        FAIL() << "operation was expected to fail";
    } catch (const TApiUsageError&) {
    }
}

TEST(ProtoFormatDerivation, HintedOutputReduceCombiner)
{
    TProtoFormatDerivationFixture fixture;

    fixture.GetClient()->MapReduce(
        TMapReduceOperationSpec()
        .ReduceBy("Host")
        .AddInput<TUrlRow>(fixture.GetWorkingDir() + "/urls1")
        .AddInput<TUrlRow>(fixture.GetWorkingDir() + "/urls2")
        .AddOutput<THostRow>(fixture.GetWorkingDir() + "/host")
        .HintReduceCombinerOutput<TUrlRow>(),
        new TEverythingSpecifiedMapper,
        new TUnspecifiedOutputReduceCombiner,
        new TEverythingSpecifiedReducer,
        TOperationOptions().Spec(TNode()("max_failed_job_count", 1)));
}

TEST(ProtoFormatDerivation, UnspecifiedReducerInput)
{
    TProtoFormatDerivationFixture fixture;

    try {
        fixture.GetClient()->MapReduce(
            TMapReduceOperationSpec()
                .ReduceBy("Host")
                .AddInput<TNode>(fixture.GetWorkingDir() + "/urls1")
                .AddInput<TNode>(fixture.GetWorkingDir() + "/urls2")
                .AddOutput<THostRow>(fixture.GetWorkingDir() + "/host"),
            nullptr,
            new TUnspecifiedInputReducer,
            TOperationOptions().Spec(TNode()("max_failed_job_count", 1)));
        FAIL() << "operation was expected to fail";
    } catch (const TApiUsageError&) {
    }
}

TEST(ProtoFormatDerivation, ReducerInputFromOperationInput)
{
    TProtoFormatDerivationFixture fixture;

    fixture.GetClient()->MapReduce(
        TMapReduceOperationSpec()
            .ReduceBy("Host")
            .AddInput<TUrlRow>(fixture.GetWorkingDir() + "/urls1")
            .AddInput<TUrlRow>(fixture.GetWorkingDir() + "/urls2")
            .AddOutput<THostRow>(fixture.GetWorkingDir() + "/host"),
        nullptr,
        new TUnspecifiedInputReducer,
        TOperationOptions().Spec(TNode()("max_failed_job_count", 1)));
}

TEST(ProtoFormatDerivation, EverythingSpecified)
{
    TProtoFormatDerivationFixture fixture;

    fixture.GetClient()->MapReduce(
        TMapReduceOperationSpec()
        .ReduceBy("Host")
        .AddInput<TUrlRow>(fixture.GetWorkingDir() + "/urls1")
        .AddInput<TUrlRow>(fixture.GetWorkingDir() + "/urls2")
        .AddOutput<THostRow>(fixture.GetWorkingDir() + "/host"),
        new TEverythingSpecifiedMapper,
        new TEverythingSpecifiedReduceCombiner,
        new TEverythingSpecifiedReducer,
        TOperationOptions().Spec(TNode()("max_failed_job_count", 1)));
}
