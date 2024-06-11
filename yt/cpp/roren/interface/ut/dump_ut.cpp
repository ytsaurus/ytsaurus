#include <yt/cpp/roren/interface/ut/proto/data.pb.h>

#include <yt/cpp/roren/interface/roren.h>
#include <yt/cpp/roren/interface/private/dummy_pipeline.h>

#include <yt/yt/core/json/json_writer.h>

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/gtest/gtest.h>


namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

TEST(TDumpPipeline, Simple)
{
    auto pipeline = NPrivate::MakeDummyPipeline();
    pipeline
        | "MeaningfulRead" >> DummyRead<i64>()
        | "MeaningfulParDo" >> ParDo([] (const i64& in) {
            return static_cast<ui64>(in);
        })
        | "MeaningfulWrite" >> DummyWrite<ui64>();

    EXPECT_THAT(pipeline.DumpDot(), NGTest::GoldenFileEq(SRC_("canondata/TDumpPipeline_Simple.dot")));

    {
        TStringStream stream;
        {
            auto config = NYT::New<NYT::NJson::TJsonFormatConfig>();
            config->Format = NYT::NJson::EJsonFormat::Pretty;
            auto jsonConsumer = NYT::NJson::CreateJsonConsumer(&stream, NYT::NYson::EYsonType::Node, config);
            pipeline.Dump(jsonConsumer.get());
            jsonConsumer->Flush();
        }
        EXPECT_THAT(stream.Str(), NGTest::GoldenFileEq(SRC_("canondata/TDumpPipeline_Simple.json")));
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TDumpPipeline, Complex)
{
    auto pipeline = NPrivate::MakeDummyPipeline();
    auto data = pipeline
        | "MeaningfulRead" >> DummyRead<i64>();

    data
        | "first" >> ParDo([] (const i64& batch) {
            return static_cast<size_t>(batch);
        })
        | "second" >> ParDo([] (const size_t& x) {
            return x + 2;
        })
        | "third" >> ParDo([] (const size_t&) {
            return;
        });

    auto msg = data
        | ParDo([] (const i64& batch) {
            return static_cast<double>(batch);
        })
        | ParDo([] (const double& msgs) {
            return static_cast<float>(msgs);
        });

    msg
        | ParDo([] (const float&) {
            return;
        });

    msg | "Size" >> ParDo([] (const float& msg) {
            return static_cast<char>(static_cast<ui64>(msg));
        })
        | ParDo([] (const char&) {
            return;
        });

    EXPECT_THAT(pipeline.DumpDot(), NGTest::GoldenFileEq(SRC_("canondata/TDumpPipeline_Complex.dot")));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
