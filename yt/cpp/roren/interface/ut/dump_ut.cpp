#include <yt/cpp/roren/interface/ut/proto/data.pb.h>

#include <yt/cpp/roren/interface/roren.h>
#include <yt/cpp/roren/interface/private/dummy_pipeline.h>

#include <yt/cpp/roren/bigrt/bigrt.h>
#include <yt/cpp/roren/bigrt/concurrency_transforms.h>

#include <yt/yt/core/json/json_writer.h>

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/gtest/gtest.h>


namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

TEST(TDumpPipeline, Simple)
{
    auto pipeline = NPrivate::MakeDummyPipeline();
    pipeline
        | "MeaningfulRead" >> DummyRead<TInputMessage>()
        | "MeaningfulParDo" >> ParDo([] (const TInputMessage& in) {
            TOutputMessage out;
            out.SetValue(in.GetValue());
            out.SetSize(in.GetValue().size());
            return out;
        })
        | "MeaningfulWrite" >> DummyWrite<TOutputMessage>();

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
        | "MeaningfulRead" >> DummyRead<NBigRT::TMessageBatch>();

    data | "first" >> ParDo([] (const NBigRT::TMessageBatch& batch) {
            return batch.Messages.size();
        }) | StartConcurrencyBlock(TConcurrentBlockConfig{})
        | "second" >> ParDo([] (const size_t& x) {
            return x + 2;
        }) | "third" >> ParDo([] (const size_t&) {
        });

    auto msg = data | ParDo([] (const NBigRT::TMessageBatch& batch) {
            return batch.Messages;
        })
        | ParDo([] (const TVector<NBigRT::TMessageBatch::TMessage>& msgs) {
            return msgs[0];
        });

    msg | ParDo([] (const NBigRT::TMessageBatch::TMessage&) {
        });

    msg | "Size" >> ParDo([] (const NBigRT::TMessageBatch::TMessage& msg) {
            return msg.GetDataSize();
        })
        | ParDo([] (const ui64&) {
        });

    EXPECT_THAT(pipeline.DumpDot(), NGTest::GoldenFileEq(SRC_("canondata/TDumpPipeline_Complex.dot")));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
