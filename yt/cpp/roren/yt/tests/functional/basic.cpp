#include <yt/cpp/roren/interface/roren.h>
#include <yt/cpp/roren/yt/yt.h>
#include <yt/cpp/roren/transforms/sum.h>

#include <mapreduce/yt/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/env.h>

using namespace NYT;
using namespace NYT::NTesting;
using namespace NRoren;

static TString GetTestProxy()
{
    // TODO: this should be returned by TTestFixture
    return GetEnv("YT_PROXY");
}

Y_UNIT_TEST_SUITE(RorenYt) {
    Y_UNIT_TEST(Read)
    {
        TTestFixture f;
        const auto& ytClient = f.GetClient();
        auto inTablePath = f.GetWorkingDir() + "/in-table";
        auto outTablePath = f.GetWorkingDir() + "/out-table";
        const std::vector<TNode> data = {
            TNode()("value", "foo"),
            TNode()("value", "bar"),
        };

        {
            auto writer = ytClient->CreateTableWriter<TNode>(inTablePath);
            for (const auto& row : data) {
                writer->AddRow(row);
            }
            writer->Finish();
        }

        TYtPipelineConfig config;
        config.SetCluster(GetTestProxy());
        config.SetWorkingDir("//tmp");
        auto pipeline = MakeYtPipeline(config);
        auto inTable = pipeline | YtRead<TNode>(inTablePath);
        inTable | YtWrite<TNode>(
            outTablePath,
            TTableSchema()
                .AddColumn("value", NTi::String())
        );

        pipeline.Run();

        std::vector<TNode> readData;
        {
            auto reader = ytClient->CreateTableReader<TNode>(outTablePath);
            for (auto& cursor : *reader) {
                readData.emplace_back(cursor.MoveRow());
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(data, readData);
    }

    Y_UNIT_TEST(Compute)
    {
        TTestFixture f;
        const auto& ytClient = f.GetClient();
        auto inTablePath = f.GetWorkingDir() + "/in-table";
        auto outTablePath = f.GetWorkingDir() + "/out-table";
        const std::vector<TNode> data = {
            TNode()("value", "foo"),
            TNode()("value", "bar"),
        };

        {
            auto writer = ytClient->CreateTableWriter<TNode>(inTablePath);
            for (const auto& row : data) {
                writer->AddRow(row);
            }
            writer->Finish();
        }

        TYtPipelineConfig config;
        config.SetCluster(GetTestProxy());
        config.SetWorkingDir("//tmp");
        auto pipeline = MakeYtPipeline(config);
        pipeline
            | YtRead<TNode>(inTablePath)
            | ParDo([] (const TNode& node) -> TNode {
                auto result = node;
                result["value"] = result["value"].AsString() + result["value"].AsString();
                return result;
            })
            | YtWrite<TNode>(
                    outTablePath,
                    TTableSchema()
                        .AddColumn("value", NTi::String())
            );
        pipeline.Run();

        std::vector<TNode> readData;
        {
            auto reader = ytClient->CreateTableReader<TNode>(outTablePath);
            for (auto& cursor : *reader) {
                readData.emplace_back(cursor.MoveRow());
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(
            readData,
            std::vector<TNode>({
                TNode()("value", "foofoo"),
                TNode()("value", "barbar")
            }));
    }

    static const auto BaTag = TTypeTag<TNode>("ba");
    static const auto FoTag = TTypeTag<TNode>("fo");

    class TSplitFoBa : public IDoFn<TNode, TMultiRow>
    {
    public:
        std::vector<TDynamicTypeTag> GetOutputTags() const override
        {
            return {FoTag, BaTag};
        }

        void Do(const TNode& node, TMultiOutput& output) override {
            auto result = node;
            if (result["value"].AsString().StartsWith("ba")) {
                result["fo"] = false;
                output.GetOutput(BaTag).Add(result);
            } else {
                result["fo"] = true;
                output.GetOutput(FoTag).Add(result);
            }
        }
    };

    Y_UNIT_TEST(ParDoMultipleOutputs)
    {
        TTestFixture f;
        const auto& ytClient = f.GetClient();
        auto inTablePath = f.GetWorkingDir() + "/in-table";
        auto foTablePath = f.GetWorkingDir() + "/fo-out-table";
        auto baTablePath = f.GetWorkingDir() + "/ba-out-table";
        const std::vector<TNode> data = {
            TNode()("value", "foo"),
            TNode()("value", "bar"),
            TNode()("value", "baz"),
        };

        {
            auto writer = ytClient->CreateTableWriter<TNode>(inTablePath);
            for (const auto& row : data) {
                writer->AddRow(row);
            }
            writer->Finish();
        }

        TYtPipelineConfig config;
        config.SetCluster(GetTestProxy());
        config.SetWorkingDir("//tmp");
        auto pipeline = MakeYtPipeline(config);

        const auto& [ba, fo] = (
            pipeline
            | YtRead<TNode>(inTablePath)
            | MakeParDo<TSplitFoBa>()
        ).Unpack(BaTag, FoTag);

        auto schema = TTableSchema()
            .AddColumn("value", NTi::String())
            .AddColumn("fo", NTi::Bool());
        ba | YtWrite<TNode>(baTablePath, schema);
        fo | YtWrite<TNode>(foTablePath, schema);
        pipeline.Run();

        std::vector<TNode> foReadData;
        {
            auto reader = ytClient->CreateTableReader<TNode>(foTablePath);
            for (auto& cursor : *reader) {
                foReadData.emplace_back(cursor.MoveRow());
            }
        }

        std::vector<TNode> baReadData;
        {
            auto reader = ytClient->CreateTableReader<TNode>(baTablePath);
            for (auto& cursor : *reader) {
                baReadData.emplace_back(cursor.MoveRow());
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(
            foReadData,
            std::vector<TNode>({
                TNode()("value", "foo")("fo", true),
            }));

        UNIT_ASSERT_VALUES_EQUAL(
            baReadData,
            std::vector<TNode>({
                TNode()("value", "bar")("fo", false),
                TNode()("value", "baz")("fo", false),
            }));
    }

    Y_UNIT_TEST(GroupByKey)
    {
        TTestFixture f;
        auto inTablePath = f.GetWorkingDir() + "/in-table";
        auto outTablePath = f.GetWorkingDir() + "/out-table";
        const auto& ytClient = f.GetClient();
        {
            auto writer = ytClient->CreateTableWriter<TNode>(inTablePath);
            writer->AddRow(TNode()("key", "foo")("value", 1));
            writer->AddRow(TNode()("key", "bar")("value", 2));
            writer->AddRow(TNode()("key", "baz")("value", 3));
            writer->AddRow(TNode()("key", "foo")("value", 4));
            writer->AddRow(TNode()("key", "baz")("value", 5));
            writer->Finish();
        }

        TYtPipelineConfig config;
        config.SetCluster(GetTestProxy());
        config.SetWorkingDir("//tmp");
        auto pipeline = MakeYtPipeline(config);
        pipeline
            | YtRead<TNode>(inTablePath)
            | ParDo([] (const TNode& node) {
                TKV<TString, int> kv;
                kv.Key() = node["key"].AsString();
                kv.Value() = node["value"].AsInt64();
                return kv;
            })
            | GroupByKey()
            | ParDo([] (const TKV<TString, TInputPtr<int>>& kv) {
                TNode result;
                std::vector<int> allValues;
                for (const auto& v : kv.Value()) {
                    allValues.emplace_back(v);
                }
                std::sort(allValues.begin(), allValues.end());

                result["key"] = kv.Key();
                auto& list = result["value"] = TNode::CreateList();
                for (const auto& v : allValues) {
                    list.Add(v);
                }
                return result;
            })
            | YtWrite<TNode>(
                outTablePath,
                TTableSchema()
                    .AddColumn("key", NTi::String())
                    .AddColumn("value", NTi::List(NTi::Int64()))
            );

        pipeline.Run();


        const std::vector<TNode> expected = {
            TNode()("key", "bar")("value", TNode().Add(2)),
            TNode()("key", "baz")("value", TNode().Add(3).Add(5)),
            TNode()("key", "foo")("value", TNode().Add(1).Add(4)),
        };
        std::vector<TNode> actual;
        {
            auto reader = ytClient->CreateTableReader<TNode>(outTablePath);
            for (const auto& cursor : *reader) {
                actual.push_back(cursor.GetRow());
            }
        }
        std::sort(actual.begin(), actual.end(), [] (const auto& lhs, const auto& rhs) {
            return lhs["key"].AsString() < rhs["key"].AsString();
        });

        UNIT_ASSERT_VALUES_EQUAL(expected, actual);
    }

    Y_UNIT_TEST(CombineByKey)
    {
        TTestFixture f;
        auto inTablePath = f.GetWorkingDir() + "/in-table";
        auto outTablePath = f.GetWorkingDir() + "/out-table";
        const auto& ytClient = f.GetClient();
        {
            auto writer = ytClient->CreateTableWriter<TNode>(inTablePath);
            writer->AddRow(TNode()("key", "foo")("value", 1));
            writer->AddRow(TNode()("key", "bar")("value", 2));
            writer->AddRow(TNode()("key", "baz")("value", 3));
            writer->AddRow(TNode()("key", "foo")("value", 4));
            writer->AddRow(TNode()("key", "baz")("value", 5));
            writer->Finish();
        }

        TYtPipelineConfig config;
        config.SetCluster(GetTestProxy());
        config.SetWorkingDir("//tmp");
        auto pipeline = MakeYtPipeline(config);
        pipeline
            | YtRead<TNode>(inTablePath)
            | ParDo([] (const TNode& node) {
                TKV<TString, int> kv;
                kv.Key() = node["key"].AsString();
                kv.Value() = node["value"].AsInt64();
                return kv;
            })
            | CombinePerKey(Sum<int>())
            | ParDo([] (const TKV<TString, int>& kv) {
                TNode result;
                result["key"] = kv.Key();
                result["value"] = kv.Value();
                return result;
            })
            | YtWrite<TNode>(
                outTablePath,
                TTableSchema()
                    .AddColumn("key", NTi::String())
                    .AddColumn("value", NTi::Int64())
            );

        pipeline.Run();

        const std::vector<TNode> expected = {
            TNode()("key", "bar")("value", 2),
            TNode()("key", "baz")("value", 8),
            TNode()("key", "foo")("value", 5),
        };
        std::vector<TNode> actual;
        {
            auto reader = ytClient->CreateTableReader<TNode>(outTablePath);
            for (const auto& cursor : *reader) {
                actual.push_back(cursor.GetRow());
            }
        }
        std::sort(actual.begin(), actual.end(), [] (const auto& lhs, const auto& rhs) {
            return lhs["key"].AsString() < rhs["key"].AsString();
        });

        UNIT_ASSERT_VALUES_EQUAL(expected, actual);
    }

    Y_UNIT_TEST(CoGroupByKey)
    {
        TTestFixture f;
        auto inTablePath1 = f.GetWorkingDir() + "/in-table-1";
        auto inTablePath2 = f.GetWorkingDir() + "/in-table-2";
        auto outTablePath = f.GetWorkingDir() + "/out-table";

        const auto& ytClient = f.GetClient();
        {
            auto writer = ytClient->CreateTableWriter<TNode>(inTablePath1);
            writer->AddRow(TNode()("key", "foo")("value", 1));
            writer->AddRow(TNode()("key", "bar")("value", 2));
            writer->AddRow(TNode()("key", "baz")("value", 3));
            writer->AddRow(TNode()("key", "foo")("value", 4));
            writer->AddRow(TNode()("key", "baz")("value", 5));
            writer->Finish();
        }

        {
            auto writer = ytClient->CreateTableWriter<TNode>(inTablePath2);
            writer->AddRow(TNode()("key", "foo")("value", "1"));
            writer->AddRow(TNode()("key", "bar")("value", "2"));
            writer->AddRow(TNode()("key", "baz")("value", "3"));
            writer->AddRow(TNode()("key", "foo")("value", "4"));
            writer->AddRow(TNode()("key", "baz")("value", "5"));
            writer->Finish();
        }

        TYtPipelineConfig config;
        config.SetCluster(GetTestProxy());
        config.SetWorkingDir("//tmp");
        auto pipeline = MakeYtPipeline(config);

        auto read = pipeline | YtRead<TNode>(inTablePath1);
        auto read2 = pipeline | YtRead<TNode>(inTablePath2);

        static const auto Input1Tag = TTypeTag<TKV<TString, int>>{"input1"};
        static const auto Input2Tag = TTypeTag<TKV<TString, TString>>{"input2"};

        auto input1 = read
            | ParDo([] (const TNode& node) {
                TKV<TString, int> kv;
                kv.Key() = node["key"].AsString();
                kv.Value() = node["value"].AsInt64();
                return kv;
            });

        auto input2 = read2
            | ParDo([] (const TNode& node) {
                TKV<TString, TString> kv;
                kv.Key() = node["key"].AsString();
                kv.Value() = node["value"].AsString() + "0";
                return kv;
            });

        auto multiPCollection = TMultiPCollection{Input1Tag, input1, Input2Tag, input2};

        auto output = multiPCollection | CoGroupByKey() | ParDo([] (const TCoGbkResult& gbk) {
            static const auto Input1Tag = TTypeTag<TKV<TString, int>>{"input1"};
            static const auto Input2Tag = TTypeTag<TKV<TString, TString>>{"input2"};

            TNode result;
            result["key"] = gbk.GetKey<TString>();

            auto& list = result["value"] = TNode::CreateList();

            for (const auto& v : gbk.GetInput(Input1Tag)) {
                list.Add(v.Value());
            }

            for (const auto& v : gbk.GetInput(Input2Tag)) {
                list.Add(std::atoi(v.Value().c_str()));
            }

            return result;
        });

        output | YtWrite<TNode>(
            outTablePath,
            TTableSchema()
                .AddColumn("key", NTi::String())
                .AddColumn("value", NTi::List(NTi::Int64()))
        );

        pipeline.Run();


        const std::vector<TNode> expected = {
            TNode()("key", "bar")("value", TNode().Add(2).Add(20)),
            TNode()("key", "baz")("value", TNode().Add(3).Add(5).Add(30).Add(50)),
            TNode()("key", "foo")("value", TNode().Add(1).Add(4).Add(10).Add(40)),
        };
        std::vector<TNode> actual;
        {
            auto reader = ytClient->CreateTableReader<TNode>(outTablePath);
            for (const auto& cursor : *reader) {
                actual.push_back(cursor.GetRow());
            }
        }
        std::sort(actual.begin(), actual.end(), [] (const auto& lhs, const auto& rhs) {
            return lhs["key"].AsString() < rhs["key"].AsString();
        });

        UNIT_ASSERT_VALUES_EQUAL(expected, actual);
    }
}
