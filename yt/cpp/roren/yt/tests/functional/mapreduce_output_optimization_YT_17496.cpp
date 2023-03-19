#include <library/cpp/testing/unittest/registar.h>

#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>
#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/roren/yt/yt.h>
#include <yt/cpp/roren/yt/yt_read.h>
#include <yt/cpp/roren/yt/yt_write.h>
#include <yt/cpp/roren/transforms/dict_join.h>

#include <util/generic/ptr.h>
#include <util/system/env.h>

using namespace NYT::NTesting;
using namespace NRoren;

void MakeDataTable(const NYT::IClientPtr& client, const TString& table, const std::vector<NYT::TNode>& data)
{
    auto writer = client->CreateTableWriter<NYT::TNode>(table);
    for (const auto& row : data) {
        writer->AddRow(row);
    }
    writer->Finish();
}

void MakeAuctionTable(const NYT::IClientPtr& client, const TString& auctionTablePath)
{
    const std::vector<NYT::TNode> data = {
        NYT::TNode()("ID", 3001ULL)("CategoryID", 1ULL)("ExpireTime", 1658226340ULL),
        NYT::TNode()("ID", 318ULL)("CategoryID", 1ULL)("ExpireTime", 1657944456ULL),
    };
    MakeDataTable(client, auctionTablePath, data);
}

void MakebidCollectionTable(const NYT::IClientPtr& client, const TString& bidCollectionTablePath)
{
    const std::vector<NYT::TNode> data = {
        NYT::TNode()("AuctionID", 3001ULL)("Price", 36986426LL)("BidTime", 1658714497ULL),
        NYT::TNode()("AuctionID", 318ULL)("Price", 41064165LL)("BidTime", 1658714600ULL),
    };
    MakeDataTable(client, bidCollectionTablePath, data);
}

void MakeData(const NYT::IClientPtr& client, const TString& auctionTablePath, const TString& bidCollectionTablePath)
{
    MakeAuctionTable(client, auctionTablePath);
    MakebidCollectionTable(client, bidCollectionTablePath);
}

void CheckResult(const NYT::IClientPtr& client, const TString& resultTablePath)
{
    const std::vector<NYT::TNode> expectedData {
        NYT::TNode()("CategoryID", 1ULL)("AvgClosePrice", 39025295.5),
    };
    std::vector<NYT::TNode> resultData;
    auto reader = client->CreateTableReader<NYT::TNode>(resultTablePath);
    for (auto& cursor : *reader) {
        resultData.emplace_back(cursor.MoveRow());
    }
    UNIT_ASSERT_VALUES_EQUAL(resultData, expectedData);
}


template <typename T>
class TMaxCombineFn
    : public NRoren::ICombineFn<T, T, T> {
public:
    T CreateAccumulator() override {
        return std::numeric_limits<T>::min();
    }

    void AddInput(T* accum, const T& input) override {
        *accum = std::max(*accum, input);
    }

    T MergeAccumulators(NRoren::TInput<T>& accums) override {
        T t = CreateAccumulator();
        while (const T* cur = accums.Next()) {
            t = std::max(t, *cur);
        }
        return t;
    }

    T ExtractOutput(const T& accum) override {
        return accum;
    }
};

template <typename T>
TIntrusivePtr<TMaxCombineFn<T>> MyMax()
{
    return MakeIntrusive<TMaxCombineFn<T>>();
}

struct TAuctionInfo {
    ui64 CategoryID;
    ui64 ExpireTime;

    Y_SAVELOAD_DEFINE(CategoryID, ExpireTime);
};

struct TBidInfo {
    ui64 BidTime;
    i64 Price;

    Y_SAVELOAD_DEFINE(BidTime, Price);
};

struct TAuctionKey {
    ui64 AuctionID;
    ui64 CategoryID; // store category id just once;

    Y_SAVELOAD_DEFINE(AuctionID, CategoryID);
};


NRoren::TPipeline MakePipeline(const TString& proxy, const TString& auctionTablePath, const TString& bidCollectionTablePath, const TString& resultTablePath)
{
    TYtPipelineConfig config;
    config.SetCluster(proxy);
    config.SetWorkingDir("//tmp");
    auto pipeline = MakeYtPipeline(config);

    const auto& auctionCollection = pipeline | YtRead<NYT::TNode>(auctionTablePath);
    const auto& bidCollection = pipeline | YtRead<NYT::TNode>(bidCollectionTablePath);

    // clang-format off
    auto auctionDict = auctionCollection
        | ParDo([](const NYT::TNode& node) {
            TKV<ui64, TAuctionInfo> kv;
            kv.Key() = node["ID"].AsUint64();
            kv.Value() = TAuctionInfo{
                .CategoryID = node["CategoryID"].AsUint64(),
                .ExpireTime = node["ExpireTime"].AsUint64()
            };
            return kv;
        });

    bidCollection
        | ParDo([](const NYT::TNode& node) {
            TKV<ui64, TBidInfo> kv;
            kv.Key() = node["AuctionID"].AsUint64();
            kv.Value() = TBidInfo{
                .BidTime = node["BidTime"].AsUint64(),
                .Price = node["Price"].AsInt64()
            };
            return kv;
        })
        | DictJoin(auctionDict)
        | ParDo([](const std::tuple<ui64, TBidInfo, std::optional<TAuctionInfo>>& input, TOutput<TKV<TAuctionKey, i64>>& out) {
            auto& [auctionID, bidInfo, auctionInfo] = input;
            Y_VERIFY(auctionInfo.has_value());

            TKV<TAuctionKey, i64> kv;
            kv.Key() = TAuctionKey{
                .AuctionID = auctionID,
                .CategoryID = auctionInfo->CategoryID
            },
            kv.Value() = bidInfo.Price;
            out.Add(kv);
        })
        | CombinePerKey(MyMax<i64>())
        | ParDo([] (const TKV<TAuctionKey, i64>& kv) {
            TKV<ui64, i64> result;
            result.Key() = kv.Key().CategoryID;
            result.Value() = kv.Value();
            return result;
        })
        | GroupByKey()
        | ParDo([] (const TKV<ui64, TInputPtr<i64>>& kv) {
            ui64 priceSum = 0;
            ui64 priceCnt = 0;
            for (const auto& v : kv.Value()) {
                priceSum += v;
                ++priceCnt;
            }

            NYT::TNode result;
            result["CategoryID"] = kv.Key();
            result["AvgClosePrice"] = static_cast<double>(priceSum) / priceCnt;
            return result;
        })
        | YtWrite<NYT::TNode>(
            resultTablePath,
            NYT::TTableSchema()
            .AddColumn("CategoryID", NTi::Uint64())
            .AddColumn("AvgClosePrice", NTi::Double())
        );

    return pipeline;
}

static TString GetTestProxy()
{
    // TODO: this should be returned by TTestFixture
    return GetEnv("YT_PROXY");
}

Y_UNIT_TEST_SUITE(RorenYtMapReduceOutputOptimizationYT17496) {
    Y_UNIT_TEST(All)
    {
        TTestFixture f;
        const auto& ytClient = f.GetClient();
        const auto auctionTablePath = f.GetWorkingDir() + "/Auction";
        const auto bidCollectionTablePath = f.GetWorkingDir() + "/BidCollection";
        const auto resultTablePath = f.GetWorkingDir() + "/result";

        MakeData(ytClient, auctionTablePath, bidCollectionTablePath);
        auto pipeline = MakePipeline(GetTestProxy(), auctionTablePath, bidCollectionTablePath, resultTablePath);
        pipeline.Run();

        CheckResult(ytClient, resultTablePath);
    }
}


