#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/misc/remedian_splitter.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Helper that generates limited random data and provides sorted copy of it.
//! Can handle any type if it supports conversion from ui64.
//! Can be replaced with RandomNumber<ui64> in cycle and std::ranges::sort, but it a bit longer.
template <class T>
void GenerateRandomData(std::vector<T>& data, std::vector<T>& dataSorted)
{
    static std::vector<ui64> randomData;
    static std::vector<ui64> halfSorted;
    dataSorted.resize(data.size());
    halfSorted.resize(data.size());
    if constexpr (std::is_same_v<T, ui64>) {
        std::swap(data, randomData);
    } else {
        randomData.resize(data.size());
    }

    constexpr ssize_t base = 1024;
    for (auto& value : randomData) {
        value = RandomNumber<ui64>() % (base * base);
    }

    // Perform simple radix sort.
    std::array<std::array<int, 2>, base> counts = {};
    for (const auto& value : randomData) {
        ui64 parts[2] = {value % base, value / base};
        counts[parts[0]][0]++;
        counts[parts[1]][1]++;
    }
    std::array<int, 2> accumulated = {};
    for (auto& count : counts) {
        std::array<int, 2> save = count;
        count = accumulated;
        accumulated[0] += save[0];
        accumulated[1] += save[1];
    }

    for (const auto& value : randomData) {
        halfSorted[counts[value % base][0]++] = value;
    }
    for (const auto& value : halfSorted) {
        dataSorted[counts[value / base][1]++] = T{value};
    }

    if constexpr (std::is_same_v<T, ui64>) {
        std::swap(data, randomData);
    } else {
        for (ssize_t i = 0; i < std::ssize(data); ++i) {
            data[i] = T{randomData[i]};
        }
    }
}

//! Basic check of entire API.
TEST(TRemedianSplitter, BasicTest)
{
    struct TArgs
    {
        ssize_t PartCount;
        ssize_t DesiredWindowSize;
    };

    {
        TRemedianSplitter<int> splitter(10, 13000);
        EXPECT_EQ(splitter.GetInnerHeight(), 1);
        EXPECT_EQ(splitter.GetLeafSize(), 120);
        EXPECT_EQ(splitter.GetInnerSize(0), 101);
    }
    {
        TRemedianSplitter<int> splitter(10, 13500);
        EXPECT_EQ(splitter.GetInnerHeight(), 2);
        EXPECT_EQ(splitter.GetLeafSize(), 30);
        EXPECT_EQ(splitter.GetInnerSize(0), 21);
        EXPECT_EQ(splitter.GetInnerSize(1), 21);
    }
    {
        TRemedianSplitter<int> splitter(2, 1000);
        EXPECT_EQ(splitter.GetInnerHeight(), 1);
        EXPECT_EQ(splitter.GetLeafSize(), 34);
        EXPECT_EQ(splitter.GetInnerSize(0), 29);
    }
    {
        TRemedianSplitter<int> splitter(2, 10000);
        EXPECT_EQ(splitter.GetInnerHeight(), 2);
        EXPECT_EQ(splitter.GetLeafSize(), 22);
        EXPECT_EQ(splitter.GetInnerSize(0), 21);
        EXPECT_EQ(splitter.GetInnerSize(1), 21);
    }
    {
        TRemedianSplitter<int> splitter(10, 10);
        EXPECT_EQ(splitter.GetInnerHeight(), 1);
        EXPECT_EQ(splitter.GetLeafSize(), 30);
        EXPECT_EQ(splitter.GetInnerSize(0), 21);
    }

    for (const auto& args : {TArgs{10, 13000}, TArgs{10, 13500}, TArgs{2, 1000}, TArgs{2, 10000}, TArgs{10, 10}}) {
        TRemedianSplitter<int> splitter(args.PartCount, args.DesiredWindowSize);
        EXPECT_EQ(splitter.GetSplitterCount(), args.PartCount - 1);
        EXPECT_EQ(splitter.GetLeafSize(), splitter.GetLeafSizeMultiplier() * args.PartCount);
        ssize_t expectedWindowSize = splitter.GetLeafSize();
        for (ssize_t i = 0; i < splitter.GetInnerHeight(); i++) {
            EXPECT_EQ(splitter.GetInnerSize(i), splitter.GetInnerSizeMultiplier(i) * args.PartCount + 1);
            expectedWindowSize *= splitter.GetInnerSize(i);
        }
        EXPECT_EQ(splitter.GetRealWindowSize(), expectedWindowSize);
        EXPECT_EQ(splitter.GetUpdateWindowSize(), splitter.GetRealWindowSize() / splitter.GetInnerSize(splitter.GetInnerHeight() - 1));
        EXPECT_FALSE(splitter.IsResultReady());
        EXPECT_EQ(splitter.GetResultVersion(), 0);
    }

    constexpr ssize_t tryCount = 100;
    using T = ui64;
    for (const auto& args : {TArgs{10, 10000}, TArgs{2, 10000}, TArgs{32, 10000}}) {
        TRemedianSplitter<T> splitter(args.PartCount, args.DesiredWindowSize);
        ssize_t windowSize = splitter.GetRealWindowSize();
        std::vector<T> data(windowSize);
        std::vector<T> dataSorted(windowSize);
        std::vector<double> averageQuantileError(splitter.GetSplitterCount());
        for (ssize_t k = 0; k < tryCount; k++) {
            if (k != 0) {
                splitter.Clear();
            }
            GenerateRandomData(data, dataSorted);
            for (const auto& value : data) {
                EXPECT_FALSE(splitter.IsResultReady());
                EXPECT_EQ(splitter.GetResultVersion(), 0);
                splitter.Push(value);
            }
            EXPECT_TRUE(splitter.IsResultReady());
            EXPECT_NE(splitter.GetResultVersion(), 0);

            for (ssize_t i = 0; i < splitter.GetSplitterCount(); i++) {
                auto value = splitter.Result()[i];
                auto it = std::lower_bound(dataSorted.begin(), dataSorted.end(), value);
                EXPECT_NE(it, dataSorted.end());
                ssize_t actualPos = std::distance(dataSorted.begin(), it);
                ssize_t expectPos = windowSize / args.PartCount * (i + 1);
                double error = std::abs((double)actualPos - (double)expectPos) / windowSize;
                EXPECT_LE(error, 0.05);
                averageQuantileError[i] += error / tryCount;
            }
        }
        for (const auto& error : averageQuantileError) {
            EXPECT_LE(error, 0.01);
        }
    }
}

//! Check how the result is updated upon receiving new data.
TEST(TRemedianSplitter, UpdateTest)
{
    struct TArgs
    {
        ssize_t PartCount;
        ssize_t DesiredWindowSize;
    };

    constexpr ssize_t tryCount = 100;
    using T = ui64;
    for (const auto& args : {TArgs{10, 10000}, TArgs{2, 10000}}) {
        TRemedianSplitter<T> splitter(args.PartCount, args.DesiredWindowSize);
        ssize_t windowSize = splitter.GetRealWindowSize();
        std::vector<T> data(windowSize);
        std::vector<T> dataSorted(windowSize);
        std::vector<double> averageQuantileError(splitter.GetSplitterCount());
        for (ssize_t k = 0; k < tryCount; k++) {
            if (k != 0) {
                splitter.Clear();
            }
            GenerateRandomData(data, dataSorted);
            for (const auto& value : data) {
                EXPECT_FALSE(splitter.IsResultReady());
                EXPECT_EQ(splitter.GetResultVersion(), 0);
                splitter.Push(value);
            }
            EXPECT_TRUE(splitter.IsResultReady());
            EXPECT_NE(splitter.GetResultVersion(), 0);

            for (auto& value : data) {
                value += 1000000;
            }
            for (auto& value : dataSorted) {
                value += 1000000;
            }
            auto lastResultVersion = splitter.GetResultVersion();
            ssize_t numPushes = 0;
            for (const auto& value : data) {
                splitter.Push(value);
                numPushes++;
                if (numPushes % splitter.GetUpdateWindowSize() != 0) {
                    EXPECT_EQ(splitter.GetResultVersion(), lastResultVersion);
                } else {
                    EXPECT_NE(splitter.GetResultVersion(), lastResultVersion);
                    lastResultVersion = splitter.GetResultVersion();
                }
            }

            for (ssize_t i = 0; i < splitter.GetSplitterCount(); i++) {
                auto value = splitter.Result()[i];
                auto it = std::lower_bound(dataSorted.begin(), dataSorted.end(), value);
                EXPECT_NE(it, dataSorted.end());
                ssize_t actualPos = std::distance(dataSorted.begin(), it);
                ssize_t expectPos = windowSize / args.PartCount * (i + 1);
                double error = std::abs((double)actualPos - (double)expectPos) / windowSize;
                EXPECT_LE(error, 0.05);
                averageQuantileError[i] += error / tryCount;
            }
        }
        for (const auto& error : averageQuantileError) {
            EXPECT_LE(error, 0.01);
        }
    }
}

struct TValue
{
    ui64 Value;
};

struct TValueCompare
{
    bool Reverted = false;

    bool operator()(const TValue& lhs, const TValue& rhs) const
    {
        return Reverted ? rhs.Value < lhs.Value : lhs.Value < rhs.Value;
    }
};

//! Check TRemedianSplitter for pure structs with external comparator.
TEST(TRemedianSplitter, AbstractValue)
{
    for (bool reverted : {false, true}) {
        TValueCompare compare{reverted};
        ssize_t partCount = 10;
        ssize_t desiredWindowSize = 10000;
        TRemedianSplitter<TValue, TValueCompare> splitter(partCount, desiredWindowSize, compare);
        constexpr ssize_t tryCount = 100;
        ssize_t windowSize = splitter.GetRealWindowSize();
        std::vector<TValue> data(windowSize);
        std::vector<TValue> dataSorted(windowSize);
        std::vector<double> averageQuantileError(splitter.GetSplitterCount());
        for (ssize_t k = 0; k < tryCount; k++) {
            if (k != 0) {
                splitter.Clear();
            }
            GenerateRandomData(data, dataSorted);
            if (reverted) {
                std::ranges::reverse(dataSorted);
            }
            for (const auto& value : data) {
                EXPECT_FALSE(splitter.IsResultReady());
                EXPECT_EQ(splitter.GetResultVersion(), 0);
                splitter.Push(value);
            }
            EXPECT_TRUE(splitter.IsResultReady());
            EXPECT_NE(splitter.GetResultVersion(), 0);

            for (ssize_t i = 0; i < splitter.GetSplitterCount(); i++) {
                auto value = splitter.Result()[i];
                auto it = std::lower_bound(dataSorted.begin(), dataSorted.end(), value, compare);
                EXPECT_NE(it, dataSorted.end());
                ssize_t actualPos = std::distance(dataSorted.begin(), it);
                ssize_t expectPos = windowSize / partCount * (i + 1);
                double error = std::abs((double)actualPos - (double)expectPos) / windowSize;
                EXPECT_LE(error, 0.05);
                averageQuantileError[i] += error / tryCount;
            }
        }
        for (const auto& error : averageQuantileError) {
            EXPECT_LE(error, 0.01);
        }
    }
}

// Check cases when the data is ordered.
TEST(TRemedianSplitter, OrderedCase)
{
    for (bool direct : {true, false}) {
        using T = ssize_t;
        ssize_t partCount = 16;
        ssize_t desiredWindowSize = 1000000;
        TRemedianSplitter<T> splitter(partCount, desiredWindowSize);
        ssize_t windowSize = splitter.GetRealWindowSize();
        for (ssize_t i = 0; i < windowSize; i++) {
            EXPECT_FALSE(splitter.IsResultReady());
            splitter.Push(direct ? i : windowSize - i - 1);
        }
        EXPECT_TRUE(splitter.IsResultReady());

        for (ssize_t i = 0; i < splitter.GetSplitterCount(); i++) {
            ssize_t actualPos = splitter.Result()[i];
            ssize_t expectPos = windowSize / partCount * (i + 1);
            EXPECT_EQ(actualPos, expectPos);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
