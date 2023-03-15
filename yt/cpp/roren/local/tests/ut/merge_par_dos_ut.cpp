#include <yt/cpp/roren/interface/private/par_do_tree.h>

#include <yt/cpp/roren/interface/roren.h>
#include <yt/cpp/roren/local/local.h>
#include <yt/cpp/roren/local/vector_io.h>
#include <yt/cpp/roren/transforms/sum.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <util/string/builder.h>

#include "set_filter.h"

using namespace NRoren;
using namespace NRoren::NPrivate;

////////////////////////////////////////////////////////////////////////////////

template <int Index, typename... Ts>
void ToStaticTagsImpl(
    const std::vector<TDynamicTypeTag>& originalDynamicTags,
    const std::vector<TDynamicTypeTag>& dynamicTags,
    const std::tuple<TTypeTag<Ts>...>& originalTags,
    std::tuple<TTypeTag<Ts>...>& resultTags)
{
    if constexpr (Index < sizeof...(Ts)) {
        const auto& originalTag = std::get<Index>(originalTags);
        Y_ENSURE(originalDynamicTags[Index].GetDescription() == originalTag.GetDescription());
        std::get<Index>(resultTags) = decltype(originalTag){dynamicTags[Index].GetDescription()};
        ToStaticTagsImpl<Index + 1>(originalDynamicTags, dynamicTags, originalTags, resultTags);
    }
}

template <typename... Ts>
std::tuple<TTypeTag<Ts>...> ToStaticTags(
    const IParDoTreePtr& parDoTree,
    const TTypeTag<Ts>&... args)
{
    auto dynamicTags = parDoTree->GetOutputTags();
    auto originalDynamicTags = parDoTree->GetOriginalOutputTags();
    Y_ENSURE(sizeof...(Ts) == dynamicTags.size());

    std::tuple<TTypeTag<Ts>...> originalTags{args...};
    std::tuple<TTypeTag<Ts>...> resultTags{args...};
    ToStaticTagsImpl<0>(originalDynamicTags, dynamicTags, originalTags, resultTags);
    return resultTags;
}

template <typename T>
std::vector<TTypeTag<T>> ToStaticTags(
    const IParDoTreePtr& parDoTree,
    const std::vector<TTypeTag<T>>& originalTags)
{
    auto dynamicTags = parDoTree->GetOutputTags();
    auto originalDynamicTags = parDoTree->GetOriginalOutputTags();
    Y_ENSURE(originalTags.size() == dynamicTags.size());
    Y_ENSURE(originalTags.size() == originalDynamicTags.size());

    std::vector<TTypeTag<T>> resultTags;
    resultTags.reserve(originalTags.size());
    for (int i = 0; i < std::ssize(originalTags); ++i) {
        Y_ENSURE(originalDynamicTags[i].GetDescription() == originalTags[i].GetDescription());
        resultTags.emplace_back(dynamicTags[i].GetDescription());
    }

    return resultTags;
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class TSerializer<std::optional<T>>
{
public:
    static inline void Save(IOutputStream* rh, const std::optional<T>& opt)
    {
        ::Save(rh, opt.has_value());
        if (opt.has_value()) {
            ::Save(rh, *opt);
        }
    }

    static inline void Load(IInputStream* rh, std::optional<T>& opt)
    {
        opt.reset();
        bool has_value;
        ::Load(rh, has_value);
        if (has_value) {
            ::Load(rh, opt.emplace());
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename T, typename ...TArgs>
auto MakeRawWithTags(TArgs&& ...args)
{
    auto parDo = ::MakeIntrusive<T>(std::forward<TArgs>(args)...);
    return std::tuple{MakeRawParDo(parDo), parDo->GetOutputTypedTags()};
}

template <typename T, typename ...TArgs>
static IRawParDoPtr MakeRaw(TArgs&& ...args)
{
    return std::get<IRawParDoPtr>(MakeRawWithTags<T>(std::forward<TArgs>(args)...));
}

template <typename T, typename ...TArgs>
auto MakeParDoWithTags(TArgs&& ...args)
{
    auto parDo = ::MakeIntrusive<T>(std::forward<TArgs>(args)...);
    return std::tuple{ParDo(parDo), parDo->GetOutputTypedTags()};
}

////////////////////////////////////////////////////////////////////////////////

static std::atomic<int> TeeParDoCount = 0;

template <typename T, typename TSecondArg = T, typename TFun = std::plus<T>>
class TTeeParDo
    : public IDoFn<T, TMultiRow>
{
public:
    TTeeParDo() = default;

    TTeeParDo(
        int outputCount,
        std::optional<TSecondArg> secondArg = {})
        : Id_(TeeParDoCount.fetch_add(1))
        , OutputCount_(outputCount)
        , SecondArg_(std::move(secondArg))
    {
        InitTags();
    }

    TDynamicTypeTag GetDynamicInputTag() const
    {
        return TTypeTag<T>{"tee-par-do"};
    }

    std::vector<TDynamicTypeTag> GetOutputTags() const override
    {
        return std::vector<TDynamicTypeTag>(Tags_.begin(), Tags_.end());
    }

    const std::vector<TTypeTag<T>>& GetOutputTypedTags() const
    {
        return Tags_;
    }

    void Start(TOutput<TMultiRow>& /*outputs*/) override
    {
        InitTags();
    }

    void Do(const T& input, TOutput<TMultiRow>& outputs) override
    {
        WriteToAll(input, outputs);
    }

    Y_SAVELOAD_DEFINE_OVERRIDE(Id_, OutputCount_, SecondArg_);

private:
    void InitTags()
    {
        for (int i = 0; i < OutputCount_; ++i) {
            Tags_.emplace_back(::TStringBuilder() << "tee-par-do-" << Id_ << "-" << i);
        }
    }

protected:
    void WriteToAll(const T& input, TOutput<TMultiRow>& outputs)
    {
        auto row = input;
        if (SecondArg_.has_value()) {
            row = TFun()(row, *SecondArg_);
        }
        for (const auto& tag : Tags_) {
            outputs.GetOutput(tag).Add(row);
        }
    }

private:
    int Id_;
    int OutputCount_ = 0;
    std::optional<TSecondArg> SecondArg_;
    std::vector<TTypeTag<T>> Tags_;
};

template <typename T>
class TTeeParDoWithStartFinish
    : public TTeeParDo<T>
{
public:
    using TBase = TTeeParDo<T>;

public:
    TTeeParDoWithStartFinish() = default;

    TTeeParDoWithStartFinish(
        int outputCount,
        std::optional<T> startValue = {},
        std::optional<T> finishValue = {})
        : TBase(outputCount)
        , StartValue_(std::move(startValue))
        , FinishValue_(std::move(finishValue))
    { }

    void Start(TOutput<TMultiRow>& outputs) override
    {
        TBase::Start(outputs);
        if (StartValue_.has_value()) {
            TBase::WriteToAll(*StartValue_, outputs);
        }
    }

    void Finish(TOutput<TMultiRow>& outputs) override
    {
        if (FinishValue_.has_value()) {
            TBase::WriteToAll(*FinishValue_, outputs);
        }
        TBase::Finish(outputs);
    }

    void Save(IOutputStream* s) const override {
        TBase::Save(s);
        ::SaveMany(s, StartValue_, FinishValue_);
    }

    void Load(IInputStream* s) override {
        TBase::Load(s);
        ::LoadMany(s, StartValue_, FinishValue_);
    }

private:
    std::optional<T> StartValue_;
    std::optional<T> FinishValue_;
};

////////////////////////////////////////////////////////////////////////////////

TEST(ParDoTreeBuilder, Simple)
{
    TParDoTreeBuilder builder;
    auto outputIds1 = builder.AddParDo(
        MakeRaw<TTeeParDo<TString>>(1, "-1"),
        builder.RootNodeId);
    ASSERT_EQ(outputIds1.size(), 1u);

    auto filterSet = THashSet<TString>{"baz-1", "x", "foo-1"};
    auto filterParDo = ::MakeIntrusive<TSetFilterParDo<TString>>(filterSet);
    auto outputIds2 = builder.AddParDo(MakeRawParDo(filterParDo), outputIds1[0]);
    ASSERT_EQ(outputIds2.size(), 1u);

    auto outputIds3 = builder.AddParDo(
        MakeRaw<TTeeParDo<TString>>(1, "-2"),
        outputIds2[0]);

    builder.MarkAsOutputs(outputIds3);
    auto rawTreeParDo = builder.Build();

    const std::vector<TString> input = {
        "foo",
        "bar",
        "baz",
        "qux",
    };

    std::vector<TString> output;

    TPipeline pipeline = MakeLocalPipeline();
    pipeline | VectorRead(input)
        | TParDoTransform<TString, TString>(rawTreeParDo, {})
        | VectorWrite(&output);

    pipeline.Run();

    const std::vector<TString> expected = {
        "foo-1-2",
        "baz-1-2",
    };

    ASSERT_EQ(expected, output);
}

TEST(ParDoTreeBuilder, MultipleOutputs)
{
    TParDoTreeBuilder builder;
    auto [tee1, tags1] = MakeRawWithTags<TTeeParDo<int>>(2, 100);
    auto outputIds = builder.AddParDo(tee1, builder.RootNodeId);
    ASSERT_EQ(outputIds.size(), 2u);

    static const auto oddTag = TTypeTag<int>("odd");
    static const auto evenTag = TTypeTag<TString>("even");
    auto splitParDo = TLambda1RawParDo::MakeIntrusive<int>(
        [] (const int& x, TMultiOutput& output) {
            if (x % 2 == 0) {
                output.GetOutput(evenTag).Add(ToString(x));
            } else {
                output.GetOutput(oddTag).Add(x);
            }
        }, {oddTag, evenTag});

    auto outputIds1 = builder.AddParDo(splitParDo, outputIds[0]);
    auto outputIds2 = builder.AddParDo(splitParDo, outputIds[1]);
    ASSERT_EQ(outputIds1.size(), 2u);
    ASSERT_EQ(outputIds2.size(), 2u);

    auto [tee2, tags2] = MakeRawWithTags<TTeeParDo<int>>(1);
    auto outputIds3 = builder.AddParDo(tee2, builder.RootNodeId);
    ASSERT_EQ(outputIds3.size(), 1u);

    builder.MarkAsOutputs(outputIds1);
    builder.MarkAsOutputs(outputIds2);
    builder.MarkAsOutputs(outputIds3);

    auto rawTreeParDo = builder.Build();

    const std::vector<int> input = {1, 2, 5, 3, 8, 67, 66, 85};

    TPipeline pipeline = MakeLocalPipeline();

    auto splitted = pipeline | VectorRead(input)
        | TParDoTransform<int, TMultiRow>(rawTreeParDo, {});

    auto [odd1Tag, even1Tag, odd2Tag, even2Tag, sameTag] = ToStaticTags<int, TString, int, TString, int>(
        rawTreeParDo,
        oddTag,
        evenTag,
        oddTag,
        evenTag,
        tags2[0]
    );

    auto [odd1, even1, odd2, even2, same] = splitted.Unpack(odd1Tag, even1Tag, odd2Tag, even2Tag, sameTag);

    std::vector<int> oddVector1, oddVector2;
    std::vector<TString> evenVector1, evenVector2;
    std::vector<int> sameVector;

    odd1 | VectorWrite(&oddVector1);
    even1 | VectorWrite(&evenVector1);
    odd2 | VectorWrite(&oddVector2);
    even2 | VectorWrite(&evenVector2);
    same | VectorWrite(&sameVector);

    pipeline.Run();

    ASSERT_EQ(
        std::vector<TString>({"102", "108", "166"}),
        evenVector1);
    ASSERT_EQ(
        std::vector<int>({101, 105, 103, 167, 185}),
        oddVector1);
    ASSERT_EQ(
        std::vector<TString>({"102", "108", "166"}),
        evenVector2);
    ASSERT_EQ(
        std::vector<int>({101, 105, 103, 167, 185}),
        oddVector2);
    ASSERT_EQ(
        input,
        sameVector);
}

TEST(ParDoTreeBuilder, CorrectStartAndFinishSequence)
{
    TParDoTreeBuilder builder;
    auto [tee1, tags1] = MakeRawWithTags<TTeeParDoWithStartFinish<TString>>(2, "s1", "f1");
    auto [tee2, tags2] = MakeRawWithTags<TTeeParDoWithStartFinish<TString>>(1, "s2", "f2");
    auto [tee3, tags3] = MakeRawWithTags<TTeeParDoWithStartFinish<TString>>(1, "s3", "f3");
    auto [tee4, tags4] = MakeRawWithTags<TTeeParDoWithStartFinish<TString>>(1, "s4", "f4");
    auto [tee5, tags5] = MakeRawWithTags<TTeeParDoWithStartFinish<TString>>(1, "s5", "f5");
    auto [tee6, tags6] = MakeRawWithTags<TTeeParDoWithStartFinish<TString>>(1, "s6", "f6");
    auto [tee7, tags7] = MakeRawWithTags<TTeeParDoWithStartFinish<TString>>(2, "s7", "f7");
    auto [tee8, tags8] = MakeRawWithTags<TTeeParDoWithStartFinish<TString>>(1, "s8", "f8");

    auto outputIds1 = builder.AddParDo(tee1, builder.RootNodeId);
    ASSERT_EQ(outputIds1.size(), 2u);

    auto outputIds2 = builder.AddParDo(tee2, outputIds1[0]);
    ASSERT_EQ(outputIds2.size(), 1u);

    auto outputIds3 = builder.AddParDo(tee3, builder.RootNodeId);
    ASSERT_EQ(outputIds3.size(), 1u);

    auto outputIds4 = builder.AddParDo(tee4, outputIds1[1]);
    ASSERT_EQ(outputIds4.size(), 1u);

    auto outputIds5 = builder.AddParDo(tee5, builder.RootNodeId);
    ASSERT_EQ(outputIds5.size(), 1u);

    auto outputIds6 = builder.AddParDo(tee6, outputIds3[0]);
    ASSERT_EQ(outputIds6.size(), 1u);

    auto outputIds7 = builder.AddParDo(tee7, outputIds4[0]);
    ASSERT_EQ(outputIds7.size(), 2u);

    auto outputIds8 = builder.AddParDo(tee8, outputIds3[0]);
    ASSERT_EQ(outputIds8.size(), 1u);

    for (const auto& outputIds : std::vector{
        outputIds2,
        outputIds7,
        outputIds6,
        outputIds8,
        outputIds5,
        outputIds3,
    }) {
        builder.MarkAsOutputs(outputIds);
    }

    auto rawTreeParDo = builder.Build();

    const std::vector<TString> input = {
        "foo",
        "bar",
    };

    TPipeline pipeline = MakeLocalPipeline();
    auto collections = pipeline | VectorRead(input)
        | TParDoTransform<TString, TMultiRow>(rawTreeParDo, {});

    auto tags = ToStaticTags(rawTreeParDo, std::vector<TTypeTag<TString>>{
        tags2[0],
        tags7[0],
        tags7[1],
        tags6[0],
        tags8[0],
        tags5[0],
        tags3[0],
    });

    std::vector<std::vector<TString>> outputs(tags.size());
    for (int i = 0; i < std::ssize(tags); ++i) {
        collections.Get(tags[i]) | VectorWrite(&outputs[i]);
    }

    pipeline.Run();

    const std::vector<std::vector<TString>> expected = {
        {"s2", "s1", "foo", "bar", "f1", "f2"},
        {"s7", "s4", "s1", "foo", "bar", "f1", "f4", "f7"},
        {"s7", "s4", "s1", "foo", "bar", "f1", "f4", "f7"},
        {"s6", "s3", "foo", "bar", "f3", "f6"},
        {"s8", "s3", "foo", "bar", "f3", "f8"},
        {"s5", "foo", "bar", "f5"},
        {"s3", "foo", "bar", "f3"},
    };

    ASSERT_EQ(expected.size(), outputs.size());

    for (int i = 0; i < std::ssize(expected); ++i) {
        ASSERT_EQ(expected[i], outputs[i]) << ", iteration " << i;
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(MergeParDos, Simple)
{
    const std::vector<TString> input = {
        "foo",
        "bar",
        "baz",
        "qux",
    };

    std::vector<TString> output;
    auto [tee1, tags1] = MakeParDoWithTags<TTeeParDo<TString>>(1, "-1");
    auto [tee2, tags2] = MakeParDoWithTags<TTeeParDo<TString>>(1, "-2");
    auto filteringParDo = ParDo(
        ::MakeIntrusive<TSetFilterParDo<TString>>(THashSet<TString>{"baz-1", "x", "foo-1"}));

    TPipeline pipeline = MakeLocalPipeline();
    auto root = pipeline | VectorRead(input);
    auto coll1 = root | tee1;
    auto coll2 = coll1.Get(tags1[0])
        | filteringParDo
        | tee2;
    coll2.Get(tags2[0])
        | VectorWrite(&output);

    ASSERT_EQ(GetRawPipeline(pipeline)->GetTransformList().size(), 5u);

    pipeline.Run();

    ASSERT_EQ(GetRawPipeline(pipeline)->GetTransformList().size(), 3u);

    const std::vector<TString> expected = {
        "foo-1-2",
        "baz-1-2",
    };

    ASSERT_EQ(expected, output);
}

namespace {

class TOddEvenSplitFn : public IDoFn<int, TMultiRow>
{
public:
    std::vector<TDynamicTypeTag> GetOutputTags() const override
    {
        return {OddTag, EvenTag};
    }

    void Do(const int& x, TMultiOutput& output) override {
        if (x % 2 == 0) {
            output.GetOutput(EvenTag).Add(ToString(x));
        } else {
            output.GetOutput(OddTag).Add(x);
        }
    };

public:
    static TTypeTag<int> OddTag;
    static TTypeTag<TString> EvenTag;
};

TTypeTag<int> TOddEvenSplitFn::OddTag{"odd"};
TTypeTag<TString> TOddEvenSplitFn::EvenTag{"even"};

} // namespace // anonymous

TEST(MergeParDos, MultipleOutputs)
{
    auto [tee1, tags1] = MakeParDoWithTags<TTeeParDo<int>>(2, 100);
    auto [tee2, tags2] = MakeParDoWithTags<TTeeParDo<int>>(1);

    const std::vector<int> input = {1, 2, 5, 3, 8, 67, 66, 85};

    TPipeline pipeline = MakeLocalPipeline();

    auto root = pipeline | VectorRead(input);
    auto coll1 = root | tee1;
    auto [coll1_left, coll1_right] = coll1.Unpack(tags1[0], tags1[1]);

    auto coll2 = coll1_left | MakeParDo<TOddEvenSplitFn>();
    auto [odd1, even1] = coll2.Unpack(TOddEvenSplitFn::OddTag, TOddEvenSplitFn::EvenTag);

    auto coll3 = coll1_right | MakeParDo<TOddEvenSplitFn>();
    auto [odd2, even2] = coll3.Unpack(TOddEvenSplitFn::OddTag, TOddEvenSplitFn::EvenTag);

    auto coll4 = root | tee2;
    auto same = coll4.Get(tags2[0]);

    std::vector<int> oddVector1, oddVector2;
    std::vector<TString> evenVector1, evenVector2;
    std::vector<int> sameVector;

    odd1 | VectorWrite(&oddVector1);
    even1 | VectorWrite(&evenVector1);
    odd2 | VectorWrite(&oddVector2);
    even2 | VectorWrite(&evenVector2);
    same | VectorWrite(&sameVector);

    ASSERT_EQ(GetRawPipeline(pipeline)->GetTransformList().size(), 10u);

    pipeline.Run();

    ASSERT_EQ(GetRawPipeline(pipeline)->GetTransformList().size(), 7u);

    ASSERT_EQ(
        std::vector<TString>({"102", "108", "166"}),
        evenVector1);
    ASSERT_EQ(
        std::vector<int>({101, 105, 103, 167, 185}),
        oddVector1);
    ASSERT_EQ(
        std::vector<TString>({"102", "108", "166"}),
        evenVector2);
    ASSERT_EQ(
        std::vector<int>({101, 105, 103, 167, 185}),
        oddVector2);
    ASSERT_EQ(
        input,
        sameVector);
}

TEST(MergeParDos, CorrectStartAndFinishSequence)
{
    const std::vector<TString> input = {
        "foo",
        "bar",
    };

    TPipeline pipeline = MakeLocalPipeline();
    auto root = pipeline | VectorRead(input);

    auto [tee1, tags1] = MakeParDoWithTags<TTeeParDoWithStartFinish<TString>>(2, "s1", "f1");
    auto [tee2, tags2] = MakeParDoWithTags<TTeeParDoWithStartFinish<TString>>(1, "s2", "f2");
    auto [tee3, tags3] = MakeParDoWithTags<TTeeParDoWithStartFinish<TString>>(1, "s3", "f3");
    auto [tee4, tags4] = MakeParDoWithTags<TTeeParDoWithStartFinish<TString>>(1, "s4", "f4");
    auto [tee5, tags5] = MakeParDoWithTags<TTeeParDoWithStartFinish<TString>>(1, "s5", "f5");
    auto [tee6, tags6] = MakeParDoWithTags<TTeeParDoWithStartFinish<TString>>(1, "s6", "f6");
    auto [tee7, tags7] = MakeParDoWithTags<TTeeParDoWithStartFinish<TString>>(2, "s7", "f7");
    auto [tee8, tags8] = MakeParDoWithTags<TTeeParDoWithStartFinish<TString>>(1, "s8", "f8");

    auto coll1 = root | tee1;
    auto coll2 = coll1.Get(tags1[0]) | tee2;
    auto coll3 = root | tee3;
    auto coll4 = coll1.Get(tags1[1]) | tee4;
    auto coll5 = root | tee5;
    auto coll6 = coll3.Get(tags3[0]) | tee6;
    auto coll7 = coll4.Get(tags4[0]) | tee7;
    auto coll8 = coll3.Get(tags3[0]) | tee8;

    auto collections = std::vector<TPCollection<TString>>{
        coll2.Get(tags2[0]),
        coll7.Get(tags7[0]),
        coll7.Get(tags7[1]),
        coll6.Get(tags6[0]),
        coll8.Get(tags8[0]),
        coll5.Get(tags5[0]),
        coll3.Get(tags3[0]),
    };

    std::vector<std::vector<TString>> outputs(collections.size());
    for (int i = 0; i < std::ssize(collections); ++i) {
        collections[i] | VectorWrite(&outputs[i]);
    }

    ASSERT_EQ(GetRawPipeline(pipeline)->GetTransformList().size(), 16u);

    pipeline.Run();

    ASSERT_EQ(GetRawPipeline(pipeline)->GetTransformList().size(), 9u);

    const std::vector<std::vector<TString>> expected = {
        {"s2", "s1", "foo", "bar", "f1", "f2"},
        {"s7", "s4", "s1", "foo", "bar", "f1", "f4", "f7"},
        {"s7", "s4", "s1", "foo", "bar", "f1", "f4", "f7"},
        {"s6", "s3", "foo", "bar", "f3", "f6"},
        {"s8", "s3", "foo", "bar", "f3", "f8"},
        {"s5", "foo", "bar", "f5"},
        {"s3", "foo", "bar", "f3"},
    };

    ASSERT_EQ(expected.size(), outputs.size());

    for (int i = 0; i < std::ssize(expected); ++i) {
        ASSERT_EQ(expected[i], outputs[i]) << ", iteration " << i;
    }
}

TEST(MergeParDos, SeveralTrees)
{
    const std::vector<TKV<TString, int>> input = {
        {"foo", 1},
        {"bar", 2},
        {"baz", 3},
        {"foo", 4},
        {"baz", 5},
    };

    using TIntKV = TKV<TString, int>;
    using TVecKV = TKV<TString, std::vector<int>>;

    struct TAddToValue
    {
        TIntKV operator() (const TIntKV& lhs, int addend)
        {
            return TIntKV(lhs.Key(), lhs.Value() + addend);
        }
    };

    struct TPushBack
    {
        TVecKV operator() (TVecKV lhs, int x)
        {
            lhs.Value().push_back(x);
            return lhs;
        }
    };

    auto [tee1, tags1] = MakeParDoWithTags<TTeeParDo<TIntKV, int, TAddToValue>>(2, 10);
    auto [tee2, tags2] = MakeParDoWithTags<TTeeParDo<TIntKV, int, TAddToValue>>(1, 100);

    auto [tee3, tags3] = MakeParDoWithTags<TTeeParDo<TVecKV, int, TPushBack>>(1, 42);
    auto [tee4, tags4] = MakeParDoWithTags<TTeeParDo<TVecKV, int, TPushBack>>(1, 43);

    auto collector = ParDo([] (const TKV<TString, TInputPtr<int>>& kv) {
        TKV<TString, std::vector<int>> result;
        result.Key() = kv.Key();

        for (const auto& v : kv.Value()) {
            result.Value().push_back(v);
        }
        std::sort(result.Value().begin(), result.Value().end());
        return result;
    });

    std::vector<TVecKV> output1, output2;

    TPipeline pipeline = MakeLocalPipeline();
    auto root = pipeline | VectorRead(input);
    auto coll1 = root | tee1;
    auto grouped1 = coll1.Get(tags1[0]) | GroupByKey() | collector;
    auto coll2 = coll1.Get(tags1[1]) | tee2;
    auto coll3 = grouped1 | tee3;
    auto grouped2 = coll2.Get(tags2[0]) | GroupByKey() | collector;
    auto coll4 = grouped2 | tee4;

    coll3.Get(tags3[0]) | VectorWrite(&output1);
    coll4.Get(tags4[0]) | VectorWrite(&output2);

    ASSERT_EQ(GetRawPipeline(pipeline)->GetTransformList().size(), 11u);

    pipeline.Run();

    ASSERT_EQ(GetRawPipeline(pipeline)->GetTransformList().size(), 8u);

    auto byKey = [] (const auto& lhs, const auto& rhs) {
        return lhs.Key() < rhs.Key();
    };

    std::sort(output1.begin(), output1.end(), byKey);
    std::sort(output2.begin(), output2.end(), byKey);

    const std::vector<TVecKV> expected1 = {
        {"bar", {12, 42}},
        {"baz", {13, 15, 42}},
        {"foo", {11, 14, 42}},
    };

    const std::vector<TVecKV> expected2 = {
        {"bar", {112, 43}},
        {"baz", {113, 115, 43}},
        {"foo", {111, 114, 43}},
    };

    ASSERT_EQ(expected1, output1);
    ASSERT_EQ(expected2, output2);
}

TEST(MergeParDos, Clone)
{
    auto rawPlusOne = TLambda1RawParDo::MakeIntrusive<int, int>([] (const int& x) {
        return x + 1;
    });

    TParDoTreeBuilder builder;

    auto outputIds1 = builder.AddParDo(rawPlusOne, builder.RootNodeId);
    ASSERT_EQ(outputIds1.size(), 1u);

    auto outputIds2 = builder.AddParDo(rawPlusOne, outputIds1[0]);
    ASSERT_EQ(outputIds2.size(), 1u);

    auto outputIds3 = builder.AddParDo(rawPlusOne, outputIds2[0]);
    ASSERT_EQ(outputIds2.size(), 1u);

    builder.MarkAsOutputs(outputIds3);
    auto rawTreeParDo = builder.Build();

    const std::vector<int> input = {0, 1, 2, 3, 4};

    std::vector<int> output;
    TPipeline pipeline = MakeLocalPipeline();
    pipeline | VectorRead(input)
        | TParDoTransform<int, int>(rawTreeParDo->Clone(), {})
        | VectorWrite(&output);

    pipeline.Run();

    const std::vector<int> expected = {3, 4, 5, 6, 7};

    ASSERT_EQ(expected, output);
}
