#pragma once

///
/// @file local.h This library provides local pipeline that runs on local machine.
/// It is designed to be used in unit tests so it doesn't optimzed for performance
/// and makes some additional checks.
///
/// Example of usage:
/// ```
/// std::vector<int> result;
/// auto pipeline = MakeLocalPipeline();
/// pipeline
///   | VectorRead<int>({1, 2, 3})
///   | ...
///   | VectorWrite(&result);
/// pipeline.Run();
///  ... // check results.
/// ```

#include <yt/cpp/roren/interface/executor.h>
#include <yt/cpp/roren/interface/private/serializable.h>
#include <yt/cpp/roren/interface/transforms.h>

#include <yt/yt/library/profiling/public.h>
#include <yt/yt/library/profiling/impl.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Create local pipeline convenient for writing unit tests.
///
/// This pipeline is convenient to write unit tests.
TPipeline MakeLocalPipeline(NYT::NProfiling::TProfiler profiler = NYT::NProfiling::TProfiler());

///
/// @brief Transform reading data from given vector.
///
/// @note This transform works only with local pipeline.
template <typename T>
TReadTransform<T> VectorRead(std::vector<T> input);

///
/// @brief Transform writing data to given vector.
///
/// @note This transform works only with local pipeline.
///
/// @note User must ensure that `destination` is live until `TPipeline::Run` is called.
template <typename T>
TWriteTransform<T> VectorWrite(std::vector<T>* destination);

////////////////////////////////////////////////////////////////////////////////

// || |\    /| |==\\ ||     |=== |\    /| ||=== |\ || ====   /\   ====  ||  //\\  |\ ||
// || ||\  /|| |==// ||    ||=== ||\  /|| ||=== ||\||  ||   /__\   ||   || ||  || ||\||
// || || \/ || ||    ||===  |=== || \/ || ||=== || \|  ||  //  \\  ||   ||  \\//  || \|

////////////////////////////////////////////////////////////////////////////////

namespace NPrivate {

////////////////////////////////////////////////////////////////////////////////

void SavePtr(IOutputStream* output, void*);
void LoadPtrImpl(IInputStream* input, void**);

template <typename T>
void LoadPtr(IInputStream* input, T** ptr)
{
    void* tmp;
    LoadPtrImpl(input, &tmp);
    *ptr = static_cast<T*>(tmp);
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class TVectorDataInput
    : public NPrivate::IRawRead
{
public:
    explicit TVectorDataInput(std::vector<T> input)
        : Input_(std::make_shared<std::vector<T>>(std::move(input)))
        , Tag_(TTypeTag<T>("vector-input"))
    { }

private:
    std::vector<TDynamicTypeTag> GetInputTags() const override
    {
        return {};
    }

    std::vector<TDynamicTypeTag> GetOutputTags() const override
    {
        return {Tag_};
    }

    const void* NextRaw() override
    {
        if (Index_ == std::ssize(*Input_)) {
            return nullptr;
        } else {
            return &(*Input_)[Index_++];
        }
    }

    TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IRawReadPtr {
            auto result = ::MakeIntrusive<TVectorDataInput<T>>(std::vector<T>{});
            return result.Get();
        };
    }

    void SaveState(IOutputStream& stream) const override
    {
        Save(&stream, *Input_);
        Save(&stream, Index_);
    }

    void LoadState(IInputStream& stream) override
    {
        Load(&stream, *Input_);
        Load(&stream, Index_);
    }

private:
    std::shared_ptr<std::vector<T>> Input_;
    TDynamicTypeTag Tag_;

    i64 Index_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class TVectorDataOutput
    : public NPrivate::IRawWrite

{
public:
    explicit TVectorDataOutput(std::vector<T>* output)
        : Output_(output)
    { }

private:
    std::vector<TDynamicTypeTag> GetInputTags() const override
    {
        return {TDynamicTypeTag{}};
    }

    std::vector<TDynamicTypeTag> GetOutputTags() const override
    {
        return {};
    }

    void AddRaw(const void* rows, ssize_t count) override
    {
        Output_->insert(
            Output_->end(),
            static_cast<const T*>(rows),
            static_cast<const T*>(rows) + count);
    }

    void Close() override
    { }

    TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IRawWritePtr {
            return ::MakeIntrusive<TVectorDataOutput<T>>(nullptr);
        };
    }

    void SaveState(IOutputStream& stream) const override
    {
        SavePtr(&stream, Output_);
    }

    void LoadState(IInputStream& stream) override
    {
        void* ptr;
        LoadPtr(&stream, &ptr);
        Output_ = static_cast<std::vector<T>*>(ptr);
    }

private:
    std::vector<T>* Output_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NPrivate

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TReadTransform<T> VectorRead(std::vector<T> input)
{
    return TReadTransform<T>{
        MakeIntrusive<NPrivate::TVectorDataInput<T>>(std::move(input))
    };
}

template <typename T>
TWriteTransform<T> VectorWrite(std::vector<T>* destination)
{
    return TWriteTransform<T>{
        ::MakeIntrusive<NPrivate::TVectorDataOutput<T>>(destination)
    };
}

template <class TKey, class TState>
class TLocalPState: public NPrivate::ISerializable<TLocalPState<TKey, TState>>
{
public:
    using typename NPrivate::ISerializable<TLocalPState<TKey, TState>>::TDefaultFactoryFunc;

    TLocalPState() = default;
    TLocalPState(THashMap<TKey, TState>& storage): Storage_(&storage) {}
    TState& Get(const TKey& key) { return (*Storage_)[key]; }

    TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return []() { return MakeIntrusive<TLocalPState>(); };
    }

    void SaveState(IOutputStream& stream) const override
    {
        stream << reinterpret_cast<const i64&>(Storage_);  // Useful only for local execution
    }

    void LoadState(IInputStream& stream) override
    {
        stream >> reinterpret_cast<i64&>(Storage_);  // Useful only for local execution
    }

protected:
    THashMap<TKey, TState>* Storage_ = nullptr;
};

template <typename TKey, typename TState>
TLocalPState<TKey, TState> MakeLocalPState(THashMap<TKey, TState>& storage)
{
    return TLocalPState(storage);
}

template <class TStatefulParDoClass>
class TLocalStatefulPardoWrapper
    : public IDoFn<typename TStatefulParDoClass::TInputRow, typename TStatefulParDoClass::TOutputRow>
{
public:
    using TInputRow = typename TStatefulParDoClass::TInputRow;
    using TOutputRow = typename TStatefulParDoClass::TOutputRow;
    using TKey = typename TStatefulParDoClass::TInputRow::TKey;
    using TState = typename TStatefulParDoClass::TState;

    TLocalStatefulPardoWrapper():
        TLocalStatefulPardoWrapper(TLocalPState<TKey,TState>())
    {
    }

    template <typename... Args>
    TLocalStatefulPardoWrapper(TLocalPState<TKey, TState> pState, Args... args)
        : StatefulParDo_(::MakeIntrusive<TStatefulParDoClass>(args...))
        , PState_(std::move(pState))
    {
    }

    void Do(const TInputRow& input, TOutput<TOutputRow>& output) override
    {
        StatefulParDo_->Do(input, output, PState_.Get(input.Key()));
    }

    void Save(IOutputStream* stream) const override
    {
        StatefulParDo_->Save(stream);
        PState_.SaveState(*stream);
    }

    void Load(IInputStream* stream) override
    {
        StatefulParDo_->Load(stream);
        PState_.LoadState(*stream);
    }

protected:
    TIntrusivePtr<TStatefulParDoClass> StatefulParDo_;
    TLocalPState<TKey, TState> PState_;
};

template <typename T, typename... Args>
auto MakeStatefulParDo(TLocalPState<typename T::TInputRow::TKey, typename T::TState> pState, Args... args)
{
    return MakeParDo<TLocalStatefulPardoWrapper<T>>(std::move(pState), args...);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
