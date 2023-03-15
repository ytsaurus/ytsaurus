#include <yt/cpp/roren/interface/roren.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

template <typename R>
class IRangeInput
{
public:
    virtual ~IRangeInput() = default;

    /// Get next range of elements. Return empty range when input is exhausted.
    virtual TConstArrayRef<R> Next() = 0;
};

template <typename K, typename V>
class TPState {};

template <typename T>
T LoadBigRtConfig(int argc, const char** argv);

class TRorenConfig;

TPipeline MakeBigRtPipeline(const TRorenConfig&);

template <typename V, typename S>
    requires NTraits::IsTKV<V>
class TStateful
{
public:
    const S& GetState() const;
    S* MutableState() const;

    // Возвращает true, если хотя бы раз позвали MutableState().
    bool StateIsChanged() const;

    const V& GetValue() const;
};

template <NTraits::CTKV TInputRow_, typename TOutputRow_, typename TState_>
class IStatefulDoFn__Range1
    : public TThrRefBase
{
public:
    using TInputRow = TInputRow_;
    using TOutputRow = TOutputRow_;
    using TState = TState_;

public:
    virtual void Do(IRangeInput<TStateful<TInputRow, TState>>& input, TOutput<TOutputRow>& output) = 0;

    virtual void Save(IOutputStream*) const
    { }

    virtual void Load(IInputStream*)
    { }
};

template <NTraits::CTKV TInputRow_, typename TOutputRow_, typename TState_>
class IStatefulDoFn__Range2
    : public TThrRefBase
{
public:
    using TInputRow = TInputRow_;
    using TOutputRow = TOutputRow_;
    using TState = TState_;

public:
    virtual void Start(TOutput<TOutputRow>& output) = 0;
    virtual void Do(const TConstArrayRef<TStateful<TInputRow, TState>>& input, TOutput<TOutputRow>& output) = 0;
    virtual void Finish(TOutput<TOutputRow>& output) = 0;

    virtual void Save(IOutputStream*) const
    { }

    virtual void Load(IInputStream*)
    { }
};

////////////////////////////////////////////////////////////////////////////////

}

class TMyConfig
{
public:
    const NRoren::TRorenConfig& GetRorenConfig() const;
};
