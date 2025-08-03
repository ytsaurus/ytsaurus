#pragma once

#include "fwd.h"
#include "attributes.h"
#include "serializable.h"
#include "raw_data_flow.h"
#include <yt/cpp/roren/interface/timers.h>

#include <any>
#include <optional>

namespace NRoren {
class TDynamicTypeTag;
}

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

enum class ERawTransformType
{
    Read,
    Write,
    ParDo,
    StatefulParDo,
    StatefulTimerParDo,
    GroupByKey,
    CombinePerKey,
    CombineGlobally,
    CoGroupByKey,
    Flatten,
};

////////////////////////////////////////////////////////////////////////////////

class IRawTransform
    : public virtual NYT::TRefCounted
    , public TAttributes
{
public:
    [[nodiscard]] virtual ERawTransformType GetType() const = 0;

    [[nodiscard]] virtual std::vector<TDynamicTypeTag> GetInputTags() const = 0;
    [[nodiscard]] virtual std::vector<TDynamicTypeTag> GetOutputTags() const = 0;

    IRawReadPtr AsRawRead();
    IRawWritePtr AsRawWrite();
    IRawParDoPtr AsRawParDo();
    IRawStatefulParDoPtr AsRawStatefulParDo();
    IRawStatefulTimerParDoPtr AsRawStatefulTimerParDo();
    IRawGroupByKeyPtr AsRawGroupByKey();
    IRawCombinePtr AsRawCombine();
    IRawCoGroupByKeyPtr AsRawCoGroupByKey();
    IRawFlattenPtr AsRawFlatten();

    [[nodiscard]] const IRawRead& AsRawReadRef() const;
    [[nodiscard]] const IRawWrite& AsRawWriteRef() const;
    [[nodiscard]] const IRawParDo& AsRawParDoRef() const;
    [[nodiscard]] const IRawStatefulParDo& AsRawStatefulParDoRef() const;
    [[nodiscard]] const IRawStatefulTimerParDo& AsRawStatefulTimerParDoRef() const;
    [[nodiscard]] const IRawGroupByKey& AsRawGroupByKeyRef() const;
    [[nodiscard]] const IRawCombine& AsRawCombineRef() const;
    [[nodiscard]] const IRawCoGroupByKey& AsRawCoGroupByKeyRef() const;
    [[nodiscard]] const IRawFlatten& AsRawFlattenRef() const;
};

DEFINE_REFCOUNTED_TYPE(IRawTransform);

////////////////////////////////////////////////////////////////////////////////

struct IRawRead
    : public IRawTransform
    , public IRawInput
    , public ISerializable<IRawRead>
{
    [[nodiscard]] ERawTransformType GetType() const final
    {
        return ERawTransformType::Read;
    }
};

DEFINE_REFCOUNTED_TYPE(IRawRead);

class TRawDummyRead
    : public NPrivate::IRawRead
{
public:
    TRawDummyRead() = default;
    explicit TRawDummyRead(TRowVtable vtable);

    const void* NextRaw() override;

    TDefaultFactoryFunc GetDefaultFactory() const override;
    void Save(IOutputStream*) const override;
    void Load(IInputStream*) override;
    std::vector<TDynamicTypeTag> GetInputTags() const override;
    std::vector<TDynamicTypeTag> GetOutputTags() const override;

private:
    TRowVtable Vtable_;
};

////////////////////////////////////////////////////////////////////////////////

struct IRawWrite
    : public IRawTransform
    , public IRawOutput
    , public ISerializable<IRawWrite>
{
    [[nodiscard]] ERawTransformType GetType() const final
    {
        return ERawTransformType::Write;
    }
};

DEFINE_REFCOUNTED_TYPE(IRawWrite);

class TRawDummyWriter
    : public IRawWrite
{
public:
    TRawDummyWriter() = default;
    explicit TRawDummyWriter(TRowVtable vtable);

    void AddRaw(const void*, ssize_t count) override;
    void Close() override;

    TDefaultFactoryFunc GetDefaultFactory() const override;
    void Save(IOutputStream*) const override;
    void Load(IInputStream*) override;
    std::vector<TDynamicTypeTag> GetInputTags() const override;
    std::vector<TDynamicTypeTag> GetOutputTags() const override;

private:
    TRowVtable Vtable_;
};

////////////////////////////////////////////////////////////////////////////////

class IRawParDo
    : public IRawTransform
    , public ISerializable<IRawParDo>
{
public:
    [[nodiscard]] ERawTransformType GetType() const final
    {
        return ERawTransformType::ParDo;
    }

    // It's assumed that par do saves outputs received in Start
    virtual void Start(const IExecutionContextPtr& context, const std::vector<IRawOutputPtr>& outputs) = 0;

    virtual void Do(const void* rows, int count) = 0;
    virtual void MoveDo(void* rows, int count)
    {
        Do(rows, count);
    }

    virtual void Finish() = 0;
    virtual const TFnAttributes& GetFnAttributes() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IRawParDo);

////////////////////////////////////////////////////////////////////////////////

class IRawStatefulParDo
    : public IRawTransform
    , public ISerializable<IRawStatefulParDo>
{
public:
    [[nodiscard]] ERawTransformType GetType() const final
    {
        return ERawTransformType::StatefulParDo;
    }

    virtual TRowVtable GetStateVtable() const = 0;

    virtual const TFnAttributes& GetFnAttributes() const = 0;

    virtual void Start(const IExecutionContextPtr& context, IRawStateStorePtr rawStateMap, const std::vector<IRawOutputPtr>& outputs) = 0;
    virtual void Do(const void* rows, int count) = 0;
    virtual void MoveDo(void* rows, int count)
    {
        Do(rows, count);
    }
    virtual void Finish() = 0;
};

DEFINE_REFCOUNTED_TYPE(IRawStatefulParDo);

////////////////////////////////////////////////////////////////////////////////

class IRawStatefulTimerParDo
    : public IRawTransform
    , public ISerializable<IRawStatefulTimerParDo>
{
public:
    [[nodiscard]] ERawTransformType GetType() const final
    {
        return ERawTransformType::StatefulTimerParDo;
    }

    virtual TRowVtable GetStateVtable() const = 0;

    virtual const TFnAttributes& GetFnAttributes() const = 0;

    virtual void Start(const IExecutionContextPtr& context, IRawStateStorePtr rawStateMap, const std::vector<IRawOutputPtr>& outputs) = 0;
    virtual void Do(const void* rows, int count) = 0;
    virtual void MoveDo(void* rows, int count)
    {
        Do(rows, count);
    }
    virtual void OnTimer(const void* rawKey, const TTimer& timer) = 0;
    virtual void Finish() = 0;

    virtual const TString& GetFnId() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IRawStatefulTimerParDo);

////////////////////////////////////////////////////////////////////////////////

class IRawGroupByKey
    : public IRawTransform
    , public ISerializable<IRawGroupByKey>
{
public:
    [[nodiscard]] ERawTransformType GetType() const final
    {
        return ERawTransformType::GroupByKey;
    }

    // Input iterates over values with same key.
    virtual void ProcessOneGroup(const IRawInputPtr& input, const IRawOutputPtr& output) = 0;
};

DEFINE_REFCOUNTED_TYPE(IRawGroupByKey);

////////////////////////////////////////////////////////////////////////////////

class IRawCombine
    : public IRawTransform
    , public ISerializable<IRawCombine>
{
public:
    explicit IRawCombine(ERawTransformType type)
        : Type_(type)
    {
        Y_ABORT_UNLESS(type == ERawTransformType::CombinePerKey || type == ERawTransformType::CombineGlobally);
    }

    [[nodiscard]] ERawTransformType GetType() const final
    {
        return Type_;
    }

    virtual void Start(const IExecutionContextPtr& context) = 0;

    virtual void CreateAccumulator(void* accum) = 0;
    virtual void AddInput(void* accum, const void* input) = 0;
    virtual void MergeAccumulators(void* accumResult, const IRawInputPtr& accums) = 0;
    virtual void ExtractOutput(void* output, const void* accum) = 0;

    virtual TRowVtable GetInputVtable() const = 0;
    virtual TRowVtable GetAccumVtable() const = 0;
    virtual TRowVtable GetOutputVtable() const = 0;
    virtual IRawCoderPtr GetAccumCoder() const = 0;

private:
    const ERawTransformType Type_;
};

DEFINE_REFCOUNTED_TYPE(IRawCombine);

////////////////////////////////////////////////////////////////////////////////

class IRawCoGroupByKey
    : public IRawTransform
    , public ISerializable<IRawCoGroupByKey>
{
public:
    [[nodiscard]] ERawTransformType GetType() const final
    {
        return ERawTransformType::CoGroupByKey;
    }
};

DEFINE_REFCOUNTED_TYPE(IRawCoGroupByKey);

class IRawFlatten
    : public IRawTransform
    , public ISerializable<IRawFlatten>
{
    [[nodiscard]] ERawTransformType GetType() const final
    {
        return ERawTransformType::Flatten;
    }
};

DEFINE_REFCOUNTED_TYPE(IRawFlatten);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
