#include "jobs.h"

#include <yt/cpp/roren/interface/roren.h>

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/format.h>
#include <yt/cpp/mapreduce/interface/logging/yt_log.h>
#include <yt/cpp/mapreduce/io/node_table_reader.h>
#include <yt/cpp/mapreduce/io/stream_table_reader.h>
#include <yt/cpp/roren/interface/private/par_do_tree.h>

#include <library/cpp/yson/writer.h>

#include <util/string/printf.h>
#include <util/system/backtrace.h>
#include <util/string/escape.h>

namespace NRoren::NPrivate {

using namespace NYT;

////////////////////////////////////////////////////////////////////////////////

class TYtExecutionContext
    : public IExecutionContext
{
public:
    TString GetExecutorName() const final override
    {
        return "yt";
    }

    NYT::NProfiling::TProfiler GetProfiler() const override
    {
        // TODO: profiler should write to statistics after job finish?
        return {};
    }

    void SetTimer(const TTimer& timer, const TTimer::EMergePolicy policy) override
    {
        Y_UNUSED(timer);
        Y_UNUSED(policy);
        Y_FAIL("not implemented");
    }

    void DeleteTimer(const TTimer::TKey& key) override
    {
        Y_UNUSED(key);
        Y_FAIL("not implemented");
    }
};

IExecutionContextPtr CreateYtExecutionContext()
{
    return ::MakeIntrusive<TYtExecutionContext>();
}

////////////////////////////////////////////////////////////////////////////////

NYT::TTableRangesReaderPtr<TNode> CreateRangesTableReader(IInputStream* stream)
{
    auto impl = ::MakeIntrusive<TNodeTableReader>(::MakeIntrusive<NYT::NDetail::TInputStreamProxy>(stream));
    return ::MakeIntrusive<TTableRangesReader<TNode>>(impl);
}

////////////////////////////////////////////////////////////////////////////////

class TImpulseRawJob
     : public NYT::IRawJob
{
public:
    TImpulseRawJob() = default;

    explicit TImpulseRawJob(IRawParDoPtr parDo)
        : ParDo_(std::move(parDo))
    { }

    void Do(const TRawJobContext& /*jobContext*/) override
    {
        signal(SIGSEGV, [] (int) {PrintBackTrace(); Y_FAIL("FAIL");});

        try {
            auto executionContext = ::MakeIntrusive<TYtExecutionContext>();
            Y_VERIFY(ParDo_->GetOutputTags().empty());
            ParDo_->Start(executionContext, {});
            int impulse = 0;
            ParDo_->Do(&impulse, 1);
            ParDo_->Finish();
        } catch (const std::exception& ex) {
            Cerr << "Error in TImpulseRawJob" << Endl;
            Cerr << ex.what() << Endl;
            Cerr << TBackTrace::FromCurrentException().PrintToString() << Endl;
            throw;
        } catch (...) {
            Cerr << "Unknown error in TImpulseRawJob" << Endl;
            Cerr << TBackTrace::FromCurrentException().PrintToString() << Endl;
            throw;
        }
    }

private:
    IRawParDoPtr ParDo_;

public:
    Y_SAVELOAD_JOB(ParDo_);
};
REGISTER_RAW_JOB(TImpulseRawJob);

NYT::IRawJobPtr CreateImpulseJob(const IRawParDoPtr& rawParDo)
{
    return ::MakeIntrusive<TImpulseRawJob>(std::move(rawParDo));
}

////////////////////////////////////////////////////////////////////////////////

class TParDoMap
    : public NYT::IRawJob
{
private:
    static constexpr TStringBuf ComputationKey_ = "computation";
    static constexpr TStringBuf ReaderKey_ = "reader";
    static constexpr TStringBuf WriterKey_ = "writer";
    static constexpr TStringBuf ExecutionContextKey_ = "execution-context";

public:
    TParDoMap() = default;

    TParDoMap(
        const IRawParDoPtr& rawParDo,
        const IYtJobInputPtr& input,
        const std::vector<IYtJobOutputPtr>& outputs)
    {
        State_[ComputationKey_] = SerializableToNode(*rawParDo);
        State_[ReaderKey_] = SerializableToNode(*input);
        State_[WriterKey_] = TNode::CreateList();
        for (const auto& o : outputs) {
            State_[WriterKey_].Add(SerializableToNode(*o));
        }
    }

    void Do(const TRawJobContext& /*jobContext*/) override
    {
        try {
            auto computation = SerializableFromNode<IRawParDo>(State_[ComputationKey_]);
            auto reader = SerializableFromNode<IYtJobInput>(State_[ReaderKey_]);
            std::vector<IRawOutputPtr> outputs;
            for (const auto& writer : State_[WriterKey_].AsList()) {
                auto output = SerializableFromNode<IYtJobOutput>(writer);
                outputs.emplace_back(std::move(output));
            }

            auto executionContext = ::MakeIntrusive<TYtExecutionContext>();

            computation->Start(executionContext, outputs);

            while (const auto* row = reader->NextRaw()) {
                computation->Do(row, 1);
            }

            computation->Finish();

            for (const auto& output : outputs) {
                output->Close();
            }
        } catch (const std::exception& ex) {
            Cerr << "Error in ParDo" << Endl;
            Cerr << ex.what() << Endl;
            Cerr << TBackTrace::FromCurrentException().PrintToString() << Endl;
            throw;
        } catch (...) {
            Cerr << "Unknown error in ParDo" << Endl;
            Cerr << TBackTrace::FromCurrentException().PrintToString() << Endl;
            throw;
        }
    }

private:
    TNode State_;

public:
    Y_SAVELOAD_JOB(State_);
};
REGISTER_RAW_JOB(TParDoMap);

////////////////////////////////////////////////////////////////////////////////

// Takes as input rows with TKV in "value" column.
// Puts key part into "key" column and value part into "value" column.
class TSplitKvMap
    : public NYT::IRawJob
{
public:
    TSplitKvMap() = default;

    explicit TSplitKvMap(const std::vector<TRowVtable>& rowVtables)
        : RowVtables_(rowVtables)
    { }

    void Do(const TRawJobContext& /*jobContext*/) override
    {
        try {
            auto reader = CreateDecodingJobInput(RowVtables_);
            auto writer = CreateKvJobOutput(/*sinkIndex*/ 0, RowVtables_);

            while (const auto* row = reader->NextRaw()) {
                writer->AddRawToTable(row, 1, reader->GetInputIndex());
            }
            writer->Close();
        } catch (...) {
            Cerr << "Error in TSplitKvMap" << Endl;
            Cerr << TBackTrace::FromCurrentException().PrintToString() << Endl;
            throw;
        }
    }

private:
    std::vector<TRowVtable> RowVtables_;

public:
    Y_SAVELOAD_JOB(RowVtables_);
};
REGISTER_RAW_JOB(TSplitKvMap);

////////////////////////////////////////////////////////////////////////////////

// Takes as input rows with TKV in "value" column.
// Puts key part into "key" column and value part into "value" column.
class TSplitStateKvMap
    : public NYT::IRawJob
{
public:
    TSplitStateKvMap() = default;

    explicit TSplitStateKvMap(const std::vector<TRowVtable>& rowVtables, TYtStateVtable stateVtable)
        : RowVtables_(rowVtables), StateVtable_(std::move(stateVtable))
    { }

    void Do(const TRawJobContext& /*jobContext*/) override
    {
        std::vector<IRawCoderPtr> decoders;
        std::vector<TRawRowHolder> rowHolders;
        TRawRowHolder stateHolder(StateVtable_.StateTKVvtable);
        try {
            auto reader = NYT::CreateTableReader<TNode>(&Cin);
            decoders.reserve(RowVtables_.size());
            rowHolders.reserve(RowVtables_.size());

            for (const auto& rowVtable : RowVtables_) {
                decoders.emplace_back(rowVtable.RawCoderFactory());
                rowHolders.emplace_back(rowVtable);
            }

            auto writer = CreateKvJobOutput(/*sinkIndex*/ 0, RowVtables_);

            for (;reader->IsValid(); reader->Next()) {
                auto node = reader->GetRow();
                size_t inputIndex = reader->GetTableIndex();
                auto& rowHolder = rowHolders[inputIndex];
                if (inputIndex == 0) {
                    // convert TNode to TStateTKV
                    StateVtable_.LoadState(rowHolder, node);
                } else {
                    // convert TNode['value'] to KV
                    decoders[inputIndex]->DecodeRow(node["value"].AsString(), rowHolder.GetData());
                }
                writer->AddRawToTable(rowHolder.GetData(), 1, inputIndex);
            }
            writer->Close();
        } catch (...) {
            Cerr << "Error in TSplitStateKvMap" << Endl;
            Cerr << TBackTrace::FromCurrentException().PrintToString() << Endl;
            throw;
        }
    }

private:
    std::vector<TRowVtable> RowVtables_;
    TYtStateVtable StateVtable_;

public:
    Y_SAVELOAD_JOB(RowVtables_, StateVtable_);
};
REGISTER_RAW_JOB(TSplitStateKvMap);

////////////////////////////////////////////////////////////////////////////////

class TGbkImpulseReadParDo
    : public IRawParDo
{
public:
    TGbkImpulseReadParDo(IRawGroupByKeyPtr rawGroupByKey)
        : RawGroupByKey_(rawGroupByKey)
    { }

    void Start(const IExecutionContextPtr& context, const std::vector<IRawOutputPtr>& outputs) override
    {
        Y_VERIFY(context->GetExecutorName() == "yt");

        Y_VERIFY(outputs.size() == 1);
        Processed_ = false;
        Output_ = outputs[0];
    }

    void Do(const void* rows, int count) override
    {
        Y_VERIFY(count == 1);
        Y_VERIFY(*static_cast<const int*>(rows) == 0);
        Y_VERIFY(!Processed_);
        Processed_ = true;

        const auto gbkInputTags = RawGroupByKey_->GetInputTags();
        Y_VERIFY(gbkInputTags.size() == 1);
        const auto& rowVtable = gbkInputTags[0].GetRowVtable();

        auto rangesReader = CreateRangesTableReader(&Cin);

        for (; rangesReader->IsValid(); rangesReader->Next()) {
            auto range = &rangesReader->GetRange();
            auto input = CreateSplitKvJobInput(std::vector{rowVtable}, range);
            RawGroupByKey_->ProcessOneGroup(input, Output_);
        }
    }

    void Finish() override
    { }

    const TFnAttributes& GetFnAttributes() const override
    {
        static TFnAttributes attributes;
        return attributes;
    }

    TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IRawParDoPtr {
            return ::MakeIntrusive<TGbkImpulseReadParDo>(nullptr);
        };
    }

    std::vector<TDynamicTypeTag> GetInputTags() const override
    {
        return {
            {"input", MakeRowVtable<int>()}
        };
    }

    std::vector<TDynamicTypeTag> GetOutputTags() const override
    {
        return RawGroupByKey_->GetOutputTags();
    }

private:
    IRawGroupByKeyPtr RawGroupByKey_;

    Y_SAVELOAD_DEFINE_OVERRIDE(RawGroupByKey_);

    IRawOutputPtr Output_;
    bool Processed_ = false;
};

void ProcessOneGroup(const IRawCoGroupByKeyPtr& rawComputation, const IYtNotSerializableJobInputPtr& input, const IRawOutputPtr& rawOutput)
{
    auto upcastedOutput = rawOutput->Upcast<TCoGbkResult>();
    auto inputTags = rawComputation->GetInputTags();

    TRowVtable keyVtable;
    TRawRowHolder keyHolder;

    std::vector<std::vector<TRawRowHolder>> values;
    values.resize(inputTags.size());

    while (const void* raw = input->NextRaw()) {
        auto inputIndex = input->GetInputIndex();
        Y_ENSURE(inputIndex < inputTags.size(), "Input index must be less than input tags count");
        auto inputVtable = inputTags[inputIndex].GetRowVtable();

        if (!IsDefined(keyVtable)) {
            keyVtable = inputVtable.KeyVtableFactory();
            keyHolder = TRawRowHolder{keyVtable};
            keyHolder.CopyFrom(reinterpret_cast<const char*>(raw) + inputVtable.KeyOffset);
        }

        values[inputIndex].emplace_back(inputVtable);
        values[inputIndex].back().CopyFrom(raw);
    }

    Y_VERIFY(IsDefined(keyVtable));

    std::vector<std::pair<TDynamicTypeTag, IRawInputPtr>> taggedInputs;
    taggedInputs.reserve(inputTags.size());

    for (ssize_t inputIndex = 0; inputIndex < ssize(values); ++inputIndex) {
        const auto& tag = inputTags[inputIndex];
        auto rawVectorInput = MakeRawVectorInput<TRawRowHolder>(values[inputIndex]);
        taggedInputs.emplace_back(tag, rawVectorInput);
    }

    upcastedOutput->Add(MakeCoGbkResult(keyHolder, std::move(taggedInputs)));
}

class TMultiJoinKvReduce
    : public NYT::IRawJob
{
public:
    TMultiJoinKvReduce() = default;

    TMultiJoinKvReduce(const IRawCoGroupByKeyPtr& rawComputation, const std::vector<TRowVtable>& inVtables, const IYtJobOutputPtr& output)
    {
        State_[ComputationKey_] = SerializableToNode(*rawComputation);
        State_[InVtablesKey_] = SaveToNode(inVtables);
        State_[OutputKey_] = SerializableToNode(*output);
    }

    void Do(const TRawJobContext& /*jobContext*/) override
    {
        signal(SIGSEGV, [] (int) {PrintBackTrace(); Y_FAIL("FAIL");});
        try {
            auto output = SerializableFromNode<IYtJobOutput>(State_.At(OutputKey_));

            auto inRowVtables = LoadVtablesFromNode(State_.At(InVtablesKey_));
            auto rawComputation = SerializableFromNode<IRawCoGroupByKey>(State_.At(ComputationKey_));
            auto rangesReader = CreateRangesTableReader(&Cin);

            for (; rangesReader->IsValid(); rangesReader->Next()) {
                auto range = &rangesReader->GetRange();
                auto input = CreateSplitKvJobInput(inRowVtables, range);
                ProcessOneGroup(rawComputation, input, output);
            }

            output->Close();
        } catch (...) {
            Cerr << "Error in TJoinKvReduce" << Endl;
            Cerr << TBackTrace::FromCurrentException().PrintToString() << Endl;
            throw;
        }
    }

private:
    TNode State_;

private:
    static constexpr TStringBuf ComputationKey_ = "computation";
    static constexpr TStringBuf InVtablesKey_ = "in_row_vtables";
    static constexpr TStringBuf OutputKey_ = "out_row_vtable";

public:
    Y_SAVELOAD_JOB(State_);
};
REGISTER_RAW_JOB(TMultiJoinKvReduce);

class TStatefulKvReduce
    : public NYT::IRawJob
{
public:
    TStatefulKvReduce() = default;

    TStatefulKvReduce(const IRawStatefulParDoPtr& rawComputation, const std::vector<TRowVtable>& inVtables, const std::vector<IYtJobOutputPtr>& outputs, const TYtStateVtable& stateVtable)
    {
        State_[ComputationKey_] = SerializableToNode(*rawComputation);
        State_[InVtablesKey_] = SaveToNode(inVtables);
        State_[OutputsKey_] = TNode::CreateList();
        for (const auto& o : outputs) {
            State_[OutputsKey_].Add(SerializableToNode(*o));
        }
        State_[StateVtableKey_] = TYtStateVtable::SerializableToNode(stateVtable);
    }

    class TRawStateStore
        : public IRawStateStore
    {
    public:
        TRawStateStore()
        {
        }

        void SetState(TRawRowHolder& stateHolder)
        {
            RawState_ = stateHolder.GetData();
        }

        void* GetStateRaw(const void* key) final
        {
            Y_UNUSED(key);
            return RawState_;
        }

    protected:
        void* RawState_ = nullptr;
    };

    void Do(const TRawJobContext& /*jobContext*/) override
    {
        signal(SIGSEGV, [] (int) {PrintBackTrace(); Y_FAIL("FAIL");});

        try {
            const auto inRowVtables = LoadVtablesFromNode(State_.At(InVtablesKey_));
            const auto statefulParDo = SerializableFromNode<IRawStatefulParDo>(State_.At(ComputationKey_));
            const auto stateVtable = TYtStateVtable::SerializableFromNode(State_[StateVtableKey_]);

            auto executionContext = ::MakeIntrusive<TYtExecutionContext>();
            auto rawStateStore = ::MakeIntrusive<TRawStateStore>();

            std::vector<IRawOutputPtr> outputs;
            const auto& rawOutputs = State_[OutputsKey_].AsList();
            for (auto it = rawOutputs.begin() + 1; it != rawOutputs.end(); ++it) {
                auto output = SerializableFromNode<IYtJobOutput>(*it);
                outputs.emplace_back(std::move(output));
            }

            statefulParDo->Start(executionContext, rawStateStore, outputs);

            for (auto rangesReader = CreateRangesTableReader(&Cin); rangesReader->IsValid(); rangesReader->Next()) {
                auto range = &rangesReader->GetRange();
                auto input = CreateSplitKvJobInput(inRowVtables, range);

                TRowVtable keyVtable;
                TRawRowHolder keyHolder;

                std::deque<std::vector<TRawRowHolder>> values;
                values.resize(inRowVtables.size());

                // read all values for key
                while (const void* raw = input->NextRaw()) {
                    auto inputIndex = input->GetInputIndex();
                    Y_ENSURE(inputIndex < inRowVtables.size(), "Input index must be less than input tags count");
                    const auto& inputVtable = inRowVtables[inputIndex];

                    if (!IsDefined(keyVtable)) {
                        keyVtable = inputVtable.KeyVtableFactory();
                        keyHolder = TRawRowHolder{keyVtable};
                        keyHolder.CopyFrom(reinterpret_cast<const char*>(raw) + inputVtable.KeyOffset);
                    }

                    values[inputIndex].emplace_back(inputVtable);
                    values[inputIndex].back().CopyFrom(raw);
                }

                Y_VERIFY(IsDefined(keyVtable));
                Y_VERIFY(values[0].size() <= 1);

                TRawRowHolder state = values[0].empty()?
                    stateVtable.StateFromKey(keyHolder.GetData())
                    : stateVtable.StateFromTKV(std::move(values[0][0].GetData()));
                values.pop_front();

                rawStateStore->SetState(state);
                for (const auto& rawRowHolder : values[0]) {
                    statefulParDo->Do(rawRowHolder.GetData(), 1);
                }

                // TODO: write State to output

                for (const auto& output : outputs) {
                    output->Close();
                }
            }

            statefulParDo->Finish();
        } catch (...) {
            Cerr << "Error in TStatefulKvReduce" << Endl;
            Cerr << TBackTrace::FromCurrentException().PrintToString() << Endl;
            throw;
        }
    }

private:
    TNode State_;

private:
    static constexpr TStringBuf ComputationKey_ = "computation";
    static constexpr TStringBuf InVtablesKey_ = "in_row_vtables";
    static constexpr TStringBuf OutputsKey_ = "out_row_vtable";
    static constexpr TStringBuf StateVtableKey_ = "state_vtable_key";

public:
    Y_SAVELOAD_JOB(State_);
};
REGISTER_RAW_JOB(TStatefulKvReduce);

////////////////////////////////////////////////////////////////////////////////

class TCoGbkImpulseReadParDo
    : public IRawParDo
{
public:
    TCoGbkImpulseReadParDo() = default;

    TCoGbkImpulseReadParDo(IRawCoGroupByKeyPtr rawCoGbk, std::vector<TRowVtable> rowVtable)
        : RawCoGroupByKey_(rawCoGbk)
        , InputRowVtableList_(rowVtable)
    { }

    void Start(const IExecutionContextPtr& context, const std::vector<IRawOutputPtr>& outputs) override
    {
        Y_VERIFY(context->GetExecutorName() == "yt");

        Y_VERIFY(outputs.size() == 1);
        Processed_ = false;
        Output_ = outputs[0];
    }

    void Do(const void* rows, int count) override
    {
        Y_VERIFY(count == 1);
        Y_VERIFY(*static_cast<const int*>(rows) == 0);
        Y_VERIFY(!Processed_);
        Processed_ = true;


        auto rangesReader = CreateRangesTableReader(&Cin);

        for (; rangesReader->IsValid(); rangesReader->Next()) {
            auto range = &rangesReader->GetRange();
            auto input = CreateSplitKvJobInput(InputRowVtableList_, range);
            ProcessOneGroup(RawCoGroupByKey_, input, Output_);
        }
    }

    void Finish() override
    { }

    const TFnAttributes& GetFnAttributes() const override
    {
        static TFnAttributes attributes;
        return attributes;
    }

    TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IRawParDoPtr {
            return ::MakeIntrusive<TCoGbkImpulseReadParDo>();
        };
    }

    std::vector<TDynamicTypeTag> GetInputTags() const override
    {
        return {
            {"input", MakeRowVtable<int>()}
        };
    }

    std::vector<TDynamicTypeTag> GetOutputTags() const override
    {
        return RawCoGroupByKey_->GetOutputTags();
    }

private:
    IRawCoGroupByKeyPtr RawCoGroupByKey_;
    std::vector<TRowVtable> InputRowVtableList_;

    Y_SAVELOAD_DEFINE_OVERRIDE(RawCoGroupByKey_, InputRowVtableList_);

    IRawOutputPtr Output_;
    bool Processed_ = false;
};

////////////////////////////////////////////////////////////////////////////////

class TCombineCombinerImpulseReadParDo
    : public IRawParDo
{
public:
    TCombineCombinerImpulseReadParDo() = default;

    explicit TCombineCombinerImpulseReadParDo(IRawCombinePtr rawCombine)
        : RawCombine_(std::move(rawCombine))
    { }

    void Start(const IExecutionContextPtr& context, const std::vector<IRawOutputPtr>& outputs) override
    {
        Y_VERIFY(context->GetExecutorName() == "yt");

        Y_VERIFY(outputs.size() == 0);
        Processed_ = false;
    }

    void Do(const void* rows, int count) override
    {
        Y_VERIFY(count == 1);
        Y_VERIFY(*static_cast<const int*>(rows) == 0);
        Y_VERIFY(!Processed_);
        Processed_ = true;

        auto rangesReader = CreateRangesTableReader(&Cin);

        auto accumVtable = RawCombine_->GetAccumVtable();
        auto inputVtable = RawCombine_->GetInputVtable();
        auto kvOutput = CreateKvJobOutput(
            /*sinkIndex*/ 0,
            inputVtable.KeyVtableFactory().RawCoderFactory(),
            accumVtable.RawCoderFactory());

        TRawRowHolder accum(accumVtable);
        TRawRowHolder currentKey(inputVtable.KeyVtableFactory());
        for (; rangesReader->IsValid(); rangesReader->Next()) {
            auto range = &rangesReader->GetRange();
            auto input = CreateSplitKvJobInput(std::vector{inputVtable}, range);

            bool first = true;
            RawCombine_->CreateAccumulator(accum.GetData());
            while (const auto* row = input->NextRaw()) {
                const auto* key = GetKeyOfKv(inputVtable, row);
                const auto* value = GetValueOfKv(inputVtable, row);

                RawCombine_->AddInput(accum.GetData(), value);

                if (first) {
                    first = false;
                    currentKey.CopyFrom(key);
                }
            }
            kvOutput->AddKvToTable(currentKey.GetData(), accum.GetData(), /*tableIndex*/ 0);
        }

        kvOutput->Close();
    }

    void Finish() override
    { }

    const TFnAttributes& GetFnAttributes() const override
    {
        static TFnAttributes attributes;
        return attributes;
    }

    TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IRawParDoPtr {
            return ::MakeIntrusive<TCombineCombinerImpulseReadParDo>();
        };
    }

    std::vector<TDynamicTypeTag> GetInputTags() const override
    {
        return {
            {"input", MakeRowVtable<int>()}
        };
    }

    std::vector<TDynamicTypeTag> GetOutputTags() const override
    {
        return {};
    }

private:
    IRawCombinePtr RawCombine_;

    bool Processed_ = false;

    Y_SAVELOAD_DEFINE_OVERRIDE(RawCombine_);
};

////////////////////////////////////////////////////////////////////////////////

class TCombineReducerImpulseReadParDo
    : public IRawParDo
{
public:
    TCombineReducerImpulseReadParDo() = default;

    explicit TCombineReducerImpulseReadParDo(IRawCombinePtr rawCombine)
        : RawCombine_(std::move(rawCombine))
    { }

    void Start(const IExecutionContextPtr& context, const std::vector<IRawOutputPtr>& outputs) override
    {
        Y_VERIFY(context->GetExecutorName() == "yt");

        Y_VERIFY(outputs.size() == 1);
        Processed_ = false;
        Output_ = outputs[0];
    }

    void Do(const void* rows, int count) override
    {
        Y_VERIFY(count == 1);
        Y_VERIFY(*static_cast<const int*>(rows) == 0);
        Y_VERIFY(!Processed_);
        Processed_ = true;

        auto outRowVtable = RawCombine_->GetOutputVtable();
        auto accumVtable = RawCombine_->GetAccumVtable();
        auto keyVtable = outRowVtable.KeyVtableFactory();

        auto rangesReader = CreateRangesTableReader(&Cin);

        TRawRowHolder accum(accumVtable);
        TRawRowHolder currentKey(outRowVtable.KeyVtableFactory());
        TRawRowHolder out(outRowVtable);
        RawCombine_->CreateAccumulator(accum.GetData());
        for (; rangesReader->IsValid(); rangesReader->Next()) {
            auto range = &rangesReader->GetRange();
            auto keySavingInput =
                ::MakeIntrusive<TKeySavingInput>(keyVtable, accumVtable, range);
            keySavingInput->SaveNextKeyTo(out.GetKeyOfKV());
            RawCombine_->MergeAccumulators(accum.GetData(), keySavingInput);
            RawCombine_->ExtractOutput(out.GetValueOfKV(), accum.GetData());

            Output_->AddRaw(out.GetData(), 1);
        }
    }

    void Finish() override
    { }

    const TFnAttributes& GetFnAttributes() const override
    {
        static TFnAttributes attributes;
        return attributes;
    }

    TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IRawParDoPtr {
            return ::MakeIntrusive<TCombineReducerImpulseReadParDo>();
        };
    }

    std::vector<TDynamicTypeTag> GetInputTags() const override
    {
        return {
            {"input", MakeRowVtable<int>()}
        };
    }

    std::vector<TDynamicTypeTag> GetOutputTags() const override
    {
        return {
            {"TCombineReducerImpulseReadParDo.Output", RawCombine_->GetOutputVtable()},
        };
    }

private:
    class TKeySavingInput
        : public IRawInput
    {
    public:
        TKeySavingInput(const TRowVtable& keyVtable, TRowVtable valueVtable, TTableReaderPtr<TNode> tableReader)
            : ValueVtable_(std::move(valueVtable))
            , TableReader_(std::move(tableReader))
            , KeyDecoder_(keyVtable.RawCoderFactory())
            , ValueDecoder_(ValueVtable_.RawCoderFactory())
            , ValueHolder_(ValueVtable_)
        { }

        void SaveNextKeyTo(void* key)
        {
            KeyOutput_ = key;
        }

        const void* NextRaw() override
        {
            if (TableReader_->IsValid()) {
                auto node = TableReader_->GetRow();
                if (KeyOutput_) {
                    KeyDecoder_->DecodeRow(node["key"].AsString(), KeyOutput_);
                    KeyOutput_ = nullptr;
                }
                ValueDecoder_->DecodeRow(node["value"].AsString(), ValueHolder_.GetData());
                TableReader_->Next();
                return ValueHolder_.GetData();
            } else {
                return nullptr;
            }
        }

    private:
        TRowVtable ValueVtable_;

        NYT::TTableReaderPtr<TNode> TableReader_;
        IRawCoderPtr KeyDecoder_;
        IRawCoderPtr ValueDecoder_;
        TRawRowHolder ValueHolder_;

        void* KeyOutput_ = nullptr;
    };


private:
    IRawCombinePtr RawCombine_;

    IRawOutputPtr Output_;
    bool Processed_ = false;

    Y_SAVELOAD_DEFINE_OVERRIDE(RawCombine_);
};

////////////////////////////////////////////////////////////////////////////////

IRawJobPtr CreateParDoMap(
    const IRawParDoPtr& rawParDo,
    const IYtJobInputPtr& input,
    const std::vector<IYtJobOutputPtr>& outputs)
{
    return ::MakeIntrusive<TParDoMap>(rawParDo, input, outputs);
}

IRawJobPtr CreateSplitKvMap(
    const std::vector<TRowVtable>& rowVtables)
{
    return ::MakeIntrusive<TSplitKvMap>(rowVtables);
}

IRawJobPtr CreateSplitKvMap(
    TRowVtable rowVtable)
{
    return CreateSplitKvMap(std::vector{std::move(rowVtable)});
}

IRawJobPtr CreateSplitStateKvMap(
    const std::vector<TRowVtable>& rowVtables,
    TYtStateVtable stateVtable)
{
    return ::MakeIntrusive<TSplitStateKvMap>(rowVtables, std::move(stateVtable));
}

IRawParDoPtr CreateGbkImpulseReadParDo(IRawGroupByKeyPtr rawGroupByKey)
{
    return ::MakeIntrusive<TGbkImpulseReadParDo>(std::move(rawGroupByKey));
}

IRawJobPtr CreateMultiJoinKvReduce(
    const IRawCoGroupByKeyPtr& rawComputation,
    const std::vector<TRowVtable>& inVtables,
    const IYtJobOutputPtr& output)
{
    return ::MakeIntrusive<TMultiJoinKvReduce>(rawComputation, inVtables, output);
}

IRawParDoPtr CreateCoGbkImpulseReadParDo(
    IRawCoGroupByKeyPtr rawCoGbk,
    std::vector<TRowVtable> rowVtableList)
{
    return ::MakeIntrusive<TCoGbkImpulseReadParDo>(std::move(rawCoGbk), std::move(rowVtableList));
}

IRawJobPtr CreateStatefulKvReduce(
    const IRawStatefulParDoPtr& rawComputation,
    const std::vector<TRowVtable>& inVtables,
    const std::vector<IYtJobOutputPtr>& outputs,
    const TYtStateVtable& stateVtable)
{
    return ::MakeIntrusive<TStatefulKvReduce>(rawComputation, inVtables, outputs, stateVtable);
}

IRawJobPtr CreateCombineCombiner(
    const IRawCombinePtr& combine,
    const TRowVtable& /*inRowVtable*/)
{
    return ::MakeIntrusive<TImpulseRawJob>(CreateCombineCombinerImpulseReadParDo(combine));
}

IRawParDoPtr CreateCombineCombinerImpulseReadParDo(IRawCombinePtr rawCombine)
{
    return ::MakeIntrusive<TCombineCombinerImpulseReadParDo>(std::move(rawCombine));
}

IRawJobPtr CreateCombineReducer(
    const IRawCombinePtr& combine,
    const TRowVtable& outRowVtable,
    const IYtJobOutputPtr& output)
{
    TParDoTreeBuilder builder;
    builder.AddParDoChain(
        TParDoTreeBuilder::RootNodeId,
        {
            CreateCombineReducerImpulseReadParDo(std::move(combine)),
            CreateOutputParDo(output, outRowVtable),
        }
    );
    return ::MakeIntrusive<TImpulseRawJob>(builder.Build());
}

IRawParDoPtr CreateCombineReducerImpulseReadParDo(IRawCombinePtr rawCombine)
{
    return ::MakeIntrusive<TCombineReducerImpulseReadParDo>(std::move(rawCombine));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
