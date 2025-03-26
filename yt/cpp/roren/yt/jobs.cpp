#include "jobs.h"

#include "table_stream_registry.h"

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
        Y_ABORT("not implemented");
    }

    void DeleteTimer(const TTimer::TKey& key) override
    {
        Y_UNUSED(key);
        Y_ABORT("not implemented");
    }
};

IExecutionContextPtr CreateYtExecutionContext()
{
    return ::MakeIntrusive<TYtExecutionContext>();
}

////////////////////////////////////////////////////////////////////////////////

NYT::TTableRangesReaderPtr<TNode> CreateRangesNodeTableReader(IInputStream* stream)
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
        signal(SIGSEGV, [] (int) {PrintBackTrace(); Y_ABORT("FAIL");});

        try {
            auto executionContext = ::MakeIntrusive<TYtExecutionContext>();
            Y_ABORT_UNLESS(ParDo_->GetOutputTags().empty());
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
            auto writer = CreateKvJobNodeOutput(/*sinkIndex*/ 0, RowVtables_);

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

            auto writer = CreateKvJobNodeOutput(/*sinkIndex*/ 0, RowVtables_);

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

class TGbkImpulseReadNodeParDo
    : public IRawParDo
{
public:
    TGbkImpulseReadNodeParDo(IRawGroupByKeyPtr rawGroupByKey)
        : RawGroupByKey_(rawGroupByKey)
    { }

    void Start(const IExecutionContextPtr& context, const std::vector<IRawOutputPtr>& outputs) override
    {
        Y_ABORT_UNLESS(context->GetExecutorName() == "yt");

        Y_ABORT_UNLESS(outputs.size() == 1);
        Processed_ = false;
        Output_ = outputs[0];
    }

    void Do(const void* rows, int count) override
    {
        Y_ABORT_UNLESS(count == 1);
        Y_ABORT_UNLESS(*static_cast<const int*>(rows) == 0);
        Y_ABORT_UNLESS(!Processed_);
        Processed_ = true;

        const auto gbkInputTags = RawGroupByKey_->GetInputTags();
        Y_ABORT_UNLESS(gbkInputTags.size() == 1);
        const auto& rowVtable = gbkInputTags[0].GetRowVtable();

        auto rangesReader = CreateRangesNodeTableReader(&Cin);

        for (; rangesReader->IsValid(); rangesReader->Next()) {
            auto range = &rangesReader->GetRange();
            auto input = CreateSplitKvJobNodeInput(std::vector{rowVtable}, range);
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
            return ::MakeIntrusive<TGbkImpulseReadNodeParDo>(nullptr);
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
        Y_ENSURE(inputIndex < ssize(inputTags), "Input index must be less than input tags count");
        auto inputVtable = inputTags[inputIndex].GetRowVtable();

        if (!IsDefined(keyVtable)) {
            keyVtable = inputVtable.KeyVtableFactory();
            keyHolder = TRawRowHolder{keyVtable};
            keyHolder.CopyFrom(reinterpret_cast<const char*>(raw) + inputVtable.KeyOffset);
        }

        values[inputIndex].emplace_back(inputVtable);
        values[inputIndex].back().CopyFrom(raw);
    }

    Y_ABORT_UNLESS(IsDefined(keyVtable));

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
        signal(SIGSEGV, [] (int) {PrintBackTrace(); Y_ABORT("FAIL");});
        try {
            auto output = SerializableFromNode<IYtJobOutput>(State_.At(OutputKey_));

            auto inRowVtables = LoadVtablesFromNode(State_.At(InVtablesKey_));
            auto rawComputation = SerializableFromNode<IRawCoGroupByKey>(State_.At(ComputationKey_));
            auto rangesReader = CreateRangesNodeTableReader(&Cin);

            for (; rangesReader->IsValid(); rangesReader->Next()) {
                auto range = &rangesReader->GetRange();
                auto input = CreateSplitKvJobNodeInput(inRowVtables, range);
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
        signal(SIGSEGV, [] (int) {PrintBackTrace(); Y_ABORT("FAIL");});

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

            auto stateStream = GetTableStream(0);
            auto stateYsonWriter = std::make_unique<::NYson::TYsonWriter>(
                stateStream,
                ::NYson::EYsonFormat::Binary,
                ::NYson::EYsonType::ListFragment
            );

            statefulParDo->Start(executionContext, rawStateStore, outputs);

            for (auto rangesReader = CreateRangesNodeTableReader(&Cin); rangesReader->IsValid(); rangesReader->Next()) {
                auto range = &rangesReader->GetRange();
                auto input = CreateSplitKvJobNodeInput(inRowVtables, range);

                TRowVtable keyVtable;
                TRawRowHolder keyHolder;

                std::deque<std::vector<TRawRowHolder>> values;
                values.resize(inRowVtables.size());

                // read all values for key
                while (const void* raw = input->NextRaw()) {
                    auto inputIndex = input->GetInputIndex();
                    Y_ENSURE(inputIndex < ssize(inRowVtables), "Input index must be less than input tags count");
                    const auto& inputVtable = inRowVtables[inputIndex];

                    if (!IsDefined(keyVtable)) {
                        keyVtable = inputVtable.KeyVtableFactory();
                        keyHolder = TRawRowHolder{keyVtable};
                        keyHolder.CopyFrom(reinterpret_cast<const char*>(raw) + inputVtable.KeyOffset);
                    }

                    values[inputIndex].emplace_back(inputVtable);
                    values[inputIndex].back().CopyFrom(raw);
                }

                Y_ABORT_UNLESS(IsDefined(keyVtable));
                Y_ABORT_UNLESS(values[0].size() <= 1);

                TRawRowHolder stateTKV = values[0].empty()? TRawRowHolder(inRowVtables[0]) : std::move(values[0][0]);
                TRawRowHolder state = values[0].empty()?
                    stateVtable.StateFromKey(keyHolder.GetData())
                    : stateVtable.StateFromTKV(stateTKV.GetData());
                values.pop_front();

                rawStateStore->SetState(state);
                for (const auto& rawRowHolder : values[0]) {
                    statefulParDo->Do(rawRowHolder.GetData(), 1);
                }

                stateVtable.SaveState(*stateYsonWriter, state.GetData(), stateTKV.GetData());

                for (const auto& output : outputs) {
                    output->Close();
                }
            }

            statefulParDo->Finish();

            stateYsonWriter.reset();
            stateStream->Flush();
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

class TStateDecodingParDo
    : public IRawParDo
{
public:
    TStateDecodingParDo() = default;

    TStateDecodingParDo(const TYtStateVtable& stateVtable)
        : StateVtable_(stateVtable)
    { }

    std::vector<TDynamicTypeTag> GetInputTags() const override
    {
        return {TDynamicTypeTag("TStateDecodingParDo.Input", MakeRowVtable<TNode>())};
    }

    std::vector<TDynamicTypeTag> GetOutputTags() const override
    {
        return {TDynamicTypeTag("TStateDecodingParDo.Output", StateVtable_.StateTKVvtable)};
    }

    void Start(const IExecutionContextPtr& context, const std::vector<IRawOutputPtr>& outputs) override
    {
        Y_ABORT_UNLESS(context->GetExecutorName() == "yt");
        Y_ABORT_UNLESS(outputs.size() == 1);
        Output_ = outputs[0];
        StateHolder_.Reset(StateVtable_.StateTKVvtable);
    }

    void Do(const void* rows, int count) override
    {
        const auto* curRow = static_cast<const TNode*>(rows);
        for (int i = 0; i < count; ++i, ++curRow) {
            StateVtable_.LoadState(StateHolder_, *curRow);

            Output_->AddRaw(StateHolder_.GetData(), 1);
        }
    }

    void Finish() override
    { }

    const TFnAttributes& GetFnAttributes() const override
    {
        static const TFnAttributes fnAttributes;
        return fnAttributes;
    }

    TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IRawParDoPtr {
            return ::MakeIntrusive<TStateDecodingParDo>();
        };
    }

private:
    TYtStateVtable StateVtable_;

    IRawOutputPtr Output_;
    TRawRowHolder StateHolder_;

    Y_SAVELOAD_DEFINE_OVERRIDE(StateVtable_);
};

////////////////////////////////////////////////////////////////////////////////

class TStateEncodingParDo
    : public IRawParDo
{
public:
    TStateEncodingParDo() = default;

    TStateEncodingParDo(const TYtStateVtable& stateVtable)
        : StateVtable_(stateVtable)
    { }

    std::vector<TDynamicTypeTag> GetInputTags() const override
    {
        return {TDynamicTypeTag("TStateEncodingParDo.Input", StateVtable_.StateTKVvtable)};
    }

    std::vector<TDynamicTypeTag> GetOutputTags() const override
    {
        return {TDynamicTypeTag("TStateEncodingParDo.Output", MakeRowVtable<TNode>())};
    }

    void Start(const IExecutionContextPtr& context, const std::vector<IRawOutputPtr>& outputs) override
    {
        Y_ABORT_UNLESS(context->GetExecutorName() == "yt");
        Y_ABORT_UNLESS(outputs.size() == 1);
        Output_ = outputs[0];
        if (!KeyCoder_) {
            KeyCoder_ = StateVtable_.StateTKVvtable.KeyVtableFactory().RawCoderFactory();
            ValueCoder_ = StateVtable_.StateTKVvtable.ValueVtableFactory().RawCoderFactory();
        }
    }

    void Do(const void* rows, int count) override
    {
        const auto* curRow = static_cast<const std::byte*>(rows);
        TRawRowHolder rowHolder;
        for (int i = 0; i < count; ++i, curRow += StateVtable_.StateTKVvtable.DataSize) {
            KeyBuffer_.clear();
            ValueBuffer_.clear();
            {
                auto keyOut = TStringOutput(KeyBuffer_);
                auto valueOut = TStringOutput(ValueBuffer_);
                KeyCoder_->EncodeRow(&keyOut, GetKeyOfKv(StateVtable_.StateTKVvtable, curRow));
                ValueCoder_->EncodeRow(&valueOut, GetValueOfKv(StateVtable_.StateTKVvtable, curRow));
            }
            ResultNode_["key"] = KeyBuffer_;
            ResultNode_["value"] = ValueBuffer_;
            Output_->AddRaw(&ResultNode_, 1);
        }
    }

    void Finish() override
    { }

    const TFnAttributes& GetFnAttributes() const override
    {
        static const TFnAttributes fnAttributes;
        return fnAttributes;
    }

    TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IRawParDoPtr {
            return ::MakeIntrusive<TStateEncodingParDo>();
        };
    }

private:
    TYtStateVtable StateVtable_;

    IRawOutputPtr Output_;
    IRawCoderPtr KeyCoder_;
    IRawCoderPtr ValueCoder_;
    TNode ResultNode_;
    TString KeyBuffer_;
    TString ValueBuffer_;

    Y_SAVELOAD_DEFINE_OVERRIDE(StateVtable_);
};

////////////////////////////////////////////////////////////////////////////////

class TStatefulParDoReducerImpulseReadNode
    : public IRawParDo
{
private:
    class TRawStateStore
        : public IRawStateStore
    {
    public:
        TRawStateStore() = default;

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

    using TRawStateStorePtr = ::TIntrusivePtr<TRawStateStore>;

    void StatefulProcessOneGroup(
        const IRawStatefulParDoPtr& statefulParDo,
        const TRawStateStorePtr& rawStateStore,
        const TYtStateVtable& stateVtable,
        const std::unique_ptr<::NYson::TYsonWriter>& stateYsonWriter,
        const IYtNotSerializableJobInputPtr& input,
        const std::vector<TRowVtable>& inVtables,
        const IRawOutputPtr& output)
    {
        TRowVtable keyVtable;
        TRawRowHolder keyHolder;

        // TODO(whatsername): deque?
        std::deque<std::vector<TRawRowHolder>> values;
        values.resize(std::ssize(inVtables));

        // Read all values for the key
        while (const void* raw = input->NextRaw()) {
            auto inputIndex = input->GetInputIndex();
            Y_ENSURE(inputIndex < std::ssize(inVtables), "Input index must be less than input tags count");
            const auto& inputVtable = inVtables[inputIndex];

            if (!IsDefined(keyVtable)) {
                keyVtable = inputVtable.KeyVtableFactory();
                keyHolder = TRawRowHolder{keyVtable};
                keyHolder.CopyFrom(reinterpret_cast<const char*>(raw) + inputVtable.KeyOffset);
            }

            values[inputIndex].emplace_back(inputVtable);
            values[inputIndex].back().CopyFrom(raw);
        }

        Y_ABORT_UNLESS(IsDefined(keyVtable));

        TRawRowHolder stateTKV;
        TRawRowHolder state;
        // Check if we have state for the key
        if (values[0].empty()) {
            stateTKV = TRawRowHolder(InVtables_[0]);
            state = stateVtable.StateFromKey(keyHolder.GetData());
        } else {
            Y_ABORT_UNLESS(values[0].size() == 1);
            stateTKV = std::move(values[0][0]);
            state = stateVtable.StateFromTKV(stateTKV.GetData());
        }
        values.pop_front();

        rawStateStore->SetState(state);
        for (const auto& rawRowHolder : values[0]) {
            statefulParDo->Do(rawRowHolder.GetData(), 1);
        }

        stateVtable.SaveState(*stateYsonWriter, state.GetData(), stateTKV.GetData());

        output->Close();
    }

public:
    TStatefulParDoReducerImpulseReadNode() = default;

    TStatefulParDoReducerImpulseReadNode(IRawStatefulParDoPtr rawStatefulParDo, const TYtStateVtable& stateVtable)
        : RawStatefulParDo_(std::move(rawStatefulParDo))
        , StateVtable_(stateVtable)
    { }

    void Start(const IExecutionContextPtr& context, const std::vector<IRawOutputPtr>& outputs) override
    {
        Y_ABORT_UNLESS(context->GetExecutorName() == "yt");
        Y_ABORT_UNLESS(outputs.size() == 1);
        Processed_ = false;

        Output_ = outputs[0];
        RawStateStore_ = ::MakeIntrusive<TRawStateStore>();

        RawStatefulParDo_->Start(context, RawStateStore_, outputs);

        InVtables_.push_back(StateVtable_.StateTKVvtable);
        for (const auto& inputTag : RawStatefulParDo_->GetInputTags()) {
            InVtables_.push_back(inputTag.GetRowVtable());
        }

        // TODO(whatsername): Something more elegant?
        StateStream_ = GetTableStream(0);
        StateYsonWriter_ = std::make_unique<::NYson::TYsonWriter>(
            StateStream_,
            ::NYson::EYsonFormat::Binary,
            ::NYson::EYsonType::ListFragment
        );
    }

    void Do(const void* rows, int count) override
    {
        Y_ABORT_UNLESS(count == 1);
        Y_ABORT_UNLESS(*static_cast<const int*>(rows) == 0);
        Y_ABORT_UNLESS(!Processed_);
        Processed_ = true;

        auto rangesReader = CreateRangesNodeTableReader(&Cin);

        for (; rangesReader->IsValid(); rangesReader->Next()) {
            auto range = &rangesReader->GetRange();
            auto input = CreateSplitKvJobNodeInput(InVtables_, range);
            StatefulProcessOneGroup(RawStatefulParDo_, RawStateStore_, StateVtable_, StateYsonWriter_, input, InVtables_, Output_);
        }
    }

    void Finish() override
    {
        RawStatefulParDo_->Finish();

        StateYsonWriter_.reset();
        StateStream_->Finish();
    }

    const TFnAttributes& GetFnAttributes() const override
    {
        static TFnAttributes attributes;
        return attributes;
    }

    TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IRawParDoPtr {
            return ::MakeIntrusive<TStatefulParDoReducerImpulseReadNode>();
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
        return RawStatefulParDo_->GetOutputTags();
    }

private:
    IRawStatefulParDoPtr RawStatefulParDo_;
    TYtStateVtable StateVtable_;

    Y_SAVELOAD_DEFINE_OVERRIDE(RawStatefulParDo_, StateVtable_);

    bool Processed_ = false;

    IRawOutputPtr Output_;
    TRawStateStorePtr RawStateStore_;

    std::vector<TRowVtable> InVtables_;

    IOutputStream* StateStream_ = nullptr;
    std::unique_ptr<::NYson::TYsonWriter> StateYsonWriter_;
};

////////////////////////////////////////////////////////////////////////////////

class TCoGbkImpulseReadNodeParDo
    : public IRawParDo
{
public:
    TCoGbkImpulseReadNodeParDo() = default;

    TCoGbkImpulseReadNodeParDo(IRawCoGroupByKeyPtr rawCoGbk, std::vector<TRowVtable> rowVtable)
        : RawCoGroupByKey_(rawCoGbk)
        , InputRowVtableList_(rowVtable)
    { }

    void Start(const IExecutionContextPtr& context, const std::vector<IRawOutputPtr>& outputs) override
    {
        Y_ABORT_UNLESS(context->GetExecutorName() == "yt");

        Y_ABORT_UNLESS(outputs.size() == 1);
        Processed_ = false;
        Output_ = outputs[0];
    }

    void Do(const void* rows, int count) override
    {
        Y_ABORT_UNLESS(count == 1);
        Y_ABORT_UNLESS(*static_cast<const int*>(rows) == 0);
        Y_ABORT_UNLESS(!Processed_);
        Processed_ = true;

        auto rangesReader = CreateRangesNodeTableReader(&Cin);

        for (; rangesReader->IsValid(); rangesReader->Next()) {
            auto range = &rangesReader->GetRange();
            auto input = CreateSplitKvJobNodeInput(InputRowVtableList_, range);
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
            return ::MakeIntrusive<TCoGbkImpulseReadNodeParDo>();
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

class TCombineCombinerImpulseReadNodeParDo
    : public IRawParDo
{
public:
    TCombineCombinerImpulseReadNodeParDo() = default;

    explicit TCombineCombinerImpulseReadNodeParDo(IRawCombinePtr rawCombine)
        : RawCombine_(std::move(rawCombine))
    { }

    void Start(const IExecutionContextPtr& context, const std::vector<IRawOutputPtr>& outputs) override
    {
        Y_ABORT_UNLESS(context->GetExecutorName() == "yt");

        Y_ABORT_UNLESS(outputs.size() == 0);
        Processed_ = false;
        RawCombine_->Start(context);
    }

    void Do(const void* rows, int count) override
    {
        Y_ABORT_UNLESS(count == 1);
        Y_ABORT_UNLESS(*static_cast<const int*>(rows) == 0);
        Y_ABORT_UNLESS(!Processed_);
        Processed_ = true;

        auto rangesReader = CreateRangesNodeTableReader(&Cin);

        auto accumVtable = RawCombine_->GetAccumVtable();
        auto inputVtable = RawCombine_->GetInputVtable();
        auto kvOutput = CreateKvJobNodeOutput(
            /*sinkIndex*/ 0,
            inputVtable.KeyVtableFactory().RawCoderFactory(),
            accumVtable.RawCoderFactory());

        TRawRowHolder accum(accumVtable);
        TRawRowHolder currentKey(inputVtable.KeyVtableFactory());
        for (; rangesReader->IsValid(); rangesReader->Next()) {
            auto range = &rangesReader->GetRange();
            auto input = CreateSplitKvJobNodeInput(std::vector{inputVtable}, range);

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
            return ::MakeIntrusive<TCombineCombinerImpulseReadNodeParDo>();
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

class TCombineReducerImpulseReadNodeParDo
    : public IRawParDo
{
public:
    TCombineReducerImpulseReadNodeParDo() = default;

    explicit TCombineReducerImpulseReadNodeParDo(IRawCombinePtr rawCombine)
        : RawCombine_(std::move(rawCombine))
    { }

    void Start(const IExecutionContextPtr& context, const std::vector<IRawOutputPtr>& outputs) override
    {
        Y_ABORT_UNLESS(context->GetExecutorName() == "yt");

        Y_ABORT_UNLESS(outputs.size() == 1);
        Processed_ = false;
        Output_ = outputs[0];
        RawCombine_->Start(context);
    }

    void Do(const void* rows, int count) override
    {
        Y_ABORT_UNLESS(count == 1);
        Y_ABORT_UNLESS(*static_cast<const int*>(rows) == 0);
        Y_ABORT_UNLESS(!Processed_);
        Processed_ = true;

        auto outRowVtable = RawCombine_->GetOutputVtable();
        auto accumVtable = RawCombine_->GetAccumVtable();
        auto keyVtable = outRowVtable.KeyVtableFactory();

        auto rangesReader = CreateRangesNodeTableReader(&Cin);

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
            return ::MakeIntrusive<TCombineReducerImpulseReadNodeParDo>();
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
            {"TCombineReducerImpulseReadNodeParDo.Output", RawCombine_->GetOutputVtable()},
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

IRawParDoPtr CreateGbkImpulseReadNodeParDo(IRawGroupByKeyPtr rawGroupByKey)
{
    return ::MakeIntrusive<TGbkImpulseReadNodeParDo>(std::move(rawGroupByKey));
}

IRawJobPtr CreateMultiJoinKvReduce(
    const IRawCoGroupByKeyPtr& rawComputation,
    const std::vector<TRowVtable>& inVtables,
    const IYtJobOutputPtr& output)
{
    return ::MakeIntrusive<TMultiJoinKvReduce>(rawComputation, inVtables, output);
}

IRawParDoPtr CreateCoGbkImpulseReadNodeParDo(
    IRawCoGroupByKeyPtr rawCoGbk,
    std::vector<TRowVtable> rowVtableList)
{
    return ::MakeIntrusive<TCoGbkImpulseReadNodeParDo>(std::move(rawCoGbk), std::move(rowVtableList));
}

IRawJobPtr CreateStatefulKvReduce(
    const IRawStatefulParDoPtr& rawComputation,
    const std::vector<TRowVtable>& inVtables,
    const std::vector<IYtJobOutputPtr>& outputs,
    const TYtStateVtable& stateVtable)
{
    return ::MakeIntrusive<TStatefulKvReduce>(rawComputation, inVtables, outputs, stateVtable);
}

IRawParDoPtr CreateStateDecodingParDo(const TYtStateVtable& stateVtable)
{
    return ::MakeIntrusive<TStateDecodingParDo>(stateVtable);
}

IRawParDoPtr CreateStateEncodingParDo(const TYtStateVtable& stateVtable)
{
    return ::MakeIntrusive<TStateEncodingParDo>(stateVtable);
}

IRawParDoPtr CreateStatefulParDoReducerImpulseReadNode(IRawStatefulParDoPtr rawStatefulParDo, const TYtStateVtable& stateVtable)
{
    return ::MakeIntrusive<TStatefulParDoReducerImpulseReadNode>(std::move(rawStatefulParDo), stateVtable);
}

IRawJobPtr CreateCombineCombiner(
    const IRawCombinePtr& combine,
    const TRowVtable& /*inRowVtable*/)
{
    return ::MakeIntrusive<TImpulseRawJob>(CreateCombineCombinerImpulseReadNodeParDo(combine));
}

IRawParDoPtr CreateCombineCombinerImpulseReadNodeParDo(IRawCombinePtr rawCombine)
{
    return ::MakeIntrusive<TCombineCombinerImpulseReadNodeParDo>(std::move(rawCombine));
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
            CreateCombineReducerImpulseReadNodeParDo(std::move(combine)),
            CreateOutputParDo(output, outRowVtable),
        }
    );
    return ::MakeIntrusive<TImpulseRawJob>(builder.Build());
}

IRawParDoPtr CreateCombineReducerImpulseReadNodeParDo(IRawCombinePtr rawCombine)
{
    return ::MakeIntrusive<TCombineReducerImpulseReadNodeParDo>(std::move(rawCombine));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
