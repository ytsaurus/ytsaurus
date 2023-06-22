#include "jobs.h"

#include <yt/cpp/roren/interface/roren.h>

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/format.h>
#include <yt/cpp/mapreduce/interface/logging/yt_log.h>
#include <yt/cpp/mapreduce/io/node_table_reader.h>
#include <yt/cpp/mapreduce/io/stream_table_reader.h>

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

class TJoinKvReduce
    : public NYT::IRawJob
{
public:
    TJoinKvReduce() = default;

    TJoinKvReduce(const IRawGroupByKeyPtr& rawComputation, const TRowVtable& inVtable, const IYtJobOutputPtr& output)
    {
        State_[ComputationKey_] = SerializableToNode(*rawComputation);
        State_[InVtableKey_] = SaveToNode(inVtable);
        State_[OutputKey_] = SerializableToNode(*output);
    }

    void Do(const TRawJobContext& /*jobContext*/) override
    {
        signal(SIGSEGV, [] (int) {PrintBackTrace(); Y_FAIL("FAIL");});
        try {
            auto output = SerializableFromNode<IYtJobOutput>(State_.At(OutputKey_));

            auto inRowVtable = LoadVtableFromNode(State_.At(InVtableKey_));
            auto rawComputation = SerializableFromNode<IRawGroupByKey>(State_.At(ComputationKey_));
            auto rangesReader = CreateRangesTableReader(&Cin);

            for (; rangesReader->IsValid(); rangesReader->Next()) {
                auto range = &rangesReader->GetRange();
                auto input = CreateSplitKvJobInput(std::vector{inRowVtable}, range);
                rawComputation->ProcessOneGroup(input, output);
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
    static constexpr TStringBuf InVtableKey_ = "in_row_vtable";
    static constexpr TStringBuf OutputKey_ = "out_row_vtable";

public:
    Y_SAVELOAD_JOB(State_);
};
REGISTER_RAW_JOB(TJoinKvReduce);

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

////////////////////////////////////////////////////////////////////////////////

class TCombineCombiner
    : public NYT::IRawJob
{
public:
    TCombineCombiner() = default;

    TCombineCombiner(const IRawCombinePtr& combine, const TRowVtable& inRowVtable)
    {
        State_[ComputationKey_] = SerializableToNode(*combine);
        State_[InVtableKey_] = SaveToNode(inRowVtable);
    }

    void Do(const TRawJobContext& /*jobContext*/) override
    {
        try {
            auto rawComputation = SerializableFromNode<IRawCombine>(State_.At(ComputationKey_));
            auto inRowVtable = LoadVtableFromNode(State_.At(InVtableKey_));
            auto accumVtable = rawComputation->GetAccumVtable();

            auto rangesReader = CreateRangesTableReader(&Cin);
            auto kvOutput = CreateKvJobOutput(
                /*sinkIndex*/ 0,
                inRowVtable.KeyVtableFactory().RawCoderFactory(),
                accumVtable.RawCoderFactory());

            TRawRowHolder accum(accumVtable);
            TRawRowHolder currentKey(inRowVtable.KeyVtableFactory());
            for (; rangesReader->IsValid(); rangesReader->Next()) {
                auto range = &rangesReader->GetRange();
                auto input = CreateSplitKvJobInput(std::vector{inRowVtable}, range);

                bool first = true;
                rawComputation->CreateAccumulator(accum.GetData());
                while (const auto* row = input->NextRaw()) {
                    const auto* key = GetKeyOfKv(inRowVtable, row);
                    const auto* value = GetValueOfKv(inRowVtable, row);

                    rawComputation->AddInput(accum.GetData(), value);

                    if (first) {
                        first = false;
                        currentKey.CopyFrom(key);
                    }
                }
                kvOutput->AddKvToTable(currentKey.GetData(), accum.GetData(), /*tableIndex*/ 0);
            }

            kvOutput->Close();
        } catch (...) {
            Cerr << "Error in TCombineCombiner" << Endl;
            Cerr << TBackTrace::FromCurrentException().PrintToString() << Endl;
            throw;
        }
    }

private:
    TNode State_;

private:
    static constexpr TStringBuf ComputationKey_ = "computation";
    static constexpr TStringBuf InVtableKey_ = "in_row_vtable";

public:
    Y_SAVELOAD_JOB(State_);
};
REGISTER_RAW_JOB(TCombineCombiner);

////////////////////////////////////////////////////////////////////////////////

class TCombineReducer
    : public NYT::IRawJob
{
public:
    TCombineReducer() = default;

    TCombineReducer(const IRawCombinePtr& combine, const TRowVtable& outRowVtable, const IYtJobOutputPtr& output)
    {
        State_[ComputationKey_] = SerializableToNode(*combine);
        State_[OutVtableKey_] = SaveToNode(outRowVtable);
        State_[OutputKey_] = SerializableToNode(*output);
    }

    void Do(const TRawJobContext& /*jobContext*/) override
    {
        try {
            auto rawCombine = SerializableFromNode<IRawCombine>(State_.At(ComputationKey_));
            auto outRowVtable = LoadVtableFromNode(State_.At(OutVtableKey_));
            auto accumVtable = rawCombine->GetAccumVtable();
            auto keyVtable = outRowVtable.KeyVtableFactory();

            auto rangesReader = CreateRangesTableReader(&Cin);
            auto output = SerializableFromNode<IYtJobOutput>(State_.At(OutputKey_));

            TRawRowHolder accum(accumVtable);
            TRawRowHolder currentKey(outRowVtable.KeyVtableFactory());
            TRawRowHolder out(outRowVtable);
            rawCombine->CreateAccumulator(accum.GetData());
            for (; rangesReader->IsValid(); rangesReader->Next()) {
                auto range = &rangesReader->GetRange();
                auto keySavingInput = ::MakeIntrusive<TKeySavingInput>(keyVtable, accumVtable, range);
                keySavingInput->SaveNextKeyTo(out.GetKeyOfKV());
                rawCombine->MergeAccumulators(accum.GetData(), keySavingInput);
                rawCombine->ExtractOutput(out.GetValueOfKV(), accum.GetData());

                output->AddRaw(out.GetData(), 1);
            }
            output->Close();
        } catch (...) {
            Cerr << "Error in TCombineReducer" << Endl;
            Cerr << TBackTrace::FromCurrentException().PrintToString() << Endl;
            throw;
        }
    }

private:
    TNode State_;

private:
    static constexpr TStringBuf ComputationKey_ = "computation";
    static constexpr TStringBuf OutputKey_ = "output";
    static constexpr TStringBuf OutVtableKey_ = "out_row_vtable";

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

public:
    Y_SAVELOAD_JOB(State_);
};
REGISTER_RAW_JOB(TCombineReducer);

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

IRawJobPtr CreateJoinKvReduce(
    const IRawGroupByKeyPtr& rawComputation,
    const TRowVtable& inVtable,
    const IYtJobOutputPtr& output)
{
    return ::MakeIntrusive<TJoinKvReduce>(rawComputation, inVtable, output);
}

IRawJobPtr CreateMultiJoinKvReduce(
    const IRawCoGroupByKeyPtr& rawComputation,
    const std::vector<TRowVtable>& inVtables,
    const IYtJobOutputPtr& output)
{
    return ::MakeIntrusive<TMultiJoinKvReduce>(rawComputation, inVtables, output);
}

IRawJobPtr CreateCombineCombiner(
    const IRawCombinePtr& combine,
    const TRowVtable& inRowVtable)
{
    return ::MakeIntrusive<TCombineCombiner>(combine, inRowVtable);
}

IRawJobPtr CreateCombineReducer(
    const IRawCombinePtr& combine,
    const TRowVtable& outRowVtable,
    const IYtJobOutputPtr& output)
{
    return ::MakeIntrusive<TCombineReducer>(combine, outRowVtable, output);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
