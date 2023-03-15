#include "local.h"

#include "local_store.h"

#include <yt/cpp/roren/interface/roren.h>
#include <yt/cpp/roren/interface/input.h>

#include <library/cpp/yson/node/node_io.h>

#include <util/system/getpid.h>

#include <list>
#include <cmath>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

namespace NPrivate {

////////////////////////////////////////////////////////////////////////////////

void SavePtr(IOutputStream* output, void* ptr)
{
    Save(output, static_cast<i64>(GetPID()));
    Save(output, reinterpret_cast<uintptr_t>(ptr));
}

void LoadPtrImpl(IInputStream* input, void** ptr)
{
    i64 pid;
    Load(input, pid);
    Y_VERIFY(input, "Interprocess transmission is not allowed");
    uintptr_t ptrValue;
    Load(input, ptrValue);
    *ptr = reinterpret_cast<void*>(ptrValue);
}

////////////////////////////////////////////////////////////////////////////////

class TLocalExecutionContext
    : public IExecutionContext
{
public:
    TLocalExecutionContext(NYT::NProfiling::TProfiler profiler)
        : Profiler_(std::move(profiler))
    { }

    TString GetExecutorName() const final override
    {
        return "local";
    }

    NYT::NProfiling::TProfiler GetProfiler() const override
    {
        return Profiler_;
    }

private:
    NYT::NProfiling::TProfiler Profiler_;
};

////////////////////////////////////////////////////////////////////////////////

class TDecodingRawInput
    : public IRawInput
{
public:
    TDecodingRawInput(IRawCoderPtr coder, TInputPtr<TString> underlying, TRawRowHolder* row)
        : Coder_(std::move(coder))
        , Underlying_(std::move(underlying))
        , Row_(row->GetData())
    { }

    const void* NextRaw() override
    {
        const auto* bytes = Underlying_->Next();
        if (bytes == nullptr) {
            return nullptr;
        }
        Coder_->DecodeRow(*bytes, Row_);
        return Row_;
    }

private:
    const IRawCoderPtr Coder_;
    const TInputPtr<TString> Underlying_;
    void* const Row_;
};

////////////////////////////////////////////////////////////////////////////////

class TGroupedLocalInput
    : public IRawGroupedInput
{
public:
    using TGroupedMap = THashMap<TString, std::vector<std::vector<TString>>>;

public:
    TGroupedLocalInput(TRowVtable keyVtable, std::vector<TRawRowHolder> rawRowHolderList, TGroupedMap coGroupedByKey)
        : CoGroupedByKey_(std::move(coGroupedByKey))
        , RawRowHolderList_(std::move(rawRowHolderList))
        , KeyHolder_(std::move(keyVtable))
        , KeyCoder_(KeyHolder_.GetRowVtable().RawCoderFactory())
    {
        for (auto& rawRowHolder : RawRowHolderList_) {
            auto rawVectorInput = MakeIntrusive<TRawVectorIterator<TString>>(std::vector<TString>{});
            VectorInputs_.push_back(rawVectorInput);

            auto coder = rawRowHolder.GetRowVtable().RawCoderFactory();
            auto decodingRawInput = MakeIntrusive<TDecodingRawInput>(coder, TInputPtr<TString>{rawVectorInput}, &rawRowHolder);
            RawInputs_.push_back(decodingRawInput);
        }
    }

    ssize_t GetInputCount() const override
    {
        return ssize(RawInputs_);
    }

    IRawInputPtr GetInput(ssize_t idx) const override
    {
        Y_VERIFY(idx >= 0 && idx < ssize(RawInputs_));
        return RawInputs_[idx];
    }

    const void* NextKey() override
    {
        if (Current_ == CoGroupedByKey_.end()) {
            return nullptr;
        } else {
            const auto& serializedKey = Current_->first;
            KeyCoder_->DecodeRow(serializedKey, KeyHolder_.GetData());
            const auto& vectorList = Current_->second;
            Y_VERIFY(vectorList.size() == VectorInputs_.size());
            for (ssize_t i = 0; i < ssize(vectorList); ++i) {
                VectorInputs_[i]->Reset(vectorList[i]);
            }
            Current_++;
            return KeyHolder_.GetData();
        }
    }

private:
    const TGroupedMap CoGroupedByKey_;
    TGroupedMap::const_iterator Current_ = CoGroupedByKey_.begin();

    std::vector<TRawRowHolder> RawRowHolderList_;
    std::vector<::TIntrusivePtr<TRawVectorIterator<TString>>> VectorInputs_;
    std::vector<IRawInputPtr> RawInputs_;
    TRawRowHolder KeyHolder_;
    IRawCoderPtr KeyCoder_;
};


////////////////////////////////////////////////////////////////////////////////

class TLocalExecutor
    : public IExecutor
{
public:
    TLocalExecutor(NYT::NProfiling::TProfiler profiler)
        : Profiler_(std::move(profiler))
    { }

    bool EnableDefaultPipelineOptimization() const override
    {
        return true;
    }

    void Run(const TPipeline& pipeline) override
    {
        THashMap<int, TLocalStorePtr> dataNodes;
        auto executionContext = ::MakeIntrusive<TLocalExecutionContext>(Profiler_);

        auto getLocalRawDataStore = [&] (int nodeId) -> TLocalStorePtr {
            auto it = dataNodes.find(nodeId);
            Y_VERIFY(it != dataNodes.end());
            return it->second;
        };

        for (const auto& transformNode : GetRawPipeline(pipeline)->GetTransformList()) {
            const auto& transform = transformNode->GetRawTransform();

            switch (transform->GetType()) {
                case ERawTransformType::Read: {
                    Y_VERIFY(transformNode->GetSourceCount() == 0);
                    Y_VERIFY(transformNode->GetSinkCount() == 1);
                    auto sink = transformNode->GetSink(0);
                    const auto&[store, output] = MakeLocalStore(sink->GetRowVtable());
                    auto insertResult = dataNodes.emplace(sink->GetId(), store);
                    Y_VERIFY(insertResult.second);
                    ApplyRead(transform->AsRawReadRef(), output);
                    break;
                }
                case ERawTransformType::Write: {
                    Y_VERIFY(transformNode->GetSinkCount() == 0);
                    Y_VERIFY(transformNode->GetSourceCount() == 1);
                    auto source = transformNode->GetSource(0);
                    auto sourceStore = getLocalRawDataStore(source->GetId());
                    ApplyWrite(transform->AsRawWriteRef(), sourceStore);
                    break;
                }
                case ERawTransformType::ParDo: {
                    Y_VERIFY(transformNode->GetSourceCount() == 1);
                    auto input = MakeLocalStoreInput(getLocalRawDataStore(transformNode->GetSource(0)->GetId()));

                    std::vector<IRawOutputPtr> outputs;
                    for (const auto& sink : transformNode->GetSinkList()) {
                        auto[store, output] = MakeLocalStore(sink->GetRowVtable());
                        const auto&[iter, inserted] = dataNodes.emplace(sink->GetId(), store);
                        Y_VERIFY(inserted);
                        outputs.emplace_back(output);
                    }

                    ApplyParDo(transformNode->GetRawTransform()->AsRawParDoRef(), input, outputs, executionContext);
                    break;
                }
                case ERawTransformType::GroupByKey: {
                    Y_VERIFY(transformNode->GetSourceCount() == 1);
                    Y_VERIFY(transformNode->GetSinkCount() == 1);
                    auto source = transformNode->GetSource(0);
                    auto sink = transformNode->GetSink(0);

                    auto input = MakeLocalStoreInput(getLocalRawDataStore(source->GetId()));

                    const auto&[store, output] = MakeLocalStore(sink->GetRowVtable());
                    auto insertResult = dataNodes.emplace(sink->GetId(), store);
                    Y_VERIFY(insertResult.second);

                    ApplyGroupByKey(
                        transformNode->GetRawTransform()->AsRawGroupByKeyRef(),
                        source->GetRowVtable(),
                        input,
                        sink->GetRowVtable(),
                        output);
                    break;
                }
                case ERawTransformType::CombinePerKey: {
                    // TODO: unify input / output creation
                    Y_VERIFY(transformNode->GetSourceCount() == 1);
                    Y_VERIFY(transformNode->GetSinkCount() == 1);
                    auto source = transformNode->GetSource(0);
                    auto sink = transformNode->GetSink(0);

                    auto input = MakeLocalStoreInput(getLocalRawDataStore(source->GetId()));

                    const auto&[store, output] = MakeLocalStore(sink->GetRowVtable());
                    auto insertResult = dataNodes.emplace(sink->GetId(), store);
                    Y_VERIFY(insertResult.second);

                    ApplyCombineByKey(
                        transformNode->GetRawTransform()->AsRawCombineRef(),
                        source->GetRowVtable(),
                        input,
                        sink->GetRowVtable(),
                        output,
                        executionContext);
                    break;
                }
                case ERawTransformType::CoGroupByKey: {
                    std::vector<std::pair<TDynamicTypeTag, IRawInputPtr>> inputs;
                    auto inputTags = transformNode->GetRawTransform()->GetInputTags();
                    Y_VERIFY(ssize(inputTags) == transformNode->GetSourceCount());

                    for (ssize_t i = 0; i < ssize(inputTags); ++i) {
                        const auto& tag = inputTags[i];
                        const auto& source = transformNode->GetSource(i);

                        auto input = MakeLocalStoreInput(getLocalRawDataStore(source->GetId()));
                        inputs.emplace_back(tag, input);
                    }

                    Y_VERIFY(transformNode->GetSinkCount() == 1);
                    auto sink = transformNode->GetSink(0);

                    const auto&[store, output] = MakeLocalStore(sink->GetRowVtable());
                    auto insertResult = dataNodes.emplace(sink->GetId(), store);
                    Y_VERIFY(insertResult.second);

                    ApplyCoGroupByKey(transformNode->GetRawTransform()->AsRawCoGroupByKeyRef(), inputs, output);
                    break;
                }
                case ERawTransformType::Flatten: {
                    std::vector<IRawInputPtr> inputs;

                    auto sourceCount = transformNode->GetSourceCount();
                    for (ssize_t i = 0; i < sourceCount; ++i) {
                        const auto& source = transformNode->GetSource(i);
                        auto input = MakeLocalStoreInput(getLocalRawDataStore(source->GetId()));
                        inputs.emplace_back(input);
                    }

                    Y_VERIFY(transformNode->GetSinkCount() == 1);
                    auto sink = transformNode->GetSink(0);

                    const auto&[store, output] = MakeLocalStore(sink->GetRowVtable());
                    auto insertResult = dataNodes.emplace(sink->GetId(), store);
                    Y_VERIFY(insertResult.second);

                    ApplyFlatten(inputs, output);
                    break;
                }
                case ERawTransformType::CombineGlobally:
                case ERawTransformType::StatefulParDo:
                    Y_FAIL("Not implemented yet");
            }
        }
    }

private:

    // Since our local executor is intended for debug usage we save / load our transform
    // in order to make sure that serialization works properly.
    template <typename T>
    static ::TIntrusivePtr<T> DebugCopyTransform(const T& serializable)
    {
        auto state = SerializableToNode(serializable);
        return SerializableFromNode<T>(state);
    }

    static void ApplyRead(const IRawRead& originalRawRead, const IRawOutputPtr& writer)
    {
        auto rawRead = DebugCopyTransform(originalRawRead);
        while (const void* data = rawRead->NextRaw()) {
            writer->AddRaw(data, 1);
        }
        writer->Close();
    }

    static void ApplyWrite(const IRawWrite& originalRawWrite, const TLocalStorePtr& source)
    {
        auto rawWrite = DebugCopyTransform(originalRawWrite);
        auto input = MakeLocalStoreInput(source);

        while (const void* data = input->NextRaw()) {
            rawWrite->AddRaw(data, 1);
        }
        rawWrite->Close();
    }

    static void ApplyParDo(const IRawParDo& originalParDo, const IRawInputPtr& input, const std::vector<IRawOutputPtr>& outputs, const IExecutionContextPtr& executionContext)
    {
        auto parDo = DebugCopyTransform(originalParDo);

        parDo->Start(executionContext, outputs);

        while (const void* row = input->NextRaw()) {
            parDo->Do(row, 1);
        }

        parDo->Finish();

        for (const auto& o : outputs) {
            o->Close();
        }
    }

    static void ApplyGroupByKey(
        const IRawGroupByKey& rawGroupByKey,
        const TRowVtable& inputVtable,
        const IRawInputPtr& input,
        const TRowVtable& outputVtable,
        const IRawOutputPtr& output)
    {
        auto computation = DebugCopyTransform(rawGroupByKey);

        THashMap<TString, std::vector<TString>> groupBy;
        auto inputCoder = inputVtable.RawCoderFactory();
        auto [keyCoder, valueCoder] = inputCoder->UnpackKV();

        while (auto row = input->NextRaw()) {
            TString keyBytes, allBytes;
            TStringOutput keyStream(keyBytes);
            TStringOutput allStream(allBytes);
            keyCoder->EncodeRow(&keyStream, reinterpret_cast<const char*>(row) + inputVtable.KeyOffset);
            inputCoder->EncodeRow(&allStream, row);
            groupBy[keyBytes].push_back(allBytes);
        }

        TRawRowHolder ungroupedRow(inputVtable);
        TRawRowHolder groupedRow(outputVtable);
        for (auto& [keyBytes, valueByteList] : groupBy) {
            auto codedStringInput = TInputPtr<TString>{::MakeIntrusive<TRawVectorIterator<TString>>(std::move(valueByteList))};
            auto rawInput = ::MakeIntrusive<TDecodingRawInput>(inputCoder, codedStringInput, &ungroupedRow);
            computation->ProcessOneGroup(rawInput.Get(), output);
        }
        output->Close();
    }

    static void ApplyCombineByKey(
        const IRawCombine& combine,
        const TRowVtable& inputVtable,
        const IRawInputPtr& input,
        const TRowVtable& outputVtable,
        const IRawOutputPtr& output,
        const IExecutionContextPtr& executionContext)
    {
        auto computationState = SerializableToNode(combine);
        auto computation = SerializableFromNode<IRawCombine>(computationState);
        computation->Start(executionContext);
        const auto accumVtable = computation->GetAccumVtable();
        const auto accumCoder = computation->GetAccumCoder();
        const auto keyVtable = (*inputVtable.KeyVtableFactory)();

        THashMap<TString, std::vector<TString>> groupBy;
        auto inputCoder = inputVtable.RawCoderFactory();
        auto [keyCoder, valueCoder] = inputCoder->UnpackKV();

        while (auto row = input->NextRaw()) {
            TString keyBytes, allBytes;
            TStringOutput keyStream(keyBytes);
            TStringOutput allStream(allBytes);
            keyCoder->EncodeRow(&keyStream, reinterpret_cast<const char*>(row) + inputVtable.KeyOffset);
            inputCoder->EncodeRow(&allStream, row);
            groupBy[keyBytes].push_back(allBytes);
        }

        TRawRowHolder inputRow(inputVtable);
        TRawRowHolder combinedRow(outputVtable);
        for (auto& [keyBytes, rowBytes] : groupBy) {
            const auto batchStep = static_cast<ssize_t>(std::sqrt(rowBytes.size()));
            std::vector<TString> serializedAccumValues;
            for (ssize_t batchBegin = 0; batchBegin < std::ssize(rowBytes); batchBegin += batchStep) {
                auto batchEnd = std::min<ssize_t>(batchBegin + batchStep, std::ssize(rowBytes));
                TRawRowHolder accum(accumVtable);
                computation->CreateAccumulator(accum.GetData());
                for (auto i = batchBegin; i < batchEnd; ++i) {
                    inputCoder->DecodeRow(rowBytes[i], inputRow.GetData());
                    computation->AddInput(accum.GetData(), inputRow.GetValueOfKV());
                }
                TStringStream accumBytes;
                accumCoder->EncodeRow(&accumBytes, accum.GetData());
                serializedAccumValues.push_back(accumBytes.Str());
            }

            auto codedAccumInput = TInputPtr<TString>{::MakeIntrusive<TRawVectorIterator<TString>>(std::move(serializedAccumValues))};

            TRawRowHolder iterSlot(accumVtable);
            auto rawInput = ::MakeIntrusive<TDecodingRawInput>(accumCoder, codedAccumInput, &iterSlot);

            TRawRowHolder mergedAccum(accumVtable);
            computation->CreateAccumulator(mergedAccum.GetData());
            computation->MergeAccumulators(mergedAccum.GetData(), rawInput.Get());

            keyCoder->DecodeRow(keyBytes, combinedRow.GetKeyOfKV());
            computation->ExtractOutput(combinedRow.GetValueOfKV(), mergedAccum.GetData());
            output->AddRaw(combinedRow.GetData(), 1);
        }
        output->Close();
    }

    static void ApplyCoGroupByKey(
        const IRawCoGroupByKey& rawCoGroupByKey,
        std::vector<std::pair<TDynamicTypeTag, IRawInputPtr>>& taggedInputs,
        const IRawOutputPtr& output)
    {
        auto computation = DebugCopyTransform(rawCoGroupByKey);

        THashMap<TString, std::vector<std::vector<TString>>> coGroupedBy;

        const auto inputCount = taggedInputs.size();
        TRowVtable keyVtable;

        for (ssize_t inputIndex = 0; inputIndex < ssize(taggedInputs); ++inputIndex) {
            const auto& [inputTag, input] = taggedInputs[inputIndex];
            const auto& inputVtable = inputTag.GetRowVtable();
            if (!IsDefined(keyVtable)) {
                keyVtable = inputVtable.KeyVtableFactory();
            }

            auto inputCoder = inputVtable.RawCoderFactory();
            const auto& [keyCoder, valueCoder] = inputCoder->UnpackKV();

            while (auto row = input->NextRaw()) {
                TString keyBytes, allBytes;
                TStringOutput keyStream(keyBytes);
                TStringOutput allStream(allBytes);

                keyCoder->EncodeRow(&keyStream, reinterpret_cast<const char*>(row) + inputVtable.KeyOffset);
                inputCoder->EncodeRow(&allStream, row);
                auto& values = coGroupedBy[keyBytes];
                values.resize(inputCount);
                values[inputIndex].push_back(allBytes);
            }
        }

        if (!taggedInputs.empty()) {
            Y_VERIFY(IsDefined(keyVtable));

            std::vector<TRawRowHolder> rowHolders;
            for (const auto& [tag, input] : taggedInputs) {
                rowHolders.emplace_back(tag.GetRowVtable());
            }

            auto groupedLocalInput = ::MakeIntrusive<TGroupedLocalInput>(keyVtable, std::move(rowHolders), std::move(coGroupedBy));

            std::vector<std::pair<TDynamicTypeTag, IRawInputPtr>> taggedGroupedInputs;
            Y_VERIFY(groupedLocalInput->GetInputCount() == ssize(taggedInputs));

            for (ssize_t i = 0; i < ssize(taggedInputs); ++i) {
                const auto& tag = taggedInputs[i].first;
                auto input = groupedLocalInput->GetInput(i);
                taggedGroupedInputs.emplace_back(tag, input);
            }

            auto coGbkResult = MakeCoGbkResult(TRawRowHolder{keyVtable}, std::move(taggedGroupedInputs));
            while (const void* key = groupedLocalInput->NextKey()) {
                SetKey(coGbkResult, key);
                output->AddRaw(&coGbkResult, 1);
            }
        }
        output->Close();
    }

    static void ApplyFlatten(
        const std::vector<IRawInputPtr>& inputList,
        const IRawOutputPtr& output)
    {
        for (const auto& input : inputList) {
            while (auto row = input->NextRaw()) {
                output->AddRaw(row, 1);
            }
        }
        output->Close();
    }

    NYT::NProfiling::TProfiler Profiler_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NPrivate

////////////////////////////////////////////////////////////////////////////////

TPipeline MakeLocalPipeline(NYT::NProfiling::TProfiler profiler)
{
    return NPrivate::MakePipeline(::MakeIntrusive<NPrivate::TLocalExecutor>(std::move(profiler)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
