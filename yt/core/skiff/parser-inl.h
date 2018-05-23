#pragma once
#ifndef PARSER_INL_H_
#error "Direct inclusion of this file is not allowed, include parser.h"
#endif

#include "skiff.h"

#include <yt/core/concurrency/coroutine.h>

namespace NYT {
namespace NSkiff {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TCoroStream
    : public IZeroCopyInput
{
public:
    TCoroStream(TStringBuf data, TCoroutine<void(TStringBuf)>* coroutine)
        : Coroutine_(coroutine)
        , PendingData_(data)
        , Finished_(data.empty())
    { }

    size_t DoNext(const void** ptr, size_t len) override
    {
        if (PendingData_.empty()) {
            if (Finished_) {
                *ptr = nullptr;
                return 0;
            }
            std::tie(PendingData_) = Coroutine_->Yield();
            if (PendingData_.Empty()) {
                Finished_ = true;
                *ptr = nullptr;
                return 0;
            }
        }
        *ptr = PendingData_.Data();
        len = Min(len, PendingData_.Size());
        PendingData_.Skip(len);
        return len;
    }

    void Complete()
    {
        if (!Finished_) {
            const void* ptr;
            if (!PendingData_.Empty() || DoNext(&ptr, 1)) {
                THROW_ERROR_EXCEPTION("Stray data in stream");
            }
        }
    }

private:
    TCoroutine<void(TStringBuf)>* const Coroutine_;
    TStringBuf PendingData_;
    bool Finished_ = false;
};

////////////////////////////////////////////////////////////////////////////////

template <class TConsumer>
class TSkiffMultiTableStreamParserImpl
{
public:
    TSkiffMultiTableStreamParserImpl(
        IInputStream* input,
        TConsumer* consumer,
        const TSkiffSchemaList& skiffSchemaList,
        const std::vector<TSkiffTableColumnIds>& tablesColumnIds,
        const TString& rangeIndexColumnName,
        const TString& rowIndexColumnName)
        : Consumer_(consumer)
        , Parser_(CreateVariant16Schema(skiffSchemaList), input)
        , TablesColumnIds(tablesColumnIds)
    {
        TableDescriptions_ = CreateTableDescriptionList(skiffSchemaList, rangeIndexColumnName, rowIndexColumnName);

        YCHECK(tablesColumnIds.size() == TableDescriptions_.size());
        for (size_t index = 0; index < TableDescriptions_.size(); ++index) {
            YCHECK(tablesColumnIds[index].DenseFieldColumnIds.size() == TableDescriptions_[index].DenseFieldDescriptionList.size());
            YCHECK(tablesColumnIds[index].SparseFieldColumnIds.size() == TableDescriptions_[index].SparseFieldDescriptionList.size());
        }
    }

    Y_FORCE_INLINE void ParseField(ui16 columnId, const TString& name, EWireType wireType, bool required = false)
    {
        if (!required) {
            ui8 tag = Parser_.ParseVariant8Tag();
            if (tag == 0) {
                Consumer_->OnEntity(columnId);
                return;
            } else if (tag > 1) {
                THROW_ERROR_EXCEPTION(
                    "Found bad variant8 tag %Qv when parsing optional field %Qv",
                    tag,
                    name);
            }
        }
        switch (wireType) {
            case EWireType::Yson32:
                Parser_.ParseYson32(&String_);
                Consumer_->OnYsonString(String_, columnId);
                break;
            case EWireType::Int64:
                Consumer_->OnInt64Scalar(Parser_.ParseInt64(), columnId);
                break;
            case EWireType::Uint64:
                Consumer_->OnUint64Scalar(Parser_.ParseUint64(), columnId);
                break;
            case EWireType::Double:
                Consumer_->OnDoubleScalar(Parser_.ParseDouble(), columnId);
                break;
            case EWireType::Boolean:
                Consumer_->OnBooleanScalar(Parser_.ParseBoolean(), columnId);
                break;
            case EWireType::String32:
                Parser_.ParseString32(&String_);
                Consumer_->OnStringScalar(String_, columnId);
                break;
            default:
                // Other types should be filtered out when we parsed skiff schema
                Y_UNREACHABLE();
        }
    }

    void Run()
    {
        while (Parser_.HasMoreData()) {
            auto tag = Parser_.ParseVariant16Tag();
            if (tag >= TableDescriptions_.size()) {
                THROW_ERROR_EXCEPTION("Unknown table index varint16 tag %v",
                    tag);
            }

            Consumer_->OnBeginRow(tag);

            for (ui16 i = 0; i < TableDescriptions_[tag].DenseFieldDescriptionList.size(); ++i) {
                const auto& field = TableDescriptions_[tag].DenseFieldDescriptionList[i];
                auto columnId = TablesColumnIds[tag].DenseFieldColumnIds[i];
                ParseField(columnId, field.Name, field.DeoptionalizedSchema->GetWireType(), field.Required);
            }

            if (!TableDescriptions_[tag].SparseFieldDescriptionList.empty()) {
                for (auto sparseFieldIdx = Parser_.ParseVariant16Tag();
                    sparseFieldIdx != EndOfSequenceTag<ui16>();
                    sparseFieldIdx = Parser_.ParseVariant16Tag())
                {
                    if (sparseFieldIdx >= TableDescriptions_[tag].SparseFieldDescriptionList.size()) {
                        THROW_ERROR_EXCEPTION("Bad sparse field index %Qv, total sparse field count %Qv",
                            sparseFieldIdx,
                            TableDescriptions_[tag].SparseFieldDescriptionList.size());
                    }

                    const auto& field = TableDescriptions_[tag].SparseFieldDescriptionList[sparseFieldIdx];
                    auto columnId = TablesColumnIds[tag].SparseFieldColumnIds[sparseFieldIdx];
                    ParseField(columnId, field.Name, field.DeoptionalizedSchema->GetWireType(), true);
                }
            }

            if (TableDescriptions_[tag].HasOtherColumns) {
                Parser_.ParseYson32(&String_);
                Consumer_->OnOtherColumns(String_);
            }

            Consumer_->OnEndRow();
        }
    }

private:
    TConsumer* const Consumer_;
    TCheckedInDebugSkiffParser Parser_;

    const std::vector<TSkiffTableColumnIds> TablesColumnIds;
    std::vector<TSkiffTableDescription> TableDescriptions_;

    // String that we parse string32 into.
    TString String_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TConsumer>
TSkiffMultiTableParser<TConsumer>::TSkiffMultiTableParser(
    TConsumer* consumer,
    TSkiffSchemaList schemaList,
    const std::vector<TSkiffTableColumnIds>& tablesColumnIds,
    const TString& rangeIndexColumnName,
    const TString& rowIndexColumnName)
    : ParserCoroutine_(BIND(
        [=] (TParserCoroutine& self, TStringBuf data) {
            TCoroStream stream(data, &self);
            TSkiffMultiTableStreamParserImpl<TConsumer> parser(
                &stream,
                consumer,
                schemaList,
                tablesColumnIds,
                rangeIndexColumnName,
                rowIndexColumnName);
            parser.Run();
            stream.Complete();
        }))
{ }

template <class TConsumer>
TSkiffMultiTableParser<TConsumer>::~TSkiffMultiTableParser()
{ }

template <class TConsumer>
void TSkiffMultiTableParser<TConsumer>::Read(TStringBuf data)
{
    if (!ParserCoroutine_.IsCompleted()) {
        ParserCoroutine_.Run(data);
    } else {
        THROW_ERROR_EXCEPTION("Input is already parsed");
    }
}

template <class TConsumer>
void TSkiffMultiTableParser<TConsumer>::Finish()
{
    Read(TStringBuf());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkiff
} // namespace NYT
