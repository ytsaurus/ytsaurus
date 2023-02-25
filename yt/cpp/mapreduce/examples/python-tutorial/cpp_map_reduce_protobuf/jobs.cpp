#include <yt/cpp/mapreduce/examples/python-tutorial/cpp_map_reduce_protobuf/data.pb.h>

#include <yt/cpp/mapreduce/interface/client.h>

#include <util/charset/utf8.h>

using namespace NYT;

class TNormalizeNameMapper
    : public IMapper<TTableReader<TLoginRecord>, TTableWriter<::google::protobuf::Message>>
{
public:
    Y_SAVELOAD_JOB(PrefixFilter_); // Заклинание, которое говорит, какие переменные нужно передавать на сервер.

    // Этот метод необходим, чтобы уметь конструировать маппер из произвольного
    // YSON-serializable объекта в питоне.
    // Инициализация объекта произойдет локально, на сервере будут восстановлены
    // только переменные из Y_SAVELOAD_JOB (Pattern_ и MaxDistance_).
    static ::TIntrusivePtr<IMapper> FromNode(const TNode& node)
    {
        auto result = MakeIntrusive<TNormalizeNameMapper>();
        result->PrefixFilter_ = node.AsString();
        return result;
    }

    void Do(TReader* reader, TWriter* writer) override
    {
        for (auto& cursor : *reader) {
            const auto& row = cursor.GetRow();
            auto normalizedName = ToLowerUTF8(row.GetName());

            TLoginRecord outRow;
            outRow.SetName(normalizedName);
            writer->AddRow(outRow, 0);

            if (normalizedName.substr(0, PrefixFilter_.size()) == PrefixFilter_) {
                TNameUidRecord outRow;
                outRow.SetName(normalizedName);
                outRow.SetUid(row.GetUid());
                writer->AddRow(outRow, 1);
            }
        }
    }

    void PrepareOperation(const IOperationPreparationContext& context, TJobOperationPreparer& preparer) const override
    {
        Y_UNUSED(context);
        preparer
            .InputDescription<TLoginRecord>(/* tableIndex */ 0)
            .OutputDescription<TLoginRecord>(/* tableIndex */ 0)
            .OutputDescription<TNameUidRecord>(/* tableIndex */ 1);
    }
private:
    TString PrefixFilter_;
};
REGISTER_MAPPER(TNormalizeNameMapper);

class TCountNamesReducer
    : public IReducer<TTableReader<TLoginRecord>, TTableWriter<TNameCountRecord>>
{
public:
    void Do(TReader* reader, TWriter* writer) override {
        int count = 0;
        TString name;
        for (auto& cursor : *reader) {
            name = cursor.GetRow().GetName();
            ++count;
        }
        TNameCountRecord outRow;
        outRow.SetName(name);
        outRow.SetCount(count);

        writer->AddRow(outRow);
    }

    void PrepareOperation(const IOperationPreparationContext& context, TJobOperationPreparer& preparer) const override
    {
        Y_UNUSED(context);
        preparer
            .InputDescription<TLoginRecord>(/* tableIndex */ 0)
            .OutputDescription<TNameCountRecord>(/* tableIndex */ 0);
    }
};
REGISTER_REDUCER(TCountNamesReducer);
