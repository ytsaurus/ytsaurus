#include <yt/python/yt/cpp_wrapper/tests/data.pb.h>

#include <yt/cpp/mapreduce/interface/client.h>

#include <util/charset/utf8.h>

using namespace NYT;

class TStatelessIncrementingMapper
    : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    void Do(TReader* reader, TWriter* writer) override
    {
        for (auto& cursor : *reader) {
            const auto& row = cursor.GetRow();

            TNode outRow;
            outRow["x"] = row["x"].AsInt64() + 1;

            writer->AddRow(outRow);
        }
    }
};
REGISTER_MAPPER(TStatelessIncrementingMapper);

class TStatefulIncrementingMapper
    : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    Y_SAVELOAD_JOB(Add_);

    static TIntrusivePtr<IMapper> FromNode(const TNode& node)
    {
        auto result = MakeIntrusive<TStatefulIncrementingMapper>();
        switch (node.GetType()) {
            case TNode::Int64: {
                result->Add_ = node.AsInt64();
                break;
            }
            case TNode::Null: {
                result->Add_ = 0;
                break;
            }
            case TNode::Map: {
                result->Add_ = node["add"].AsInt64();
                break;
            }
            default: {
                Y_FAIL();
            }
        }
        return result;
    }

    void Do(TReader* reader, TWriter* writer) override
    {
        for (auto& cursor : *reader) {
            const auto& row = cursor.GetRow();

            TNode outRow;
            outRow["x"] = row["x"].AsInt64() + Add_;

            writer->AddRow(outRow);
        }
    }

private:
    int Add_ = 1;
};
REGISTER_MAPPER(TStatefulIncrementingMapper);

class TComputeEmailsProtoMapper
    : public IMapper<TTableReader<TLoginRecord>, TTableWriter<TEmailRecord>>
{
public:
    void Do(TReader* reader, TWriter* writer) override
    {
        for (auto& cursor : *reader) {
            const auto& loginRecord = cursor.GetRow();

            TEmailRecord emailRecord;
            emailRecord.SetName(loginRecord.GetName());
            emailRecord.SetEmail(loginRecord.GetLogin() + "@yandex-team.ru");

            writer->AddRow(emailRecord);
        }
    }

    void PrepareOperation(const IOperationPreparationContext& context, TJobOperationPreparer& preparer) const override
    {
        Y_UNUSED(context);
        preparer
            .InputDescription<TLoginRecord>(/* tableIndex */ 0)
            .OutputDescription<TEmailRecord>(/* tableIndex */ 0);
    }
};
REGISTER_MAPPER(TComputeEmailsProtoMapper);

class TProtoMapperWithoutPrepareOperation
    : public IMapper<TTableReader<TLoginRecord>, TTableWriter<TEmailRecord>>
{
    void Do(TReader* /* reader */, TWriter* /* writer */) override
    { }
};
REGISTER_MAPPER(TProtoMapperWithoutPrepareOperation);

class TSplitHumanRobotsReduce
    : public IReducer<
          TTableReader<::google::protobuf::Message>,
          TTableWriter<::google::protobuf::Message>>
{
public:
    void Do(TReader* reader, TWriter* writer) override {
        TUserRecord userRecord;
        bool isRobot = false;
        for (auto& cursor : *reader) {
            auto tableIndex = cursor.GetTableIndex();
            if (tableIndex == 0) {
                userRecord = cursor.GetRow<TUserRecord>();
            } else if (tableIndex == 1) {
                const auto& isRobotRecord = cursor.GetRow<TIsRobotRecord>();
                isRobot = isRobotRecord.GetIsRobot();
            } else {
                Y_FAIL();
            }
        }

        if (isRobot) {
            TRobotRecord robotRecord;
            robotRecord.SetUid(userRecord.GetUid());
            robotRecord.SetLogin(userRecord.GetLogin());
            writer->AddRow(robotRecord, 0);
        } else {
            THumanRecord humanRecord;
            humanRecord.SetName(userRecord.GetName());
            humanRecord.SetLogin(userRecord.GetLogin());
            humanRecord.SetEmail(userRecord.GetLogin() + "@yandex-team.ru");
            writer->AddRow(humanRecord, 1);
        }
    }

    void PrepareOperation(const IOperationPreparationContext& context, TJobOperationPreparer& preparer) const override
    {
        Y_UNUSED(context);
        preparer
            .InputDescription<TUserRecord>(0)
            .InputDescription<TIsRobotRecord>(1)
            .OutputDescription<TRobotRecord>(0)
            .OutputDescription<THumanRecord>(1);
    }
};
REGISTER_REDUCER(TSplitHumanRobotsReduce);

class TNormalizeNameMapper
    : public IMapper<TTableReader<TUserRecord>, TTableWriter<::google::protobuf::Message>>
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

            TUserRecord outRow;
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
            .InputDescription<TUserRecord>(/* tableIndex */ 0)
            .OutputDescription<TUserRecord>(/* tableIndex */ 0)
            .OutputDescription<TNameUidRecord>(/* tableIndex */ 1);
    }
private:
    TString PrefixFilter_;
};
REGISTER_MAPPER(TNormalizeNameMapper);

class TCountNamesReducer
    : public IReducer<TTableReader<TUserRecord>, TTableWriter<TNameCountRecord>>
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
            .InputDescription<TUserRecord>(/* tableIndex */ 0)
            .OutputDescription<TNameCountRecord>(/* tableIndex */ 0);
    }
};
REGISTER_REDUCER(TCountNamesReducer);

class TTableAndRowIndexMapper
    : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    void Do(TReader* reader, TWriter* writer) override {
        for (auto& cursor : *reader) {
            const auto& row = cursor.GetRow();

            TNode outRow = row;
            outRow["row_index"] = cursor.GetRowIndex();
            outRow["table_index"] = cursor.GetTableIndex();
            outRow["range_index"] = cursor.GetRangeIndex();

            writer->AddRow(outRow);
        }
    }
};
REGISTER_MAPPER(TTableAndRowIndexMapper);
