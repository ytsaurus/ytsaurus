#include <yt/python/yt/cpp_wrapper/tests/data.pb.h>
#include <mapreduce/yt/interface/client.h>

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
