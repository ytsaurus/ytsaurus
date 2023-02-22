#include <mapreduce/yt/examples/python-tutorial/cpp_multiple_input_multiple_output_reduce_protobuf/data.pb.h>

#include <mapreduce/yt/interface/client.h>
#include <mapreduce/yt/interface/config.h>

#include <util/stream/output.h>
#include <util/system/user.h>

using namespace NYT;

class TSplitHumanRobotsReduce
    // Обратите внимание наш редьюс работает с несколькими типами записей
    // как на вход так и на выход, поэтому мы используем ::google::protobuf::Message
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

    // Для protobuf джобов PrepareOperation() обязателен.
    // Несмотря на то, что мы использовали ::google::protobuf::Message,
    // здесь необходимо указать конкретный тип для каждой таблицы.
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
REGISTER_REDUCER(TSplitHumanRobotsReduce)
