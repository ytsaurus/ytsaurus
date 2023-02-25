#include <yt/cpp/mapreduce/interface/client.h>

#include <util/stream/output.h>
#include <util/system/user.h>

using namespace NYT;

class TSplitHumanRobotsReduce
    : public IReducer<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    void Do(TReader* reader, TWriter* writer) override {
        TNode loginRow;
        bool isRobot = false;
        for (auto& cursor : *reader) {
            const auto& curRow = cursor.GetRow();

            auto tableIndex = cursor.GetTableIndex();
            if (tableIndex == 0) {
                loginRow = curRow;
            } else if (tableIndex == 1) {
                isRobot = curRow["is_robot"].AsBool();
            } else {
                Y_FAIL();
            }
        }

        // Второй аргумент метода `AddRow' указывает, в какую таблицу будет записано значение.
        if (isRobot) {
            writer->AddRow(loginRow, 0);
        } else {
            writer->AddRow(loginRow, 1);
        }
    }
};
REGISTER_REDUCER(TSplitHumanRobotsReduce)
