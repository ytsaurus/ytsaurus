#include <yt/cpp/mapreduce/examples/python-tutorial/cpp_simple_map_protobuf/data.pb.h>

#include <yt/cpp/mapreduce/interface/operation.h>

using namespace NYT;

class TComputeEmailsMapper
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

    // Для protobuf джобов PrepareOperation() обязателен.
    // Здесь нужно для каждой входной и выходной таблицы указать их типы.
    void PrepareOperation(const IOperationPreparationContext& context, TJobOperationPreparer& preparer) const override
    {
        Y_UNUSED(context);
        preparer
            .InputDescription<TLoginRecord>(/* tableIndex */ 0)
            .OutputDescription<TEmailRecord>(/* tableIndex */ 0);
    }
};
REGISTER_MAPPER(TComputeEmailsMapper);
