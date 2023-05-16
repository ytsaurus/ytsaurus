#include <yt/cpp/mapreduce/examples/tutorial/prepare_operation/grepper.pb.h>

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/config.h>

#include <util/stream/output.h>
#include <util/system/user.h>

using namespace NYT;

// Этот маппер отдаёт все строки, значение в колонке `column` которых равно `pattern`.
class TGrepper
    : public IMapper<TTableReader<TGrepperRecord>, TTableWriter<TGrepperRecord>>
{
public:
    TGrepper() = default;
    TGrepper(TString column, TString pattern)
        : Column_(column)
        , Pattern_(pattern)
    { }

    void Do(TReader* reader, TWriter* writer) override
    {
        for (const auto& cursor : *reader) {
            auto row = cursor.GetRow();
            if (row.GetKey() == Pattern_) {
                writer->AddRow(row);
            }
        }
    }

    void PrepareOperation(const IOperationPreparationContext& context, TJobOperationPreparer& preparer) const override
    {
        auto schema = context.GetInputSchema(/* tableIndex */ 0);
        // Выходная схема совпадает со входной во всём, кроме названия колонки, по которой ищем.
        for (auto& column : schema.MutableColumns()) {
            if (column.Name() == Column_) {
                column.Name("Key");
            }
        }
        preparer
            .InputColumnRenaming(/* tableIndex */ 0, {{Column_, "Key"}})
            .InputDescription<TGrepperRecord>(/* tableIndex */ 0)
            // Выключаем автовывод схемы, т.к. он ничего не даст (у нас есть поле, помеченное OTHER_COLUMNS).
            .OutputDescription<TGrepperRecord>(/* tableIndex */ 0, /* inferSchema */ false)
            .OutputSchema(/* tableIndex */ 0, schema);
    }

    Y_SAVELOAD_JOB(Column_, Pattern_);

private:
    TString Column_;
    TString Pattern_;
};
REGISTER_MAPPER(TGrepper)

int main() {
    NYT::Initialize();

    auto client = CreateClient("freud");

    // Выходная табличка у нас будет лежать в tmp и содержать имя текущего пользователя.
    const TString outputTable = "//tmp/" + GetUsername() + "-tutorial-grepped-emails";

    client->Map(
        new TGrepper("login", "lev"),
        "//home/dev/tutorial/staff_unsorted_schematized",
        outputTable);

    Cout << "Output table: https://yt.yandex-team.ru/freud/#page=navigation&offsetMode=row&path=" << outputTable << Endl;

    return 0;
}
