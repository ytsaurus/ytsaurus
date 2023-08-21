#include <yt/cpp/mapreduce/interface/client.h>

#include <util/system/user.h>

using namespace NYT;

int main() {
    NYT::Initialize();

    auto client = CreateClient("freud");

    // Создаём batch запрос (это можно делать и из транзакции).
    auto request = client->CreateBatchRequest();

    // Добавляем запросы в batch
    NThreading::TFuture<bool> docTitleExists = request->Exists("//home/dev/tutorial/doc_title");
    NThreading::TFuture<bool> unexistingTableExists = request->Exists("//home/dev/tutorial/unexisting_table");

    const TString outputTable = "//tmp/" + GetUsername() + "-tutorial-test-batch";
    NThreading::TFuture<TNodeId> createResult = request->Create(outputTable, NT_TABLE);

    // Выполняем batch запрос.
    request->ExecuteBatch();

    // Проверяем результаты.
    Cout << "Table //home/dev/tutorial/doc_title exists: " << docTitleExists.GetValue() << Endl;
    Cout << "Table //home/dev/tutorial/unexisting_table exists: " << unexistingTableExists.GetValue() << Endl;

    try {
        // Следует проверять все результаты с помощью GetValue(),
        // т.к. отдельные запросы могут пофейлиться и тогда соответствующая TFuture будет содержать ошибку.
        //
        // Если запускать эту программу второй раз то Create пофейлится, потому что таблица уже существует.
        createResult.GetValue();
    } catch (const std::exception& ex) {
        Cerr << "Create " << outputTable << " failed: " << ex.what() << Endl;
    }

    return 0;
}
