#include <yt/cpp/mapreduce/interface/client.h>

#include <yt/cpp/mapreduce/util/wait_for_tablets_state.h> // for WaitForTabletsState

#include <util/stream/output.h>

using namespace NYT;

int main(int argc, const char** argv) {
    NYT::Initialize();

    //
    // Программа принимает аргументами имя кластера и путь к таблице, с которой она будет работать (таблица не должна существовать).
    //
    // По умолчанию у пользователей нет прав монтировать динамические таблицы, и перед запуском программы необходимо получить
    // такие права (права на монтирование таблиц) на каком-нибудь кластере YT.
    if (argc != 3) {
        Cerr << "Usage:\n";
        Cerr << '\t' << argv[0] << " <server-name> <path-to-dynamic-table>\n";
        Cerr << '\n';
        Cerr << "For example:\n";
        Cerr << '\t' << argv[0] << " freud //home/ermolovd/test-dyntable" << Endl;
        return 1;
    }

    auto client = CreateClient(argv[1]);
    const TString dynamicTablePath = argv[2];

    auto schema = TTableSchema()
        .AddColumn("key", EValueType::VT_INT64, ESortOrder::SO_ASCENDING)
        .AddColumn("value", EValueType::VT_STRING);

    // ВАЖНО: при создании динамической таблицы нам нужно
    //  - указать атрибут dynamic: true
    //  - указать схему
    // Это нужно сделать обязательно в вызове Create. Т.е. не получится сначала создать таблицу,
    // а потом проставить эти атрибуты.
    client->Create(
        dynamicTablePath, NT_TABLE,
        TCreateOptions()
        .Force(true)
        .Attributes(
            TNode()
                ("dynamic", true)
                ("schema", schema.ToNode())));

    // Для того чтобы начать работу с динамической таблицей, её необходимо "подмонтировать".
    //
    // Часто создание / монтирование / размонтирование таблиц делается отдельными административными скриптами,
    // а приложение просто расчитывает, что таблицы существуют и уже подмонтированы.
    //
    // Мы для полноты примера подмонтируем таблицу, а в конце её размонтируем.
    client->MountTable(dynamicTablePath);
    Cout << "Waiting tablets are mounted..." << Endl;

    // Функция MountTable (и UnmountTable) запускает асинхронный процесс монтирования,
    // который может занять значительное время (десятки секунд) для больших таблиц.
    // Нам необходимо дождаться завершения этого процесса.
    WaitForTabletsState(client, dynamicTablePath, TS_MOUNTED);

    // Вставлять значения в динтаблицу можно с помощью InsertRows
    client->InsertRows(dynamicTablePath, {
        TNode()("key", 1)("value", "один"),
        TNode()("key", 42)("value", "сорок два"),
        TNode()("key", 100500)("value", "стопятьсот"),
    });

    // Получать значения из динтаблицы можно с помощью LookupRows,
    // возвращает список найденных строчек.
    auto result = client->LookupRows(dynamicTablePath, {
        TNode()("key", 100500),
        TNode()("key", 42),
    });

    Cout << "====== LOOKUP RESULT ======" << Endl;
    for (const auto& row : result) {
        Cout << "key: " << row["key"].AsInt64() << " value: " << row["value"].AsString() << Endl;
    }
    Cout << "====== END LOOKUP RESULT ======" << Endl;

    client->UnmountTable(dynamicTablePath);
    Cout << "Waiting tablets are unmounted..." << Endl;
    WaitForTabletsState(client, dynamicTablePath, TS_UNMOUNTED);

    return 0;
}
