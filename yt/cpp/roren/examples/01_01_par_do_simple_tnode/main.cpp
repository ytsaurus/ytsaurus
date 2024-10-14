// Этот пример можно собрать и запустить.

#include <yt/cpp/roren/interface/roren.h>
#include <yt/cpp/roren/yt/yt.h>

#include <util/system/user.h>

// Объявляем структуры для промежуточных данных.
// Тут можем использовать произвольные структуры, главное, чтобы у них были определена аркадийная сериализация.
// Самый простой способ её определить - использовать макрос Y_SAVELOAD_DEFINE.
struct TUserLoginInfo
{
    TString Name;
    TString Login;

    Y_SAVELOAD_DEFINE(Name, Login);
};

struct TUserEmailInfo
{
    TString Name;
    TString Email;

    Y_SAVELOAD_DEFINE(Name, Email);
};

int main() {
    // Первым делом, нам необходимо заклинание, для того чтобы иметь возможность запускать ытёвые джобы.
    NYT::Initialize();

    // Создаём пайплайн. В качестве аргументов указываем кластер и директорию, где Roren будет хранить промежуточные данные.
    // Для production процессов рекомендуется использовать временную директорию внутри своей квоты. Для экспериментов можно использовать `//tmp`.
    auto pipeline = NRoren::MakeYtPipeline("freud", "//tmp");

    // Путь к выходной таблице будет содержать имя текущего пользователя (нужно, чтобы этот пример могли запускать одновременно несколько людей)
    const TString outputTable = "//tmp/" + GetUsername() + "-roren-tutorial-output";

    // Дальше мы собственно начинаем создавать пайплайн и добавлять в него операции.
    // В данном простом случае пайплайн прямолинеен и делается в одно выражение.
    //
    // Никакие вычисления не запускаются до вызова `pipeline.Run()`.
    //
    // Пайплайн в данном случае многословнее, чем мог бы быть,
    // многословность обусловлена желанием проиллюстрировать побольше возможностей.
    pipeline
        // Читаем входную таблицу
        | NRoren::YtRead<NYT::TNode>("//home/ermolovd/yt-tutorial/staff_unsorted")

        // Первым делом парсим данные во внутренний формат.
        // Делаем это с помощью простой лямбда функции, принимающей и возвращающей одно значение.
        // В лямбда функциях нельзя использовать непустой capture-list.
        | NRoren::ParDo([] (const NYT::TNode& node) -> TUserLoginInfo {
            TUserLoginInfo info;
            info.Name = node["name"].AsString();
            info.Login = node["login"].AsString();
            return info;
        })

        // Дальше идёт пользовательская логика. Здесь у нас отображение не один к одному,
        // поэтому нам нужна другая сигнатура функции.
        | NRoren::ParDo([] (const TUserLoginInfo& info, NRoren::TOutput<TUserEmailInfo>& output) {
            if (info.Name.empty()) {
                return;
            }
            TUserEmailInfo result;
            result.Name = info.Name;
            result.Email = info.Login + "@yandex-team.ru";
            output.Add(result);
        })

        // Преобразуем данные обратно в TNode, чтобы записать их в таблицу.
        | NRoren::ParDo([] (const TUserEmailInfo& info) {
            NYT::TNode result;
            result["name"] = info.Name;
            result["email"] = info.Email;
            return result;
        })

        // Финально записываем данные в таблицу, указывая путь к таблице и её схему.
        | NRoren::YtWrite(
            outputTable,
            NYT::TTableSchema()
                .AddColumn("name", NTi::String())
                .AddColumn("email", NTi::String())
        );

    pipeline.Run();

    Cout << "Output table: https://yt.yandex-team.ru/freud/#page=navigation&offsetMode=row&path=" << outputTable << Endl;

    return 0;
}
