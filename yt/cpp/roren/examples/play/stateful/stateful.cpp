#include <yt/cpp/roren/interface/fns.h>
#include <yt/cpp/roren/interface/private/row_vtable.h>
#include <yt/cpp/roren/interface/roren.h>

#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <util/generic/array_ref.h>


using namespace NRoren;
using namespace NYT::NTableClient;

struct TState
{
    int UserCount = 0;
};

struct TInRow
{
public:

};

struct TOutRow
{

};

TOutRow F(TState&, const TInRow&);
void Cleanup(std::vector<TState*>&);

class TStatefulMap1 : public IStatefulDoFn__Range2<TKV<TString, TInRow>, TOutRow, TState>
{
public:
    void Start(TOutput<TOutputRow>&) override{}

    // Можем дать гарантию, что одинаковые ключи идут подряд (а можем не давать) ?
    void Do(TConstArrayRef<TStateful<TInputRow, TState>>& batch, TOutput<TOutputRow>& output) override
    {
        // По этому range'у можно бегать в памяти взад назад, обращаться к любым элементам, модифицировать состояние.

        THashSet<TString> keys;
        std::vector<TState*> states;

        for (int i = 0; i < std::ssize(batch); ++i) {
            auto outRow = F(*batch[i].MutableState(), batch[i].GetValue().Value);
            output.Add(outRow);
            auto inserted = keys.insert(batch[i].GetValue().Key).second;
            if (inserted) {
                states.push_back(batch[i].MutableState());
            }
        }

        Cleanup(states);
    }

    void Finish(TOutput<TOutputRow>&) override {}
};

template <typename TKey, typename TState>
class IDynamicTableStateTransform
{
public:
    // Важно, чтобы это была инъекция.
    TLegacyOwningKey MapKey(const TKey&);

    // Тут идёт логика про патчи.
    TState ReadState(const TUnversionedRow& );
    TUnversionedOwningRow WriteState(const TState&);
};

class TMyDynamicTableStateTransform
    : public IDynamicTableStateTransform<typename TKey, typename TState>
{};

class TStateIO;

int main(int argc, const char** argv)
{
    TMyConfig config = LoadBigRtConfig<TMyConfig>(argc, argv);
    auto pipeline = MakeBigRtPipeline(config.GetRorenConfig());

    TPState<TLegacyOwningKey, TUnversionedOwningRow> state =
        pipeline.DefineState(
            BigRtDynamicTableState(
                "some description from config or //path/to/some/table",
                new TMyDynamicTableStateTransform
        ));

    TPState<TString, TUnversionedOwningRow> state2 = state | ChangeKey([] (const TString& in) -> TLegacyOwningKey {
    });

    TPCollection<TKV<TString, TInRow>> input = pipeline.Apply(BigRtRead("description from config"));

    TPCollection<TOutRow> output = input | StatefulParDo(new TStatefulMap1, state3);

    output | BigRtWrite("description from config");
}

// Вопросы:
//  - Есть ли возможность сказать, что для какого-то ключа я не хочу читать state ?
//
//  - Мутабельность рейнжа.
//  - Сортированность рейнжа.
