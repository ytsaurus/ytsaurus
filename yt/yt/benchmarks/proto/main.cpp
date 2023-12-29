#include <util/system/hp_timer.h>

#include <util/datetime/base.h>

#include <vector>

#include <yt/yt/benchmarks/proto/test_base.pb.h>
#include <yt/yt/benchmarks/proto/test_full.pb.h>
#include <yt/yt/benchmarks/proto/test_part.pb.h>

using namespace NHPTimer;

const char* KEY = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

int main(int argc, const char* argv[])
{
    int samples = 50;
    if (argc > 1)
        samples = atoi(argv[1]);

    int N  = 30000;
    if (argc > 2)
        N = atoi(argv[2]);

    TExtendFull extFull;
    extFull.set_int_field(5678);
    extFull.set_bool_field(false);

    for (int i = 0; i < samples; ++i) {
        auto* sample = extFull.add_samples();
        for (int j = 0; j < 5; ++j)
            //sample->add_keys(KEY);
            sample->add_keys(23423);
        sample->set_row_index(4785);
    }

    //auto input = extFull;
    TBase input;
    *input.MutableExtension(TExtendFull::full) = extFull;

    std::vector<char> data;
    data.resize(input.ByteSize());

    {
        STime now;
        GetTime(&now);

        for (int i = 0; i < N; ++i) {
            (void)input.SerializeToArray(data.begin(), data.size());
        }

        Cout << "Serialize time: " << GetTimePassed(&now) / N << Endl;
    }

    Cout << "Proto size: " << data.size() << Endl;

    {
        STime now;
        GetTime(&now);

        for (int i = 0; i < N; ++i) {
            TBase output;
            //TExtendFull output;
            (void)output.ParseFromArray(data.begin(), data.size());
            //auto& ext = output.GetExtension(TExtendPart::part);
            //auto& ext = output.GetExtension(TExtendFull::full);
        }

        Cout << "Deserialize time: " << GetTimePassed(&now) / N << Endl;
    }
}

