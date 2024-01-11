#include <yt/yt/core/misc/arithmetic_formula.h>

int main(int /*argc*/, char** /*argv*/)
{
    while (true) {
        TString t = Cin.ReadLine();
        auto f = NYT::MakeArithmeticFormula(TString(t));
        Cout << f.Eval({{"a", 10}}) << Endl;
    }
}
