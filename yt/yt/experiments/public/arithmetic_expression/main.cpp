#include <yt/yt/core/misc/arithmetic_formula.h>

int main(int /*argc*/, char** /*argv*/)
{
    while (true) {
        std::string t = Cin.ReadLine();
        auto f = NYT::MakeArithmeticFormula(t);
        Cout << f.Eval({{"a", 10}}) << Endl;
    }
}
