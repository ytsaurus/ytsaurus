#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionStringToString.h>
#include <Functions/LowerUpperUTF8Impl.h>
#include <Functions/FunctionFactory.h>
#include <DBPoco/Unicode.h>


namespace DB
{
namespace
{

struct NameLowerUTF8
{
    static constexpr auto name = "lowerUTF8";
};

using FunctionLowerUTF8 = FunctionStringToString<LowerUpperUTF8Impl<'A', 'Z', DBPoco::Unicode::toLower, UTF8CyrillicToCase<true>>, NameLowerUTF8>;

}

REGISTER_FUNCTION(LowerUTF8)
{
    factory.registerFunction<FunctionLowerUTF8>();
}

}
