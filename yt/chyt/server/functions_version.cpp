#include "version.h"

#include <yt/yt/build/build.h>

#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

template <class TName>
class TFunctionVersion
    : public DB::IFunction
{
public:
    static constexpr auto name = TName::Name;

    static DB::FunctionPtr create(DB::ContextPtr context)
    {
        return std::make_shared<TFunctionVersion>(context->isDistributed());
    }

    explicit TFunctionVersion(bool isDistributed)
        : IsDistributed_(isDistributed)
    { }

    std::string getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    DB::DataTypePtr getReturnTypeImpl(const DB::DataTypes& /*arguments*/) const override
    {
        return std::make_shared<DB::DataTypeString>();
    }

    bool isDeterministic() const override
    {
        return false;
    }

    bool isDeterministicInScopeOfQuery() const override
    {
         return true;
    }

    bool isSuitableForConstantFolding() const override
    {
         return !IsDistributed_;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DB::DataTypesWithConstInfo& /*arguments*/) const override
    {
         return false;
    }

    DB::ColumnPtr executeImpl(const DB::ColumnsWithTypeAndName& /*arguments*/, const DB::DataTypePtr& /*resultType*/, size_t inputRowsCount) const override
    {
        if (strcmp(name, "chytVersion") == 0) {
            return DB::DataTypeString().createColumnConst(inputRowsCount, String(GetCHYTVersion()));
        } else if (strcmp(name, "ytVersion") == 0) {
            return DB::DataTypeString().createColumnConst(inputRowsCount, String(GetVersion()));
        } else {
            YT_ABORT();
        }
    }

private:
    bool IsDistributed_;
};

////////////////////////////////////////////////////////////////////////////////

struct TNameFunctionCHYTVersion { static constexpr auto Name = "chytVersion"; };
struct TNameFunctionYTVersion { static constexpr auto Name = "ytVersion"; };

////////////////////////////////////////////////////////////////////////////////

using TFunctionCHYTVersion = TFunctionVersion<TNameFunctionCHYTVersion>;
using TFunctionYTVersion = TFunctionVersion<TNameFunctionYTVersion>;

////////////////////////////////////////////////////////////////////////////////

REGISTER_FUNCTION(CHYT_Version)
{
    factory.registerFunction<TFunctionCHYTVersion>();
    factory.registerFunction<TFunctionYTVersion>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
