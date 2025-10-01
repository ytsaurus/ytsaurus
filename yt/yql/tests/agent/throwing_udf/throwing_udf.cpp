#include <yql/essentials/public/udf/udf_helpers.h>
#include <yql/essentials/public/udf/udf_value_builder.h>

#include <util/generic/yexception.h>


namespace {

using namespace NKikimr::NUdf;

SIMPLE_UDF(TParseWithThrow, char*(TAutoMap<char*>, TAutoMap<bool>)) {
    if (auto needThrow = args[1].Get<bool>()) {
        try {
            ythrow yexception() << "expected exception";
        } catch (...) {
            return valueBuilder->NewString(CurrentExceptionMessage());
        }
    }

    return args[0];
};

SIMPLE_MODULE(TThrowingUdfModule,
    TParseWithThrow);

REGISTER_MODULES(TThrowingUdfModule);

} // anonymous namespace
