#include <contrib/ydb/library/yql/dq/opt/dq_opt_join_cbo_factory.h>
#include <yql/tools/yqlrun/lib/yqlrun_lib.h>

namespace {

class TYtFileRunTool : public NYql::TYqlRunTool {
public:
    TYtFileRunTool() {
        GetRunOptions().EnableLineage = true;
    }
protected:
    virtual NYql::IOptimizerFactory::TPtr CreateCboFactory() override {
        return NYql::NDq::MakeCBOOptimizerFactory();
    }
};

}  // namespace

int main(int argc, const char *argv[]) {
    try {
        return TYtFileRunTool().Main(argc, argv);
    }
    catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }
}
