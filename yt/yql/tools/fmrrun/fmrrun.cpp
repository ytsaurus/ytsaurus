#include <yt/yql/tools/fmrrun/lib/fmrrun_lib.h>
#include <contrib/ydb/library/yql/dq/opt/dq_opt_join_cbo_factory.h>

int main(int argc, const char *argv[]) {
    try {
        return NYql::TFmrRunTool().Main(argc, argv);
    }
    catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }
}
