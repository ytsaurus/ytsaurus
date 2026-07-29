#include <yt/yql/tools/dqrun_light/lib/dqrun_light_lib.h>
#include <util/generic/yexception.h>

int main(int argc, const char *argv[]) {
    try {
        return NYql::TDqRunLightTool().Main(argc, argv);
    }
    catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }
}
