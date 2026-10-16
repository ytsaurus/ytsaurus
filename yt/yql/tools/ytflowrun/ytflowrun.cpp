#include <yt/yql/tools/ytflowrun/lib/ytflowrun_lib.h>

int main(int argc, const char *argv[]) {
    try {
        return NYql::TYtflowRunTool().Main(argc, argv);
    }
    catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }
}
