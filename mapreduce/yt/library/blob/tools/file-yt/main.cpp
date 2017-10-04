#include <mapreduce/yt/interface/init.h>
#include <mapreduce/yt/library/blob/tools/file-yt/lib/modes.h>

int main(const int argc, const char* argv[]) {
    NYT::Initialize(argc, argv);
    return NFileYtTool::Main(argc, argv);
}
