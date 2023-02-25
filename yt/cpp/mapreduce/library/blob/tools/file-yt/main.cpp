#include <yt/cpp/mapreduce/interface/init.h>
#include <yt/cpp/mapreduce/library/blob/tools/file-yt/lib/modes.h>

int main(const int argc, const char* argv[]) {
    NYT::Initialize(argc, argv);
    return NFileYtTool::Main(argc, argv);
}
