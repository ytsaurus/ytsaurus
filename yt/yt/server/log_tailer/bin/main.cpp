#include <yt/yt/server/log_tailer/program.h>

int main(int argc, const char** argv)
{
    NYT::NLogTailer::RunLogTailerProgram(argc, argv);
}
