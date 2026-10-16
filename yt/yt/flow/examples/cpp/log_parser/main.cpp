#include <yt/yt/flow/examples/cpp/log_parser/lib/log_parser_process_function.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    NYT::NFlow::TSimpleSpecBuilder builder;
    builder.RegisterStream<NYT::NFlow::NExample::TLogRecordMessage>("records");
    return NYT::NFlow::TSimpleRunnerProgram(std::move(builder)).Run(argc, argv);
}
