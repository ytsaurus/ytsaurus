#include <yt/server/clickhouse_server/program.h>

int main(int argc, const char** argv)
{
    return NYT::NClickHouseServer::TClickHouseServerProgram().Run(argc, argv);
}
