#include <yql/essentials/public/udf/udf_registrator.h>
#include <yql/essentials/utils/backtrace/backtrace.h>
#include <yql/essentials/utils/log/log.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_job_impl.h>
#include <yt/yql/providers/yt/fmr/process/yql_yt_job_fmr.h>

int main() {
    NYql::NBacktrace::RegisterKikimrFatalActions();
    NYql::NBacktrace::EnableKikimrSymbolize();
    EnableKikimrBacktraceFormat();
    Y_UNUSED(NYql::NUdf::GetStaticSymbols());

    NYql::NLog::YqlLoggerScope logger(&Cerr);
    try {
        NYql::NFmr::TFmrUserJob mapJob;
        TFileInput fileStream("fmrjob.bin");
        mapJob.Load(fileStream);
        mapJob.DoFmrJob(NYql::NFmr::TFmrUserJobOptions{.WriteStatsToFile = true});
        return 0;
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }
}
