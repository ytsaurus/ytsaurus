#include <mapreduce/yt/interface/operation.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

using IStructuredJobPtr = TIntrusiveConstPtr<IStructuredJob>;

IStructuredJobPtr ConstructJob(TString jobName, TString state);

TString GetJobStateString(const IStructuredJob& job);

TString GetIoInfo(
    const IStructuredJob& job,
    TString cluster,
    TString transactionId,
    TString inputPaths,
    TString outputPaths,
    TString neededColumns);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
