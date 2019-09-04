# Stubs for pyspark.status (Python 3.5)
#

from typing import Any, List, NamedTuple, Optional
from py4j.java_gateway import JavaArray, JavaObject  # type: ignore

class SparkJobInfo(NamedTuple):
    jobId: int
    stageIds: JavaArray
    status: str

class SparkStageInfo(NamedTuple):
    stageId: int
    currentAttemptId: int
    name: str
    numTasks: int
    numActiveTasks: int
    numCompletedTasks: int
    numFailedTasks: int

class StatusTracker:
    def __init__(self, jtracker: JavaObject) -> None: ...
    def getJobIdsForGroup(self, jobGroup: Optional[str] = ...) -> List[int]: ...
    def getActiveStageIds(self) -> List[int]: ...
    def getActiveJobsIds(self) -> List[int]: ...
    def getJobInfo(self, jobId: int) -> SparkJobInfo: ...
    def getStageInfo(self, stageId: int) -> SparkStageInfo: ...
