public:
    DEFINE_BYVAL_RW_PROPERTY(TNullable<TInstant>, DelayedTime);

public:
    //! For persistence only.
    TTask();
    explicit TTask(TOperationControllerBase* controller);

    void Initialize();

    virtual TString GetId() const = 0;
    virtual TTaskGroupPtr GetGroup() const = 0;

    virtual int GetPendingJobCount() const;
    int GetPendingJobCountDelta();

    virtual int GetTotalJobCount() const;
    int GetTotalJobCountDelta();

    const TProgressCounter& GetJobCounter() const;

    virtual TJobResources GetTotalNeededResources() const;
    TJobResources GetTotalNeededResourcesDelta();

    virtual bool IsIntermediateOutput() const;

    bool IsStderrTableEnabled() const;

    bool IsCoreTableEnabled() const;

    virtual TDuration GetLocalityTimeout() const = 0;
    virtual i64 GetLocality(NNodeTrackerClient::TNodeId nodeId) const;
    virtual bool HasInputLocality() const;

    TJobResources GetMinNeededResources() const;

    virtual NScheduler::TExtendedJobResources GetNeededResources(const TJobletPtr& joblet) const = 0;

    void ResetCachedMinNeededResources();

    void AddInput(NChunkPools::TChunkStripePtr stripe);
    void AddInput(const std::vector<NChunkPools::TChunkStripePtr>& stripes);
    void FinishInput();

    void CheckCompleted();

    void ScheduleJob(
        NScheduler::ISchedulingContext* context,
        const TJobResources& jobLimits,
        NScheduler::TScheduleJobResult* scheduleJobResult);

    virtual void OnJobCompleted(TJobletPtr joblet, const NScheduler::TCompletedJobSummary& jobSummary);
    virtual void OnJobFailed(TJobletPtr joblet, const NScheduler::TFailedJobSummary& jobSummary);
    virtual void OnJobAborted(TJobletPtr joblet, const NScheduler::TAbortedJobSummary& jobSummary);
    virtual void OnJobLost(TCompletedJobPtr completedJob);

    // First checks against a given node, then against all nodes if needed.
    void CheckResourceDemandSanity(
        const TJobResources& nodeResourceLimits,
        const TJobResources& neededResources);

    // Checks against all available nodes.
    void CheckResourceDemandSanity(
        const TJobResources& neededResources);

    void DoCheckResourceDemandSanity(const TJobResources& neededResources);
