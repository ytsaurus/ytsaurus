#pragma once

#include <yt/cpp/roren/interface/roren.h>

#include <yt/microservices/id_to_path_mapping/id_to_path_updater/lib/messages.pb.h>

template <typename T>
class TLoggingDoFn : public NRoren::IDoFn<T&&, T>
{
public:
    NYT::NLogging::TLogger Logger{"LogOnceInAWhile"};

public:
    TLoggingDoFn() = default;

    TLoggingDoFn(TString objectName)
        : ObjectName_(objectName)
    { }

    void Start(NRoren::TOutput<T>&) override
    {
        Logged_ = false;
    }

    void Do(T&& row, NRoren::TOutput<T>& output)
    {
        if (!Logged_) {
            YT_LOG_INFO("Processing %v: %v", ObjectName_, ToString(row));
            Logged_ = true;
        }
        output.Add(std::move(row));
    }

private:
    TString ObjectName_;
    Y_SAVELOAD_DEFINE_OVERRIDE(ObjectName_);

    bool Logged_ = false;
};

class TLogOnceInAWhileTransform
{
public:
    TLogOnceInAWhileTransform(TString objectName)
        : ObjectName_(std::move(objectName))
    { }

    template <typename T>
    NRoren::TPCollection<T> ApplyTo(const NRoren::TPCollection<T>& pCollection) const
    {
        return pCollection | NRoren::MakeParDo<TLoggingDoFn<T>>(ObjectName_);
    }

    TString GetName() const
    {
        return "LogOnceInAWhile";
    }

private:
    TString ObjectName_;
};

NRoren::TParDoApplicator<NJson::TJsonValue, TIdToPathRow> MakeUpdateItem(TString forceCluster);
NRoren::TParDoApplicator<TIdToPathRow, TIdToPathRow> AllowClusters(THashSet<std::string> clusterFilter);
NRoren::TParDoApplicator<TIdToPathRow, TIdToPathRow> DenyClusters(THashSet<std::string> clusterFilter);
TLogOnceInAWhileTransform LogOnceInAWhile(TString objectName);
