#pragma once

#include <yt/cpp/mapreduce/interface/fwd.h>

#include <util/generic/fwd.h>

class IInputStream;
class IOutputStream;

namespace NYtBlob {
    class TBlobInfo;

    void Upload(
        const TString& path, const TString& filename,
        const ::NYT::TYPath& table, ::NYT::IClientBasePtr client);

    void Upload(
        IInputStream& in, const TString& filename,
        const ::NYT::TYPath& table, ::NYT::IClientBasePtr client);

    void Download(
        const TString& path, const TStringBuf filename,
        const ::NYT::TYPath& table, ::NYT::IClientBasePtr client,
        const bool storeEntireFileInMemoryWhileDownloading);
    bool TryDownload(
        const TString& path, const TStringBuf filename,
        const ::NYT::TYPath& table, ::NYT::IClientBasePtr client,
        const bool storeEntireFileInMemoryWhileDownloading);

    void Download(
        const TString& path, const TStringBuf filename,
        const ::NYT::TYPath& table, ::NYT::IClientBasePtr client);
    bool TryDownload(
        const TString& path, const TStringBuf filename,
        const ::NYT::TYPath& table, ::NYT::IClientBasePtr client);

    void Download(
        IOutputStream& out, const TStringBuf filename,
        const ::NYT::TYPath& table, ::NYT::IClientBasePtr client);
    bool TryDownload(
        IOutputStream& out, const TStringBuf filename,
        const ::NYT::TYPath& table, ::NYT::IClientBasePtr client);

    void CreateTable(const ::NYT::TYPath& table, ::NYT::IClientBasePtr client);
    void Finish(const ::NYT::TYPath& table, ::NYT::IClientBasePtr client);
    void Finish(const ::NYT::TYPath& table, ::NYT::IClientBasePtr client, const bool uniqueFiles);
    void Reset(const ::NYT::TYPath& table, ::NYT::IClientBasePtr client);

    void List(const ::NYT::TYPath& table, ::NYT::IClientBasePtr client,
              TVector<TBlobInfo>& infos);
    TVector<TBlobInfo> List(const ::NYT::TYPath& table, ::NYT::IClientBasePtr client);
}
