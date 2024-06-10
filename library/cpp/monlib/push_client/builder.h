#pragma once

#include <util/generic/ptr.h>
#include <util/datetime/base.h>

namespace NSolomon {
    class TSolomonJsonBuilder {
    public:
        using TSensorPath = TVector<std::pair<TString, TString>>;
        enum class EFormat {
            LegacyJson = 0,
            Json,
            Spack,
        };

        // Use proper content-type header from library/cpp/monlib/encode/format.h when replying to Solomon
        TSolomonJsonBuilder(EFormat format = EFormat::Json);
        ~TSolomonJsonBuilder();

        void Reset();

        template <typename T>
        void AddSensor(const TSensorPath& path, T value);

        template <typename T>
        void AddSensor(const TSensorPath& path, TInstant timestamp, T value);

        template <typename T>
        void AddDerivSensor(const TSensorPath& path, T value);

        template <typename T>
        void AddDerivSensor(const TSensorPath& path, TInstant timestamp, T value);

        // TODO: rename to something more appropriate
        TString GetJsonValue();

        EFormat Format() const;

    protected:
        void WriteCommonLabels(const TSensorPath& labels);

    protected:
        class TImpl;
        THolder<TImpl> Impl_;
    };
} // namespace NSolomon
