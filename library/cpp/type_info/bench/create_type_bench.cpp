#include <library/cpp/testing/benchmark/bench.h>

#include <library/cpp/type_info/type_info.h>

struct TRaw {
    NTi::IPoolTypeFactory* F;

    auto Void() {
        return F->VoidRaw();
    }
    auto Null() {
        return F->NullRaw();
    }
    auto Bool() {
        return F->BoolRaw();
    }
    auto Int8() {
        return F->Int8Raw();
    }
    auto Int16() {
        return F->Int16Raw();
    }
    auto Int32() {
        return F->Int32Raw();
    }
    auto Int64() {
        return F->Int64Raw();
    }
    auto Uint8() {
        return F->Uint8Raw();
    }
    auto Uint16() {
        return F->Uint16Raw();
    }
    auto Uint32() {
        return F->Uint32Raw();
    }
    auto Uint64() {
        return F->Uint64Raw();
    }
    auto Float() {
        return F->FloatRaw();
    }
    auto Double() {
        return F->DoubleRaw();
    }
    auto String() {
        return F->StringRaw();
    }
    auto Utf8() {
        return F->Utf8Raw();
    }
    auto Date() {
        return F->DateRaw();
    }
    auto Datetime() {
        return F->DatetimeRaw();
    }
    auto Timestamp() {
        return F->TimestampRaw();
    }
    auto TzDate() {
        return F->TzDateRaw();
    }
    auto TzDatetime() {
        return F->TzDatetimeRaw();
    }
    auto TzTimestamp() {
        return F->TzTimestampRaw();
    }
    auto Interval() {
        return F->IntervalRaw();
    }
    auto Decimal(ui8 precision, ui8 scale) {
        return F->DecimalRaw(precision, scale);
    }
    auto Json() {
        return F->JsonRaw();
    }
    auto Yson() {
        return F->YsonRaw();
    }
    auto Uuid() {
        return F->UuidRaw();
    }
    auto Optional(const NTi::TType* item) {
        return F->OptionalRaw(item);
    }
    auto List(const NTi::TType* item) {
        return F->ListRaw(item);
    }
    auto Dict(const NTi::TType* key, const NTi::TType* value) {
        return F->DictRaw(key, value);
    }
    auto Struct(NTi::TStructType::TMembers members) {
        return F->StructRaw(members);
    }
    auto Struct(TMaybe<TStringBuf> name, NTi::TStructType::TMembers members) {
        return F->StructRaw(name, members);
    }
    auto Tuple(NTi::TTupleType::TElements elements) {
        return F->TupleRaw(elements);
    }
    auto Tuple(TMaybe<TStringBuf> name, NTi::TTupleType::TElements elements) {
        return F->TupleRaw(name, elements);
    }
    auto Variant(const NTi::TType* inner) {
        return F->VariantRaw(inner);
    }
    auto Variant(TMaybe<TStringBuf> name, const NTi::TType* inner) {
        return F->VariantRaw(name, inner);
    }
    auto Tagged(const NTi::TType* type, TStringBuf tag) {
        return F->TaggedRaw(type, tag);
    }
};

template <typename TFactory>
Y_NO_INLINE auto DoAllocHugeType(TFactory& f) {
    // Allocate type for the reqans log. Note: this tests allocation and deduplication speed,
    // it doesn't test deserialization speed.

    auto osFamily = f.Variant(
        "OsFamily",
        f.Struct({
            {"IOS", f.Void()},
            {"ANDROID", f.Void()},
            {"WINDOWS", f.Void()},
            {"MACOS", f.Void()},
            {"SYMBIAN", f.Void()},
            {"WINDOWSPHONE", f.Void()},
            {"TIZEN", f.Void()},
            {"BADA", f.Void()},
            {"FIREFOXOS", f.Void()},
            {"JAVA", f.Void()},
            {"MEEGO", f.Void()},
            {"RIMTABLETOS", f.Void()},
            {"WINDOWSMOBILE", f.Void()},
            {"WINDOWSRT", f.Void()},
            {"LINUX", f.Void()},
            {"UNKNOWN", f.Void()},
        }));

    auto browserEngine = f.Variant(
        "BrowserEngine",
        f.Struct({
            {"PRESTO", f.Void()},
            {"GECKO", f.Void()},
            {"TRIDIENT", f.Void()},
            {"WEBKIT", f.Void()},
            {"KHTML", f.Void()},
            {"UCBBROWSER", f.Void()},
            {"TEXTBASED", f.Void()},
            {"PROPRIETARY", f.Void()},
            {"UNKNOWN", f.Void()},
        }));

    auto platform = f.Variant(
        "Platform",
        f.Struct({
            {"ANDROID", f.Void()},
            {"APAD", f.Void()},
            {"IPHONE", f.Void()},
            {"IPAD", f.Void()},
            {"WIN_PHONE", f.Void()},
            {"WIN_RT", f.Void()},
            {"IPOD", f.Void()},
            {"IOS", f.Void()},
            {"UNKNOWN", f.Void()},
        }));

    auto domain = f.Variant(
        "Domain",
        f.Struct({
            {"RU", f.Void()},
            {"COM", f.Void()},
            {"COM_TR", f.Void()},
            {"UA", f.Void()},
            {"BY", f.Void()},
            {"KZ", f.Void()},
            {"UZ", f.Void()},
            {"NET", f.Void()},
        }));

    auto provider = f.Variant(
        "Provider",
        f.Struct({
            {"PERL_REPORT", f.Void()},
            {"IMAGES_PERL_REPORT", f.Void()},
            {"VIDEO_PERL_REPORT", f.Void()},
            {"NODE_LAYOUT", f.Void()},
            {"ATOM_FRONT", f.Void()},
            {"BROWSER_LAYOUT", f.Void()},
            {"UNKNOWN", f.Void()},
            {"MOBILE_ANDROID_APP", f.Void()},
            {"MOBILE_IOS_APP", f.Void()},
            {"MOBILE_WP_APP", f.Void()},
            {"MOBILE_SEARCH_APP", f.Void()},
            {"WEATHER_NODE_LAYOUT", f.Void()},
            {"RENDERER", f.Void()},
            {"MAIL", f.Void()},
            {"MARKET", f.Void()},
            {"RTB", f.Void()},
            {"ERROR", f.Void()},
            {"PDB_BACKEND", f.Void()},
            {"PDB_INFORMERS_BACKEND", f.Void()},
            {"BLOCKSTAT_CONVERTER", f.Void()},
            {"IMG_REQANS_CONVERTER", f.Void()},
            {"JOINED_BS_DSP_RTB_CONVERTER", f.Void()},
            {"MARKET_ACCESS_CONVERTER", f.Void()},
            {"MARKET_CLICKS_CONVERTER", f.Void()},
            {"MARKET_CLICKS_ORDERS_CONVERTER", f.Void()},
            {"MARKET_CPA_CLICKS_CONVERTER", f.Void()},
            {"MARKET_FEATURES_SET_CONVERTER", f.Void()},
            {"MARKET_REPORT_CONVERTER", f.Void()},
            {"REQANS_CONVERTER", f.Void()},
            {"VIDEOREQANS_CONVERTER", f.Void()},
            {"WEATHER_ACCESS_CONVERTER", f.Void()},
            {"MARKET_SPLITTER", f.Void()},
            {"IMG_REQANS_SPLITTER", f.Void()},
            {"REQANS_SPLITTER", f.Void()},
            {"VIDEO_PLAYER", f.Void()},
            {"RECOMMENDER_BACKEND", f.Void()},
            {"UGC", f.Void()},
            {"LAUNCHER", f.Void()},
            {"NEWS", f.Void()},
            {"METRICA_CONVERTER", f.Void()},
            {"CREATE_SCARAB_LIB", f.Void()},
            {"TRANSLATE", f.Void()},
        }));

    auto userInterface = f.Variant(
        "UserInterface",
        f.Struct({
            {"DESKTOP", f.Void()},
            {"PAD", f.Void()},
            {"TOUCH", f.Void()},
            {"MOBILE_APP", f.Void()},
            {"TV", f.Void()},
            {"OLD_MOBILE_APP", f.Void()},
            {"YANDEX_BROWSER_MOBILE_APP", f.Void()},
            {"MOBILE", f.Void()},
            {"SITE_SEARCH", f.Void()},
            {"SITE_SEARCH_WEB", f.Void()},
            {"DIRECT_SEARCH", f.Void()},
            {"UNKNOWN", f.Void()},
        }));

    auto navLevel = f.Variant(
        "NavLevel",
        f.Struct({
            {"NAVIG", f.Void()},
            {"URLNAV", f.Void()},
        }));

    auto serviceKey = f.Variant(
        "ServiceKey",
        f.Struct({
            {"Assistant", f.Void()},
            {"Atomsearch", f.Void()},
            {"Brosearch", f.Void()},
            {"ConfigExport", f.Void()},
            {"Jsonproxy", f.Void()},
            {"Mobile", f.Void()},
            {"Searchapi", f.Void()},
            {"Searchapp", f.Void()},
            {"Suggest", f.Void()},
            {"Touchsearch", f.Void()},
            {"People", f.Void()},
            {"SearchWww", f.Void()},
            {"Direct", f.Void()},
            {"Iznanka", f.Void()},
        }));

    auto userAgent = f.Struct(
        "UserAgent",
        {
            {"OsFamily", f.Optional(osFamily)},
            {"OsVersion", f.Optional(f.String())},
            {"BrowserEngine", f.Optional(browserEngine)},
            {"Platform", f.Optional(platform)},
        });

    auto experiment = f.Struct(
        "Experiment",
        {
            {"TestId", f.String()},
            {"Bucket", f.Int32()},
        });

    auto version = f.Struct(
        "Version",
        {
            {"MajorVersion", f.Uint32()},
            {"MinorVersion", f.Uint32()},
            {"Revision", f.Optional(f.Uint32())},
            {"Build", f.Optional(f.Uint32())},
        });

    auto userId = f.Struct(
        "UserId",
        {
            {"uuid", f.Optional(f.String())},
            {"yandex_uid", f.Optional(f.String())},
            {"platform_id", f.Optional(f.String())},
            {"platform_id2", f.Optional(f.String())},
            {"device_id", f.Optional(f.String())},
            {"yandex_fuid", f.Optional(f.String())},
            {"yandex_login", f.Optional(f.String())},
            {"passport_uid", f.Optional(f.String())},
            {"robot_uid", f.Optional(f.String())},
            {"i_cookie", f.Optional(f.String())},
        });

    auto misspellInfo = f.Struct(
        "MisspellInfo",
        {
            {"Rule", f.String()},
            {"Relev", f.Int32()},
            {"CorrectedQuery", f.String()},
            {"Source", f.Optional(f.String())},
            {"Flags", f.Optional(f.Uint32())},
        });

    auto webDocument = f.Struct(
        "WebDocument",
        {
            {"Url", f.String()},
            {"Source", f.String()},
            {"PassagesCount", f.Optional(f.Uint32())},
            {"Position", f.Uint32()},
            {"Relevance", f.Int64()},
            {"SavedDoc", f.Optional(f.String())},
            {"Language", f.Optional(f.String())},
            {"NavLevel", f.Optional(navLevel)},
            {"Regions", f.List(f.Int32())},
            {"SnippetType", f.Optional(f.String())},
            {"IsFirst", f.Bool()},
            {"IsSecond", f.Bool()},
            {"Shard", f.Optional(f.String())},
            {"BaseType", f.Optional(f.String())},
            {"Markers", f.Dict(f.String(), f.String())},
            {"SiteLinks", f.List(f.String())},
            {"FullDocId", f.Optional(f.String())},
            {"HostId", f.Optional(f.String())},
        });

    return f.Struct(
        "WebReportSearchReqansEvent",
        {
            {"ParentRequestId", f.Optional(f.String())},
            {"UserAgent", f.Optional(userAgent)},
            {"UserAgentRaw", f.Optional(f.String())},
            {"UserRegionId", f.Int32()},
            {"UserIp", f.String()},
            {"Referer", f.Optional(f.String())},
            {"Domain", domain},
            {"Experiments", f.List(experiment)},
            {"ConfigVersion", f.Optional(f.Uint32())},
            {"ServerVersion", version},
            {"ServiceKey", serviceKey},
            {"RequestId", f.String()},
            {"UserId", userId},
            {"PageNo", f.Uint32()},
            {"RequestedPageSize", f.Uint32()},
            {"Catalogue", f.Optional(f.String())},
            {"GroupBy", f.Optional(f.String())},
            {"RelevRegion", f.Optional(f.Int32())},
            {"Msp", f.Optional(misspellInfo)},
            {"Reask", f.Optional(misspellInfo)},
            {"Provider", provider},
            {"UserInterface", userInterface},
            {"Query", f.String()},
            {"GoodWords", f.String()},
            {"UiLanguage", f.String()},
            {"FullRequest", f.String()},
            {"Relev", f.Dict(f.String(), f.String())},
            {"Rearr", f.Dict(f.String(), f.String())},
            {"SearchProps", f.Optional(f.Dict(f.String(), f.String()))},
            {"BsBlockId", f.Optional(f.String())},
            {"NumDocs", f.Uint64()},
            {"Documents", f.List(webDocument)},
            {"AdvExperiments", f.List(experiment)},
            {"BsBlockIds", f.List(f.String())},
            {"Host", f.String()},
        });
}

Y_CPU_BENCHMARK(AllocHugeType_Pool_Dedup_Raw, params) {
    for (size_t i = 0; i < params.Iterations(); ++i) {
        NBench::Clobber();
        auto f = NTi::PoolFactory(true);
        auto r = TRaw{f.Get()};
        auto ptr = DoAllocHugeType(r);
        NBench::DoNotOptimize(ptr);
    }
}

Y_CPU_BENCHMARK(AllocHugeType_Pool_Dedup_Safe, params) {
    for (size_t i = 0; i < params.Iterations(); ++i) {
        NBench::Clobber();
        auto f = NTi::PoolFactory(true);
        auto ptr = DoAllocHugeType(*f);
        NBench::DoNotOptimize(ptr);
    }
}

Y_CPU_BENCHMARK(AllocHugeType_Pool_NoDedup_Raw, params) {
    for (size_t i = 0; i < params.Iterations(); ++i) {
        NBench::Clobber();
        auto f = NTi::PoolFactory(false);
        auto r = TRaw{f.Get()};
        auto ptr = DoAllocHugeType(r);
        NBench::DoNotOptimize(ptr);
    }
}

Y_CPU_BENCHMARK(AllocHugeType_Pool_NoDedup_Safe, params) {
    for (size_t i = 0; i < params.Iterations(); ++i) {
        NBench::Clobber();
        auto f = NTi::PoolFactory(false);
        auto ptr = DoAllocHugeType(*f);
        NBench::DoNotOptimize(ptr);
    }
}

Y_CPU_BENCHMARK(AllocHugeType_Heap_NoDedup_Safe, params) {
    for (size_t i = 0; i < params.Iterations(); ++i) {
        NBench::Clobber();
        auto f = NTi::HeapFactory();
        auto ptr = DoAllocHugeType(*f);
        NBench::DoNotOptimize(ptr);
    }
}
