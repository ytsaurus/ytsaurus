#include <util/system/defaults.h>

#if defined(_win_)
    #include <fcntl.h>
    #include <io.h>
    #include <windows.h>
    #include <crtdbg.h>
#endif

#if defined(_unix_)
    #include <unistd.h>
#endif

#ifdef WITH_VALGRIND
    #define NOTE_IN_VALGRIND(test) VALGRIND_PRINTF("%s::%s", test->unit->name, test->name)
#else
    #define NOTE_IN_VALGRIND(test)
#endif

#include <util/generic/hash.h>
#include <util/generic/stroka.h>
#include <util/string/util.h>
#include <util/memory/profile.h>
#include <util/stream/ios.h>
#include <util/generic/hash_set.h>
#include <util/generic/yexception.h>
#include <util/system/valgrind.h>
#include <util/system/execpath.h>
#include <util/network/init.h>
#include <util/datetime/base.h>

#include <library/unittest/registar.h>

using namespace NUnitTest;

class TColoredProcessor: public ITestSuiteProcessor {
    public:
        inline TColoredProcessor()
            : PrintBeforeSuite_(false)
            , PrintTimes_(false)
            , IsTTY_(false)
            , PrevTime_(TInstant::Now())
            , ShowFails(false)
        {
#if defined(_unix_)
            IsTTY_ = isatty(fileno(stderr));
#endif
        }

        virtual ~TColoredProcessor() throw () {
        }

        inline void Disable(const char* name) {
            Disabled_.insert(name);
        }

        inline void Enable(const char* name) {
            Enabled_.insert(name);
        }

        inline void SetPrintBeforeSuite(bool print) {
            PrintBeforeSuite_ = print;
        }

        inline void SetPrintTimes(bool print) {
            PrintTimes_ = print;
        }

        inline void SetShowFails(bool show) {
            ShowFails = show;
        }

    private:
        virtual void OnUnitStart(const TUnit* unit) {
            if (PrintBeforeSuite_) {
                fprintf(stderr, "%s<-----%s %s\n", BlueColor(), OldColor(), unit->name);
            }
        }

        virtual void OnUnitStop(const TUnit* unit) {
            fprintf(stderr, "%s----->%s %s -> ok: %s%u%s",
                BlueColor(), OldColor(), unit->name,
                GreenColor(), GoodTestsInCurrentUnit(), OldColor()
            );
            if (FailTestsInCurrentUnit()) {
                fprintf(stderr, ", err: %s%u%s",
                    RedColor(), FailTestsInCurrentUnit(), OldColor()
                );
            }
            fprintf(stderr, "\n");
        }

        virtual void OnError(const TError* descr) {
            const Stroka err = Sprintf("[%sFAIL%s] %s::%s -> %s%s%s\n", RedColor(), OldColor(),
                descr->test->unit->name,
                descr->test->name,
                RedColor(), descr->msg, OldColor()
            );
            if (ShowFails) {
                Fails.push_back(err);
            }
            fprintf(stderr, "%s", ~err);
            NOTE_IN_VALGRIND(descr->test);
            PrintTimes();
        }

        virtual void OnSuccess(const TSuccess* descr) {
            fprintf(stderr, "[%sgood%s] %s::%s\n", GreenColor(), OldColor(),
                descr->test->unit->name,
                descr->test->name
            );
            NOTE_IN_VALGRIND(descr->test);
            PrintTimes();
        }

        inline void PrintTimes() {
            if (!PrintTimes_) {
                return;
            }

            const TInstant now = TInstant::Now();
            Cerr << now - PrevTime_ << Endl;
            PrevTime_ = now;
        }

        virtual void OnEnd() {
            fprintf(stderr, "[%sDONE%s] ok: %s%u%s",
                YellowColor(), OldColor(),
                GreenColor(), GoodTests(), OldColor()
            );
            if (FailTests())
                fprintf(stderr, ", err: %s%u%s",
                    RedColor(), FailTests(), OldColor()
                );
            fprintf(stderr, "\n");

            if (ShowFails) {
                for (size_t i = 0; i < Fails.size(); ++i) {
                    printf("%s", ~Fails[i]);
                }
            }
        }

        virtual bool CheckAccess(const char* name) {
            if (Disabled_.find(name) != Disabled_.end()) {
                return false;
            }

            if (Enabled_.empty()) {
                return true;
            }

            return Enabled_.find(name) != Enabled_.end();
        }

    private:
        inline const char* OldColor() throw () {
            return Color(0);
        }

        inline const char* RedColor() throw () {
            return Color(31);
        }

        inline const char* YellowColor() throw () {
            return Color(33);
        }

        inline const char* GreenColor() throw () {
            return Color(32);
        }

        inline const char* BlueColor() throw () {
            return Color(34);
        }

        inline const char* Color(int val) throw () {
            if (IsTTY_) {
                Stroka& ret = Bufs_[val];

                if (ret.empty()) {
                    if (val == 0) {
                        ret = Sprintf("\033[0m");
                    } else {
                        ret = Sprintf("\033[1;%dm", val);
                    }
                }

                return ~ret;
            }

            return "";
        }

    private:
        bool PrintBeforeSuite_;
        bool PrintTimes_;
        yhash<int, Stroka> Bufs_;
        bool IsTTY_;
        yhash_set<Stroka> Disabled_;
        yhash_set<Stroka> Enabled_;
        TInstant PrevTime_;
        bool ShowFails;
        yvector<Stroka> Fails;
};

#if !defined(UTMAIN)
    #define UTMAIN main
#endif

int UTMAIN(int argc, char** argv) {
    InitDebugAllocator();
    InitNetworkSubSystem();

    try {
        GetExecPath();
    } catch (...) {
    }

#ifdef _win_
    ::SetConsoleOutputCP(1251);
    _setmode(_fileno(stdout), _O_BINARY);
    _CrtSetDbgFlag(_CRTDBG_ALLOC_MEM_DF | _CRTDBG_LEAK_CHECK_DF);
#endif // _win_

#ifndef UT_SKIP_EXCEPTIONS
    try {
#endif
        TColoredProcessor processor;

        for (size_t i = 1; i < (size_t)argc; ++i) {
            const char* name = argv[i];
            if (name && *name) {
                if (strcmp(name, "--print-before-suite") == 0) {
                    processor.SetPrintBeforeSuite(true);
                } else if (strcmp(name, "--show-fails") == 0) {
                    processor.SetShowFails(true);
                } else if (strcmp(name, "--print-times") == 0) {
                    processor.SetPrintTimes(true);
                } else if (*name == '-') {
                    processor.Disable(name + 1);
                } else if (*name == '+') {
                    processor.Enable(name + 1);
                } else {
                    processor.Enable(name);
                }
            }
        }

        TTestFactory::Instance().SetProcessor(&processor);

        const unsigned ret = TTestFactory::Instance().Execute();

        if (ret) {
            Cerr << "SOME TESTS FAILED!!!!" << Endl;
        }

        return ret;
#ifndef UT_SKIP_EXCEPTIONS
    } catch (...) {
        Cerr << "caught exception in test suite(" << CurrentExceptionMessage() << ")" << Endl;
    }
#endif

    return 1;
}
