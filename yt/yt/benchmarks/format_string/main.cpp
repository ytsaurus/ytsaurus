#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/yt/string/format.h>

#include <util/string/cast.h>

using namespace NYT;
using namespace NYT::NConcurrency;
using namespace NYT::NProfiling;

Y_FORCE_INLINE void Iteration(int callsCount)
{
    // All samples taken from lyrics of "Smash Mouth -- All Star".

    [[maybe_unused]] static constexpr auto mediumDensityOfMarkers =
        "Somebody %d told me the world is gonna roll me"
        "I ain%vt the sharpest tool in the shed"
        "She was looking kind of dumb with her finger and her thumb"
        "In the shape of an %Qv on her forehead"
        ""
        "Well the years start coming and they don%vt stop coming"
        "Fed to the rules and I hit the ground running"
        "Didn%vt make sense not to live for fun"
        "Your brain gets smart but your head gets dumb"
        "So much to do, so much to see"
        "So what's wrong with taking the back streets?"
        "%v know %v go"
        "%v shine %v glow"
        ""
        "Hey now, you're an all-star, get your game on, go play"
        "%v"
        "And all that glitters is gold"
        "Only shooting stars break the %v"
        "";

    [[maybe_unused]] static constexpr auto lowDensityOfMarkers =
        "It's a cool place and they say it gets colder"
        "You're bundled up now, wait till you get older"
        "But the meteor men %v to differ"
        "Judging by the hole in the satellite picture"
        "The ice we skate is getting pretty thin"
        "The water's getting warm so you might as well swim"
        "My %v's on fire, how about yours?"
        "That's the way I like it and I never get bored"
        ""
        "Hey now, you're an all-star, get your game on, go play"
        "Hey now, you're a rock star, get the show on, get paid"
        "All that %v is gold"
        "Only shooting stars break the mold"
        ""
        "Hey now, you're an all-star, get your game on, go play"
        "Hey now, you're a rock star, get the show, on get paid"
        "And all that glitters is gold"
        "Only %v stars"
        "";

    [[maybe_unused]] static constexpr auto highDensityOfMarkers =
        "Someb%vdy once asked could I spare some change for %das?"
        "I need to %v myself away from this pl%vce"
        "I said yep what a %v"
        "I could use a little fuel %v"
        "%v"
        ""
        "%v, the years start coming and they don't stop coming"
        "Fed to the rules and I %v running"
        "Didn't make sense not to live for fun"
        "Your brain gets %v but your head gets %v"
        "So much %v, so much %v"
        "So what's wrong with taking the back streets?"
        "%v%v"
        ""
        "%v, you're an %v, get your game on, go play"
        "%v, you're a %v, get the show on, get paid"
        "And all that glitters is gold"
        "Only %v break the mold"
        ""
        "And all that glitters is gold"
        "Only %v break the mold"
        ;
    [[maybe_unused]] static constexpr auto myNormalString =
        "Partition reader config (UseNativeTabletNodeApi: %v, UsePullQueueConsumer: %v, UsePullQueueConsumer: %v, UsePullQueueConsumer: %v, UsePullQueueConsumer: %v)";

    for (int i = 0; i < callsCount; ++i) {
        switch (i % 3) {
            default:
                NYT::Format
                // NYT::NDetail::FormatOld
                (
                    myNormalString,
                    2,
                    "foo",
                    "bar",
                    "143215",
                    "fdsgdsafas"
                );
            // case 0:
            //     // NYT::Format
            //     NYT::NDetail::FormatOld
            //     (
            //         mediumDensityOfMarkers,
            //         1,
            //         '\'',
            //         'L',
            //         '\'',
            //         '\'',
            //         "You'll never",
            //         "if you don't",
            //         "You'll never",
            //         "if you don't",
            //         NYT::Format("Hey now, %v're a rock star, get the show on, get paid", "you"),
            //         "mold");
            //     break;

            // case 1:
            //     // NYT::Format
            //     NYT::NDetail::FormatOld
            //     (
            //         lowDensityOfMarkers,
            //         "beg",
            //         "world",
            //         "glitters",
            //         "shooting");
            //     break;

            // case 2:
            //     // NYT::Format
            //     NYT::NDetail::FormatOld
            //     (
            //         highDensityOfMarkers,
            //         0,
            //         9,
            //         "get",
            //         "@",
            //         "concept",
            //         "myself",
            //         NYT::Format(
            //             "And we could all %v",
            //             NYT::Format("use %v %v %v", "a", "little", "change")),
            //         "Well",
            //         "hit the ground",
            //         "smart",
            //         "dumb",
            //         "to do",
            //         "to see",
            //         NYT::Format("You'll %v if you don't %v %v", "never know", "go", "(go!)"),
            //         NYT::Format("You'll %v if you don't %v", "never shine", "glow"),
            //         "Hey now",
            //         "all-star",
            //         "Hey now",
            //         "rock star",
            //         "shooting stars",
            //         "shooting stars");
            //     break;
        }
    }
}

int main(int argc, char** argv)
{
    if (argc < 3) {
        Cerr << "Usage: " << argv[0] << " [iteration_count] [calls_per_iteration]" << Endl;
        return -1;
    }

    const int iterations = FromString<int>(argv[1]);
    const int callsCount = FromString<int>(argv[2]);

    for (int index = 0; index < iterations; ++index) {
        Cout << "Mambo number " << index << Endl;

        TWallTimer timer;

        Iteration(callsCount);

        auto ms = timer.GetElapsedTime().MilliSeconds();

        Cout << "Total time (ms): " << ms << Endl;
    }

    return 0;
}
