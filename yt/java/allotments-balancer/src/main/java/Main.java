
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import ru.yandex.allotment.Balancer;
import ru.yandex.inside.yt.kosher.CloseableYt;
import ru.yandex.inside.yt.kosher.Yt;
import ru.yandex.inside.yt.kosher.impl.YtConfiguration;

class Main {

    public static void main(String[] args) {
        OptionParser parser = new OptionParser();

        OptionSpec<String> proxyOpt = parser.accepts("proxy", "proxy")
                .withRequiredArg().ofType(String.class).defaultsTo("socrates.yt.yandex.net");

        OptionSpec runOpt = parser.accepts("run", "run");

        OptionSpec printStatusOpt = parser.accepts("status", "status");

        OptionSpec fixAllOpt = parser.accepts("fix", "fix");

        OptionSet option = parser.parse(args);

        String proxy = option.valueOf(proxyOpt);
        boolean dryRun = ! option.has(runOpt);

        YtConfiguration.Builder builder = YtConfiguration.builder().withApiHost(proxy);

        CloseableYt yt = Yt.builder(builder.build())
                .http()
                .build();

        Balancer balancer = new Balancer(yt);

        if (option.has(printStatusOpt)) {
            balancer.printStatus();
        }

        if (option.has(fixAllOpt)) {
            balancer.balance(dryRun);
        }

        System.exit(0);
    }
}
