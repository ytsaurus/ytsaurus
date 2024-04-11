# CLI and Python API

You can use the CHYT CLI and the CHYT Python API to run your clique or send a query to CHYT from a program or the command line. They can be obtained as part of the {% if audience == "public" %}`ytsaurus-client`{% else %}`yandex-yt` package or built from Arcadia{% endif %}.

The command line utility accepts two environment variables: `YT_PROXY` and `CHYT_ALIAS`. You can use the former to specify the {{product-name}} cluster, and the latter to specify the used clique.

For example, you can use the following command for Linux and macOS  to set environment variables and no longer pass the `--proxy <cluster_name>` and `--alias *ch_public` parameters to all subsequent calls:

```bash
export YT_PROXY=<cluster_name> YT_ALIAS=*ch_public
```

{% cut "About using the `--alias` parameter" %}

{% include [Note about alias and asterisk](_includes/about-alias.md) %}

{% endcut %}

The process that launches the clique can sometimes be informally called a *launcher*.

## {{package-name}} { #{{package-name}} }

The main way to get started with {{product-name}} is to install the `{{package-name}}` package. For more information, see [Python Wrapper](../../../../api/python/start.md).
