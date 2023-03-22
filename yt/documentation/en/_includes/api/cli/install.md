# {{product-name}} CLI

## Installation

There are several ways to install the CLI.
The CLI consists of several parts. The main part is written in Python, it is compatible with Python 2&3, and it is platform-independent.
To use the [YSON](../../../user-guide/storage/yson.md) format of data representation, you can install the so called YSON bindings.
They are a separate library available only for Linux and MacOS and written in C++.

You can find out the version of the installed CLI by calling the `yt --version` command, which is the easiest way to check if the CLI is working.

{% note warning "Attention!" %}

We do not recommend installing the library and YSON bindings in different ways at the same time.
This can lead to problems that are difficult to diagnose.

{% endnote %}

{% note info "Note" %}

If you have any problems, check the [FAQ](../../../faq/faq.md) section.

{% endnote %}

The package is called `ytsaurus-client`.

Before installing the package, you can install the `wheel` package to be able to install a version other than the system one or
install the {{product-name}} CLI without sudo).

By default, the latest stable version of the package is installed from PyPI.
All test versions have the "a1" suffix and can be installed via pip by adding the `--pre` option.
The command for installing from pypi:
```bash
# Install the {{product-name}} CLI
pip install ytsaurus-client
# Install YSON bindings
pip install ytsaurus-yson-bindings
```

## Autocompletion { #autocompletion }

The Deb and PyPI packages also come with a bash autocompletion script.

You can check whether it is operating correctly by entering this in the console:

```bash
yt <TAB><TAB>
abort-job
abort-op
abort-tx
add-member
alter-table
alter-table-replica
check-permission
...
```

The main advantage of this feature is supplementing paths in Cypress. For this function to work, the cluster you are working with must be specified in the environment variables, for example, using the `export YT_PROXY=<cluster_name>` command. Example:

```bash
export YT_PROXY=<cluster-name>
yt list /<TAB>/<TAB><TAB>
//@               //home/           //porto_layers/   //statbox/        //test_q_roc_auc  //trash           //userfeat/       //userstats/
//cooked_logs/    //logs            //projects/       //sys             //tmp/            //userdata/       //user_sessions/
```

If you see something else (either nothing or paths in the current directory) instead of Cypress command names or paths, then autocompletion is not enabled. Check that the following conditions are met:

- The main bash_completion package must be installed. On Ubuntu-like systems, this can be achieved by running the `sudo apt-get install bash-completion` command.
- Bash-completion must be initiated when a bash session is started. For example, by default, Ubuntu contains a `. /etc/bash_completion` string. If you don't see it, you should add it to .bashrc.
- The `yt_completion` file that configures autocompletion for the yt command must be placed to /etc/bash_completion.d. If all the contents of /etc/bash_completion.d do not run automatically on your system for some reason, you can run this script yourself at the end of your .bashrc.
