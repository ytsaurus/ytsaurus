This directory contains a set of small examples that can be used to understand the basic principles of working with the library. The code of interesting examples is located in `Example01....java`, `Example02...java`, and other files.

If you are not familiar with the library, we recommend reading them in order. We also recommend reading ya.make and log4j2.xml, described below.

## Building and running examples

You can run any example without fear of breaking anything down.

You can build examples in the usual way for Arcadia. In the directory where the file you are currently reading (
README.md) is located, run the command:

```
ya make
```

To run the example, you can use the resulting `run.sh` script. You should pass the name of the example you are interested in as an argument:

```
./run.sh Example01CypressOperations
```

You can also generate an IDEA project for this tutorial and run examples from the IDE.

## Logging

The YT client keeps quite detailed logs. We recommend keeping logs enabled at the `debug` level for production processes.

In these examples, exactly this logging level is set. To understand how logging is enabled, read the files:
ya.make and log4j2.xml.
