# xsf
Special function implementations.

See https://github.com/scipy/xsf/issues/1 for context.

## Tests

To run the tests:
- [clone this repository](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository)
- `cd xsf`
- [install Pixi](https://pixi.sh/latest/#installation)
- `pixi run tests`

You can trigger a rebuild inbetween test runs with:

```shell
pixi run build-tests
```

For subsequent test runs, to skip re-cloning [`xsref`](https://github.com/scipy/xsref) or to control parallelism for individual commands, you can use:

```shell
pixi run clone-xsf
pixi run configure-tests
pixi run build-only -j8
pixi run --skip-deps tests -j2
```

> [!NOTE]  
> This has currently only been tested on Linux.
