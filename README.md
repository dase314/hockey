# Hockey

Hockey is a hybrid PMem-SSD storage engine for analytical database.
![Hockey](/docs/hockey-docs/hockey_overview.png)

The code base for Hockey is [ClickHouse](https://github.com/ClickHouse/ClickHouse).

## Build

Please reference [here](/docs/en/development/build.md). In addition, the following librarys are needed.
* [PMDK](https://github.com/pmem/pmdk)
* [libpmemobj-cpp](https://github.com/pmem/libpmemobj-cpp)

## Benchmark

Part of the benchmark results were performed under star schema benchmark.
![ssb_results](docs/hockey-docs/ssb_performance.png)
## Misc

Hockey is still in development, any contribution is welcome.


## License

Hockey is under the Apache 2.0 license.


