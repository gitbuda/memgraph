#include "benchmark/benchmark.h"
#include "benchmark/benchmark_api.h"
#include "logging/default.hpp"
#include "logging/streams/stderr.hpp"
#include "mvcc/record.hpp"
#include "mvcc/version_list.hpp"

class Prop : public mvcc::Record<Prop> {};

// Benchmark multiple updates, and finds, focused on finds.
// This a rather weak test, but I'm not sure what's the better way to test this
// in the future.
// TODO(dgleich): Refresh this.
void MvccMix(benchmark::State &state) {
  while (state.KeepRunning()) {
    state.PauseTiming();
    tx::Engine engine;
    auto t1 = engine.begin();
    mvcc::VersionList<Prop> version_list(*t1);

    auto t2 = engine.begin();
    t1->commit();

    state.ResumeTiming();
    version_list.update(*t2);
    state.PauseTiming();

    state.ResumeTiming();
    version_list.find(*t2);
    state.PauseTiming();

    t2->abort();

    auto t3 = engine.begin();
    state.ResumeTiming();
    version_list.update(*t3);
    state.PauseTiming();
    auto t4 = engine.begin();

    // Repeat find state.range(0) number of times.
    state.ResumeTiming();
    for (int i = 0; i < state.range(0); ++i) {
      version_list.find(*t4);
    }
    state.PauseTiming();

    t3->commit();
    t4->commit();
    state.ResumeTiming();
  }
}

BENCHMARK(MvccMix)
    ->RangeMultiplier(2)       // Multiply next range testdata size by 2
    ->Range(1 << 14, 1 << 23)  // 1<<14, 1<<15, 1<<16, ...
    ->Unit(benchmark::kMillisecond);

int main(int argc, char **argv) {
  logging::init_async();
  logging::log->pipe(std::make_unique<Stderr>());

  ::benchmark::Initialize(&argc, argv);
  ::benchmark::RunSpecifiedBenchmarks();
  return 0;
}
