# Rate Bottleneck Diagnosis

Investigation date: 2026-05-01
Scope: `rate` leaf-service reruns for `200m x 2`, `200m x 3`, `500m x 2`, and `500m x 3`.
Method: direct gRPC load on `rate`, cold-cache steps, `kubectl top`, pod cgroup stats, `perf stat`, and `pidstat -wt`.

## Summary

- `200m x 2`: MSC `10` RPS, profiled load `20` RPS, avg CPU `9.5m`, diagnosis: Throttle counters increased during the profiled load window. Memory stayed low, so memory pressure is unlikely to be the bottleneck.
- `200m x 3`: MSC `20` RPS, profiled load `30` RPS, avg CPU `9.3m`, diagnosis: Throttle counters increased during the profiled load window. Memory stayed low, so memory pressure is unlikely to be the bottleneck.
- `500m x 2`: MSC `10` RPS, profiled load `20` RPS, avg CPU `10.0m`, diagnosis: No single hard limit dominated; weak scaling likely comes from per-request fan-out plus coordination overhead. Memory stayed low, so memory pressure is unlikely to be the bottleneck.
- `500m x 3`: MSC `20` RPS, profiled load `30` RPS, avg CPU `9.7m`, diagnosis: No single hard limit dominated; weak scaling likely comes from per-request fan-out plus coordination overhead. Memory stayed low, so memory pressure is unlikely to be the bottleneck.

## Detailed Findings

### 200m x 2

- MSC search violation reason: `success_rate`
- Profile step: input `20` RPS, success `1.000`, effective `20.0` RPS, p50/p90/p99 `10.6/45.6/48.4` ms
- Pod `rate-6ffb8b54c-7lftp`: throttled periods `2`, throttled usec `252873`, avg voluntary/involuntary ctx switches per second `7.4/0.4`, run-queue wait `0.0` ms, cycles `332388293`, instructions `250770480`, cache misses `2146031`, task migrations `776`
- Pod `rate-6ffb8b54c-pn9tc`: throttled periods `0`, throttled usec `16074`, avg voluntary/involuntary ctx switches per second `6.7/0.3`, run-queue wait `0.0` ms, cycles `337382566`, instructions `261569124`, cache misses `1901319`, task migrations `804`

### 200m x 3

- MSC search violation reason: `success_rate`
- Profile step: input `30` RPS, success `0.982`, effective `29.5` RPS, p50/p90/p99 `41.7/47.4/130.8` ms
- Pod `rate-6ffb8b54c-7lftp`: throttled periods `2`, throttled usec `422359`, avg voluntary/involuntary ctx switches per second `8.3/0.5`, run-queue wait `0.0` ms, cycles `555380844`, instructions `546270060`, cache misses `2723963`, task migrations `969`
- Pod `rate-6ffb8b54c-pc7kj`: throttled periods `0`, throttled usec `0`, avg voluntary/involuntary ctx switches per second `6.6/0.4`, run-queue wait `0.0` ms, cycles `252726670`, instructions `141680930`, cache misses `1390592`, task migrations `735`
- Pod `rate-6ffb8b54c-pn9tc`: throttled periods `1`, throttled usec `49029`, avg voluntary/involuntary ctx switches per second `5.6/0.4`, run-queue wait `0.0` ms, cycles `250364001`, instructions `133926688`, cache misses `1405271`, task migrations `626`

### 500m x 2

- MSC search violation reason: `success_rate`
- Profile step: input `20` RPS, success `1.000`, effective `20.0` RPS, p50/p90/p99 `7.6/45.8/48.7` ms
- Pod `rate-776fd867f5-g7xjd`: throttled periods `0`, throttled usec `0`, avg voluntary/involuntary ctx switches per second `7.0/0.3`, run-queue wait `0.2` ms, cycles `305319879`, instructions `239105918`, cache misses `1730715`, task migrations `745`
- Pod `rate-776fd867f5-wmpxr`: throttled periods `0`, throttled usec `0`, avg voluntary/involuntary ctx switches per second `8.0/0.3`, run-queue wait `2.0` ms, cycles `338629790`, instructions `270013864`, cache misses `1922585`, task migrations `817`

### 500m x 3

- MSC search violation reason: `success_rate`
- Profile step: input `30` RPS, success `0.980`, effective `29.4` RPS, p50/p90/p99 `41.2/47.3/51.0` ms
- Pod `rate-776fd867f5-g7xjd`: throttled periods `0`, throttled usec `0`, avg voluntary/involuntary ctx switches per second `8.5/0.4`, run-queue wait `0.2` ms, cycles `548234071`, instructions `523837133`, cache misses `2506152`, task migrations `1017`
- Pod `rate-776fd867f5-wmpxr`: throttled periods `0`, throttled usec `0`, avg voluntary/involuntary ctx switches per second `6.1/0.3`, run-queue wait `0.0` ms, cycles `253131494`, instructions `140272594`, cache misses `1390920`, task migrations `633`
- Pod `rate-776fd867f5-xks5d`: throttled periods `0`, throttled usec `0`, avg voluntary/involuntary ctx switches per second `6.7/0.3`, run-queue wait `0.0` ms, cycles `271041218`, instructions `150407274`, cache misses `1545113`, task migrations `625`
