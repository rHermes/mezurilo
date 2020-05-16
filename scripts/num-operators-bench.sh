#!/bin/bash

set -e

FLINK_HOME="${HOME}/madsci/thesis/setup/flink-1.9.3"
FLINK_BIN="${FLINK_HOME}/bin/flink"
MEZURILO_JAR="${HOME}/madsci/thesis/codes/mezurilo/target/mezurilo-bundled-1.0-SNAPSHOT.jar"

TO_N=50000000
NUM_RUNS=10
NODES_FROM=0
NODES_TO=10
PARALLEL=1

printf "to_n\tnodes\trun\tduration\n"
for (( nodes=${NODES_FROM}; nodes < ${NODES_TO}; nodes++ )); do
  for (( run=0; run < ${NUM_RUNS}; run++ )); do
    # "
    DUR=$("${FLINK_BIN}" run -p ${PARALLEL} "$MEZURILO_JAR" FlinkNumOperators --to "${TO_N}" --nodes "${nodes}" |& sed -nE 's/Job Runtime: (.*) ms/\1/p')
    printf "%s\t%s\t%s\t%s\n" "${TO_N}" "${nodes}" "${run}" "${DUR}"
  done
done
