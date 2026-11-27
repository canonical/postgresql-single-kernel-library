#!/usr/bin/env bash
# Collect Juju and workload logs of every model on the current controller (used by CI after the
# spread job). Every command is best effort: a failing or hanging one must not stop the rest.
#
# Usage: collect_logs.sh <output dir> <vm|k8s>
set -uo pipefail

OUT_DIR="$1"
SUBSTRATE="$2"
# Generous per-command limit, so that one stuck command cannot eat the whole step timeout
CMD_TIMEOUT=180

VM_LOGS_DIR=/var/snap/charmed-postgresql/common/var/log
K8S_LOGS_GLOB='/var/lib/pg/logs/*/main'

run() {
    # run <output file> <command...>
    local out="$1"
    shift
    echo "+ $*" >> "$OUT_DIR/collect_logs.txt"
    timeout "$CMD_TIMEOUT" "$@" > "$out" 2>&1 || echo "  failed ($?): $*" >> "$OUT_DIR/collect_logs.txt"
}

run_script() {
    # run_script <output file> <script> <command...>: like run, feeding <script> on stdin
    local out="$1" script="$2"
    shift 2
    echo "+ $*" >> "$OUT_DIR/collect_logs.txt"
    timeout "$CMD_TIMEOUT" "$@" <<< "$script" > "$out" 2>&1 \
        || echo "  failed ($?): $*" >> "$OUT_DIR/collect_logs.txt"
}

mkdir -p "$OUT_DIR"

# Concatenate rotated log files oldest first (patroni.log.N is older than patroni.log.1,
# which is older than patroni.log)
cat_logs_script() {
    local dir="$1"
    echo "for d in $dir; do
        [ -d \"\$d\" ] || continue
        for f in \$(ls -1 \"\$d\" | sort -t. -k3,3nr -k1,1); do
            echo \"===== \$d/\$f\"; cat \"\$d/\$f\"
        done
    done"
}

run "$OUT_DIR/juju-models.txt" juju models

if [ "$SUBSTRATE" = k8s ]; then
    run "$OUT_DIR/k8s-pods.txt" k8s kubectl get pods --all-namespaces --output wide
    run "$OUT_DIR/k8s-events.txt" k8s kubectl get events --all-namespaces --sort-by=.lastTimestamp
    run "$OUT_DIR/k8s-cilium.txt" k8s kubectl --namespace kube-system exec daemonset/cilium -- cilium-dbg status --verbose
    run "$OUT_DIR/k8s-cilium-endpoints.txt" k8s kubectl --namespace kube-system exec daemonset/cilium -- cilium-dbg endpoint list
fi

models=$(juju models --format json | jq -r '.models[] | select(."is-controller" | not) | ."short-name"')
for model in $models; do
    model_dir="$OUT_DIR/$model"
    mkdir -p "$model_dir"

    if [ "$SUBSTRATE" = vm ]; then
        run "$model_dir/juju-status.txt" juju status --model "$model" --relations --storage
    else
        run "$model_dir/juju-status.txt" juju status --model "$model" --relations
    fi
    run "$model_dir/juju-status.yaml" juju status --model "$model" --format yaml
    run "$model_dir/juju-debug-log.txt" juju debug-log --model "$model" --replay --no-tail
    # jhack reads the logs from stdin when it is not a TTY (as on CI), so feed it the debug-log
    run_script "$model_dir/jhack-tail.txt" "$(cat "$model_dir/juju-debug-log.txt")" \
        jhack tail --printer raw

    if [ "$SUBSTRATE" = k8s ]; then
        run "$model_dir/k8s-describe-pods.txt" k8s kubectl --namespace "$model" describe pods
    fi

    status_json=$(timeout "$CMD_TIMEOUT" juju status --model "$model" --format json) || continue
    units=$(jq -r '.applications // {} | to_entries[] | .value.units // {} | keys[]' <<< "$status_json")
    for unit in $units; do
        unit_file="${unit//\//-}"
        run "$model_dir/status-log-$unit_file.txt" juju show-status-log --model "$model" "$unit" --days 1
        if [ "$SUBSTRATE" = k8s ]; then
            run "$model_dir/k8s-pod-logs-$unit_file.txt" k8s kubectl --namespace "$model" logs "$unit_file" --all-containers --prefix
            run "$model_dir/k8s-pod-logs-previous-$unit_file.txt" k8s kubectl --namespace "$model" logs "$unit_file" --all-containers --prefix --previous
        fi
    done

    # Patroni and PostgreSQL logs of the PostgreSQL units. `juju ssh` (not `juju exec`) so that
    # it does not wait for the machine lock held by a stuck hook.
    pg_units=$(jq -r '.applications // {} | to_entries[]
        | select(.value."charm-name" == "postgresql" or .value."charm-name" == "postgresql-k8s")
        | .value.units // {} | keys[]' <<< "$status_json")
    for unit in $pg_units; do
        unit_file="${unit//\//-}"
        if [ "$SUBSTRATE" = vm ]; then
            run_script "$model_dir/patroni-$unit_file.txt" "$(cat_logs_script "$VM_LOGS_DIR/patroni")" \
                juju ssh --model "$model" "$unit" -- sudo bash -s
            run_script "$model_dir/postgresql-$unit_file.txt" "$(cat_logs_script "$VM_LOGS_DIR/postgresql")" \
                juju ssh --model "$model" "$unit" -- sudo bash -s
        else
            run_script "$model_dir/patroni-$unit_file.txt" "$(cat_logs_script "$K8S_LOGS_GLOB/patroni_logs")" \
                juju ssh --model "$model" --container postgresql "$unit" bash -s
            run_script "$model_dir/postgresql-$unit_file.txt" "$(cat_logs_script "$K8S_LOGS_GLOB/pg_logs")" \
                juju ssh --model "$model" --container postgresql "$unit" bash -s
        fi
    done
done

exit 0
