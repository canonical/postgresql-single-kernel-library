#!/usr/bin/env bash
# Collect Juju and workload logs after the spread job (used by .github/workflows/integration_test.yaml).
# Each command is run by its own CI step, so that its output can be read on the GitHub web UI;
# the output is also saved to <output dir> for the logs artifact. Per-unit/pod output is split in
# collapsible groups. Everything is best effort: a failing or hanging command does not stop the rest
# and the script always exits 0.
#
# Usage: collect_logs.sh <output dir> <vm|k8s> <command> [model]
#   status <model>         juju status
#   debug-log <model>      juju debug-log (needed by jhack)
#   jhack <model>          jhack tail of the saved debug-log
#   status-log <model>     juju show-status-log of every unit
#   workload-logs <model>  Patroni and PostgreSQL logs of every PostgreSQL unit
#   k8s-cluster            K8s pods and events of the whole cluster
#   k8s-describe           kubectl describe pods of every Juju model
#   k8s-pod-logs           logs (current and previous) of every pod of every Juju model
#   cilium                 Cilium status and endpoints
set -uo pipefail

OUT_DIR="$1"
SUBSTRATE="$2"
COMMAND="$3"
MODEL="${4:-}"
# Per-command limit, so that one stuck command cannot eat the whole step timeout
CMD_TIMEOUT=180

VM_LOGS_DIR=/var/snap/charmed-postgresql/common/var/log
K8S_LOGS_GLOB='/var/lib/pg/logs/*/main'

group() { echo "::group::$*"; }
endgroup() { echo "::endgroup::"; }

run() {
    # run <output file> <command...>: print the output and append it to <output file>
    local out="$1"
    shift
    mkdir -p "$(dirname "$out")"
    echo "+ $*" | tee -a "$out"
    timeout "$CMD_TIMEOUT" "$@" 2>&1 | tee -a "$out"
    local rc="${PIPESTATUS[0]}"
    [ "$rc" = 0 ] || echo "  failed ($rc): $*" | tee -a "$out"
}

run_stdin() {
    # run_stdin <output file> <stdin> <command...>: like run, feeding <stdin> to the command
    local out="$1" input="$2"
    shift 2
    mkdir -p "$(dirname "$out")"
    echo "+ $*" | tee -a "$out"
    timeout "$CMD_TIMEOUT" "$@" <<< "$input" 2>&1 | tee -a "$out"
    local rc="${PIPESTATUS[0]}"
    [ "$rc" = 0 ] || echo "  failed ($rc): $*" | tee -a "$out"
}

# Shell script printing the log files of <dirs> (glob allowed), oldest first
# (patroni.log.N is older than patroni.log.1, which is older than patroni.log)
cat_logs_script() {
    echo "for d in $1; do
        [ -d \"\$d\" ] || continue
        for f in \$(ls -1 \"\$d\" | sort -t. -k3,3nr -k1,1); do
            echo \"===== \$d/\$f\"; cat \"\$d/\$f\"
        done
    done"
}

juju_models() {
    juju models --format json | jq -r '.models[] | select(."is-controller" | not) | ."short-name"'
}

model_units() {
    # model_units <model> [jq filter on applications]
    local filter="${2:-.}"
    timeout "$CMD_TIMEOUT" juju status --model "$1" --format json \
        | jq -r ".applications // {} | to_entries[] | select($filter) | .value.units // {} | keys[]"
}

model_dir="$OUT_DIR/${MODEL:-k8s}"

case "$COMMAND" in
    status)
        if [ "$SUBSTRATE" = vm ]; then
            run "$model_dir/juju-status.txt" juju status --model "$MODEL" --relations --storage
        else
            run "$model_dir/juju-status.txt" juju status --model "$MODEL" --relations
        fi
        timeout "$CMD_TIMEOUT" juju status --model "$MODEL" --format yaml > "$model_dir/juju-status.yaml" 2>&1
        ;;
    debug-log)
        run "$model_dir/juju-debug-log.txt" juju debug-log --model "$MODEL" --replay --no-tail
        ;;
    jhack)
        # jhack reads the logs from stdin when it is not a TTY (as on CI), so feed it the debug-log
        run_stdin "$model_dir/jhack-tail.txt" "$(cat "$model_dir/juju-debug-log.txt" 2>/dev/null)" \
            jhack tail --printer raw
        ;;
    status-log)
        for unit in $(model_units "$MODEL"); do
            group "$unit"
            run "$model_dir/status-log-${unit//\//-}.txt" \
                juju show-status-log --model "$MODEL" "$unit" --days 1
            endgroup
        done
        ;;
    workload-logs)
        # `juju ssh`, not `juju exec`, so that it does not wait for the machine lock held by a stuck hook
        pg_filter='.value."charm-name" == "postgresql" or .value."charm-name" == "postgresql-k8s"'
        for unit in $(model_units "$MODEL" "$pg_filter"); do
            unit_file="${unit//\//-}"
            for service in patroni postgresql; do
                group "$unit $service"
                if [ "$SUBSTRATE" = vm ]; then
                    run_stdin "$model_dir/$service-$unit_file.txt" \
                        "$(cat_logs_script "$VM_LOGS_DIR/$service")" \
                        juju ssh --model "$MODEL" "$unit" -- sudo bash -s
                else
                    [ "$service" = patroni ] && dir=patroni_logs || dir=pg_logs
                    run_stdin "$model_dir/$service-$unit_file.txt" \
                        "$(cat_logs_script "$K8S_LOGS_GLOB/$dir")" \
                        juju ssh --model "$MODEL" --container postgresql "$unit" bash -s
                fi
                endgroup
            done
        done
        ;;
    k8s-cluster)
        group pods
        run "$model_dir/pods.txt" k8s kubectl get pods --all-namespaces --output wide
        endgroup
        group events
        run "$model_dir/events.txt" k8s kubectl get events --all-namespaces --sort-by=.lastTimestamp
        endgroup
        ;;
    k8s-describe)
        for model in $(juju_models); do
            group "$model"
            run "$model_dir/describe-pods-$model.txt" k8s kubectl --namespace "$model" describe pods
            endgroup
        done
        ;;
    k8s-pod-logs)
        for model in $(juju_models); do
            for pod in $(k8s kubectl --namespace "$model" get pods --output name | cut -d/ -f2); do
                group "$model/$pod"
                run "$model_dir/pod-logs-$model-$pod.txt" \
                    k8s kubectl --namespace "$model" logs "$pod" --all-containers --prefix
                endgroup
                group "$model/$pod (previous)"
                run "$model_dir/pod-logs-previous-$model-$pod.txt" \
                    k8s kubectl --namespace "$model" logs "$pod" --all-containers --prefix --previous
                endgroup
            done
        done
        ;;
    cilium)
        group status
        run "$model_dir/cilium-status.txt" \
            k8s kubectl --namespace kube-system exec daemonset/cilium -- cilium-dbg status --verbose
        endgroup
        group endpoints
        run "$model_dir/cilium-endpoints.txt" \
            k8s kubectl --namespace kube-system exec daemonset/cilium -- cilium-dbg endpoint list
        endgroup
        ;;
    *)
        echo "unknown command: $COMMAND"
        ;;
esac

exit 0
