#!/usr/bin/env bash
# Collect Juju and workload logs after the spread job (used by .github/workflows/integration_test.yaml).
# The output of each command is printed in its own collapsible group (one per command and model, or per
# unit/pod), so that it can be read on the GitHub web UI, and saved to <output dir>/<model>/ (K8s cluster
# output to <output dir>/k8s/) for the logs artifact. Similar to data-platform-workflows
# tee-log-for-all-models (v42.1.0), but best effort: a failing or hanging command does not stop the rest
# and the script always exits 0.
#
# Usage: collect_logs.sh <output dir> <vm|k8s> <command> [model]
# Without [model], the per-model commands run for every Juju model.
#   all             everything below (K8s commands only on k8s)
#   status          juju status
#   debug-log       juju debug-log (needed by jhack)
#   jhack           jhack tail of the saved debug-log
#   status-log      juju show-status-log of every unit
#   workload-logs   Patroni and PostgreSQL logs of every PostgreSQL unit
#   k8s-cluster     K8s pods and events of the whole cluster
#   k8s-describe    kubectl describe pods
#   k8s-pod-logs    logs (current and previous) of every pod
#   cilium          Cilium status and endpoints
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

status() {
    local model="$1" dir="$OUT_DIR/$1"
    group "juju status · $model"
    if [ "$SUBSTRATE" = vm ]; then
        run "$dir/juju-status.txt" juju status --model "$model" --color --relations --storage
    else
        run "$dir/juju-status.txt" juju status --model "$model" --color --relations
    fi
    timeout "$CMD_TIMEOUT" juju status --model "$model" --format yaml > "$dir/juju-status.yaml" 2>&1
    endgroup
}

debug_log() {
    local model="$1" dir="$OUT_DIR/$1"
    group "juju debug-log · $model"
    run "$dir/juju-debug-log.txt" juju debug-log --model "$model" --color --replay --no-tail
    endgroup
}

jhack_tail() {
    # jhack reads the logs from stdin when it is not a TTY (as on CI), so feed it the debug-log saved
    # by debug_log (without the colour codes added by `juju debug-log --color`)
    local model="$1" dir="$OUT_DIR/$1"
    group "jhack tail · $model"
    run_stdin "$dir/jhack-tail.txt" \
        "$(sed 's/\x1b\[[0-9;]*m//g' "$dir/juju-debug-log.txt" 2>/dev/null)" \
        jhack tail --printer raw
    endgroup
}

status_log() {
    local model="$1" dir="$OUT_DIR/$1" unit
    for unit in $(model_units "$model"); do
        group "show-status-log · $model · $unit"
        run "$dir/status-log-${unit//\//-}.txt" juju show-status-log --model "$model" "$unit" --days 1
        endgroup
    done
}

workload_logs() {
    # `juju ssh`, not `juju exec`, so that it does not wait for the machine lock held by a stuck hook
    local model="$1" dir="$OUT_DIR/$1" unit service name logs_dir
    local pg_filter='.value."charm-name" == "postgresql" or .value."charm-name" == "postgresql-k8s"'
    for unit in $(model_units "$model" "$pg_filter"); do
        for service in patroni postgresql; do
            [ "$service" = patroni ] && name=Patroni || name=PostgreSQL
            group "$name · $model · $unit"
            if [ "$SUBSTRATE" = vm ]; then
                run_stdin "$dir/$service-${unit//\//-}.txt" \
                    "$(cat_logs_script "$VM_LOGS_DIR/$service")" \
                    juju ssh --model "$model" "$unit" -- sudo bash -s
            else
                [ "$service" = patroni ] && logs_dir=patroni_logs || logs_dir=pg_logs
                run_stdin "$dir/$service-${unit//\//-}.txt" \
                    "$(cat_logs_script "$K8S_LOGS_GLOB/$logs_dir")" \
                    juju ssh --model "$model" --container postgresql "$unit" bash -s
            fi
            endgroup
        done
    done
}

k8s_cluster() {
    group "k8s pods"
    run "$K8S_DIR/pods.txt" k8s kubectl get pods --all-namespaces --output wide
    endgroup
    group "k8s events"
    run "$K8S_DIR/events.txt" k8s kubectl get events --all-namespaces --sort-by=.lastTimestamp
    endgroup
}

k8s_describe() {
    local model="$1"
    group "k8s describe pods · $model"
    run "$K8S_DIR/describe-pods-$model.txt" k8s kubectl --namespace "$model" describe pods
    endgroup
}

k8s_pod_logs() {
    local model="$1" pod
    for pod in $(k8s kubectl --namespace "$model" get pods --output name | cut -d/ -f2); do
        group "k8s pod logs · $model · $pod"
        run "$K8S_DIR/pod-logs-$model-$pod.txt" \
            k8s kubectl --namespace "$model" logs "$pod" --all-containers --prefix
        endgroup
        group "k8s pod logs (previous) · $model · $pod"
        run "$K8S_DIR/pod-logs-previous-$model-$pod.txt" \
            k8s kubectl --namespace "$model" logs "$pod" --all-containers --prefix --previous
        endgroup
    done
}

cilium() {
    group "Cilium status"
    run "$K8S_DIR/cilium-status.txt" \
        k8s kubectl --namespace kube-system exec daemonset/cilium -- cilium-dbg status --verbose
    endgroup
    group "Cilium endpoints"
    run "$K8S_DIR/cilium-endpoints.txt" \
        k8s kubectl --namespace kube-system exec daemonset/cilium -- cilium-dbg endpoint list
    endgroup
}

for_each_model() {
    # for_each_model <function>: run <function> for every Juju model
    local model
    for model in $MODELS; do
        "$1" "$model"
    done
}

K8S_DIR="$OUT_DIR/k8s"
MODELS="${MODEL:-$(juju_models)}"

case "$COMMAND" in
    all)
        group "juju models"
        run "$OUT_DIR/juju-models.txt" juju models
        endgroup
        for_each_model status
        for_each_model debug_log
        for_each_model jhack_tail
        for_each_model status_log
        for_each_model workload_logs
        if [ "$SUBSTRATE" = k8s ]; then
            k8s_cluster
            for_each_model k8s_describe
            for_each_model k8s_pod_logs
            cilium
        fi
        ;;
    status) for_each_model status ;;
    debug-log) for_each_model debug_log ;;
    jhack) for_each_model jhack_tail ;;
    status-log) for_each_model status_log ;;
    workload-logs) for_each_model workload_logs ;;
    k8s-cluster) k8s_cluster ;;
    k8s-describe) for_each_model k8s_describe ;;
    k8s-pod-logs) for_each_model k8s_pod_logs ;;
    cilium) cilium ;;
    *)
        echo "unknown command: $COMMAND"
        ;;
esac

exit 0
