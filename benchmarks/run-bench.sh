#!/usr/bin/env bash
# Persistent (file-mode) benchmark entrypoint for the Zhensegg broker.
#
# Starts the broker in durable file mode, then runs the honest closed-loop
# benchmark with producers and consumers on separate OS threads, then prints
# the collected RPS / latency figures and (optionally) verifies sampled
# ACKed records straight off the media via O_DIRECT.
set -euo pipefail

# ---- tunables ----
ADDR="${ADDR:-127.0.0.1:9090}"
HTTP_ADDR="${HTTP_ADDR:-127.0.0.1:9091}"
CORES="${CORES:-4}"                     # broker shards (SO_REUSEPORT / core affinity)
RING_MB="${RING_MB:-4096}"              # persistent ring file size in MiB
RING_FILE="${RING_FILE:-/data/ring.dat}"

TOPIC="${TOPIC:-bench}"
PRODUCERS="${PRODUCERS:-8}"
CONSUMERS="${CONSUMERS:-8}"             # >0 => different-thread consumer+producer test
BATCH="${BATCH:-256}"
PAYLOAD="${PAYLOAD:-256}"               # bytes, >=8 (first 8 carry latency ts)
MSGS="${MSGS:-0}"                       # 0 => run by seconds
SECS="${SECS:-30}"                      # benchmark duration in seconds (persistent soak)

# Security: TLS=1 terminates TLS on the data plane; AUTH_TOKEN (if non-empty)
# requires every client to present it before any other command.
TLS="${TLS:-0}"
AUTH_TOKEN="${AUTH_TOKEN:-}"

BROKER_EXTRA=()
BENCH_EXTRA=()

if [ "${TLS}" = "1" ]; then
    echo "[run] TLS enabled: generating self-signed cert for localhost"
    CERT=/data/tls.crt
    KEY=/data/tls.key
    openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 \
        -keyout "${KEY}" -out "${CERT}" -days 1 -nodes \
        -subj "/CN=localhost" \
        -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
        -addext "basicConstraints=critical,CA:FALSE" \
        -addext "keyUsage=digitalSignature,keyEncipherment" \
        -addext "extendedKeyUsage=serverAuth" \
        >/dev/null 2>&1
    BROKER_EXTRA+=(--tls-cert "${CERT}" --tls-key "${KEY}")
    BENCH_EXTRA+=(--tls --cafile "${CERT}")
fi

if [ -n "${AUTH_TOKEN}" ]; then
    echo "[run] auth enabled: shared token required"
    BROKER_EXTRA+=(--auth-token "${AUTH_TOKEN}")
    BENCH_EXTRA+=(--auth-token "${AUTH_TOKEN}")
fi

# ---- start the broker in persistent file mode ----
echo "[run] starting broker: mode=file file=${RING_FILE} ring=${RING_MB}MB cores=${CORES} tls=${TLS} auth=${AUTH_TOKEN:+yes}"
rm -f "${RING_FILE}"

/usr/local/bin/zhensegg-broker \
    --addr "${ADDR}" \
    --http-addr "${HTTP_ADDR}" \
    --mode file \
    --file "${RING_FILE}" \
    --ring-capacity-mb "${RING_MB}" \
    --cores "${CORES}" \
    "${BROKER_EXTRA[@]}" &
BROKER_PID=$!

# give the broker time to bind the sockets and spin up the flusher
sleep 3

if ! kill -0 "${BROKER_PID}" 2>/dev/null; then
    echo "[run] BROKER FAILED TO START" >&2
    exit 1
fi

echo "[run] broker pid=${BROKER_PID} | running persistent benchmark..."

# ---- honest benchmark: separate producer/consumer threads, closed loop ----
CMD_ARGS=(--addr "${ADDR}" --topic "${TOPIC}"
          --producers "${PRODUCERS}" --consumers "${CONSUMERS}"
          --batch "${BATCH}" --payload-size "${PAYLOAD}"
          --verify-file "${RING_FILE}"
          "${BENCH_EXTRA[@]}")

if [ "${MSGS}" -gt 0 ]; then
    CMD_ARGS+=(--msgs "${MSGS}")
else
    CMD_ARGS+=(--secs "${SECS}")
fi

set +e
/usr/local/bin/zhensegg-bench "${CMD_ARGS[@]}"
BENCH_RC=$?
set -e

# stop the broker (graceful drain happens on signal)
echo "[run] benchmark finished rc=${BENCH_RC}, stopping broker"
kill "${BROKER_PID}" 2>/dev/null || true
wait "${BROKER_PID}" 2>/dev/null || true

echo "[run] done."
exit "${BENCH_RC}"
