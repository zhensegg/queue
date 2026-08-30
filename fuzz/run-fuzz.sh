#!/usr/bin/env bash
# Zhensegg protocol fuzz + binary soak (Linux container entrypoint).
#
#   docker build -f fuzz/Dockerfile -t zhensegg-fuzz .
#   docker run --rm zhensegg-fuzz
#
# Does three distinct things in one container, failing fast on any regression:
#   1. In-process, coverage-guided mutation fuzzing of the real wire parser
#      (the unsafe zero-copy entry point).
#   2. A TCP dispatch soak against a *running* broker: parallel flooders send
#      mixed garbage + well-formed frames for SOAK_SECS, then a fresh-connection
#      Ping probe confirms the broker still parses and answers. This runs over
#      plain TCP + shared-token auth so the length-prefix parser and the
#      publish/subscribe/fetch dispatcher are exercised end to end through the
#      real process.
#   3. A live TLS-mode broker checks the SIGHUP rotation path (the thing that
#      cannot be exercised on Windows): it re-reads the TLS cert/key and the
#      data-plane + HTTP token from disk on HUP; we rotate both and prove the
#      new HTTP token is served and the process survives.
set -euo pipefail

ADDR="${ADDR:-127.0.0.1:9090}"
SOAK_ADDR="${SOAK_ADDR:-127.0.0.1:9092}"
HTTP_ADDR="${HTTP_ADDR:-127.0.0.1:9091}"
CORES="${CORES:-2}"
FUZZ_SECS="${FUZZ_SECS:-20}"
FUZZ_ITERS="${FUZZ_ITERS:-0}"           # 0 => fuzz by time only
SOAK_CONNS="${SOAK_CONNS:-8}"
SOAK_SECS="${SOAK_SECS:-15}"
WORK=/fuzz
mkdir -p /data "${WORK}"

fail() { echo "ERROR: $*" >&2; exit 1; }

stop_broker() { # stop_broker PID
    [ -n "${1:-}" ] && kill -TERM "$1" 2>/dev/null || true
}

gen_cert() { # gen_cert NAME
    local name="$1"
    openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 \
        -keyout "/data/${name}.key" -out "/data/${name}.crt" -days 1 -nodes \
        -subj "/CN=localhost" \
        -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" 2>/dev/null
}
gen_cert cert1

# ---- 1. In-process protocol fuzzing ---------------------------------------
echo "[fuzz] coverage-guided fuzzing of the wire parser ..."
CORPUS="${WORK}/corpus"; CRASH="${WORK}/crash"
rm -rf "${CORPUS}" "${CRASH}"
if [ "${FUZZ_ITERS}" != "0" ]; then
    zhensegg-fuzz check --seconds "${FUZZ_SECS}" --iters "${FUZZ_ITERS}" \
        --corpus "${CORPUS}" --crash "${CRASH}"
else
    zhensegg-fuzz check --seconds "${FUZZ_SECS}" \
        --corpus "${CORPUS}" --crash "${CRASH}"
fi || fail "parser fuzzing found a crashing input (see ${CRASH})"
[ -n "$(find "${CRASH}" -type f 2>/dev/null | head -n1)" ] \
    && fail "crash corpus is not empty"

# ---- 2a. Plain-TCP broker with auth: dispatch soak -------------------------
echo "[soak] starting plain-TCP broker on ${SOAK_ADDR} (auth on) ..."
# Use a dedicated HTTP plane so it never collides with the TLS broker's HTTP
# server (which also keeps the rotation curl checks pointing at the right one).
PLAIN_HTTP="${PLAIN_HTTP:-127.0.0.1:9093}"
echo -n "token-plain" > /data/plain.token
zhensegg-broker \
    --addr "${SOAK_ADDR}" \
    --http-addr "${PLAIN_HTTP}" \
    --mode file --file /data/plain-ring.dat --ring-capacity-mb 512 --cores "${CORES}" \
    --auth-token-file /data/plain.token \
    --auth-timeout-secs 10 &
SOAK_PID=$!
trap 'stop_broker "${SOAK_PID}"; stop_broker "${TLS_PID:-" "}"; exit 1' ERR

sleep 1
kill -0 "${SOAK_PID}" || fail "plain-TCP broker exited during startup"

echo "[soak] flooding broker (${SOAK_CONNS} conns x ${SOAK_SECS}s) ..."
zhensegg-fuzz soak --addr "${SOAK_ADDR}" --auth-token /data/plain.token \
    --conns "${SOAK_CONNS}" --seconds "${SOAK_SECS}" \
    || fail "dispatch soak failed"
stop_broker "${SOAK_PID}"
unset SOAK_PID

# ---- 2b. TLS broker + live SIGHUP rotation --------------------------------
echo "[rotate] starting TLS broker on ${ADDR} (TLS + token auth + HTTP auth) ..."
echo -n "token-A" > /data/auth.token
zhensegg-broker \
    --addr "${ADDR}" --http-addr "${HTTP_ADDR}" \
    --mode file --file /data/ring.dat --ring-capacity-mb 512 --cores "${CORES}" \
    --tls-cert /data/cert1.crt --tls-key /data/cert1.key \
    --auth-token-file /data/auth.token \
    --http-auth-token-file /data/auth.token \
    --http-loopback-only \
    --auth-timeout-secs 10 &
TLS_PID=$!

for i in $(seq 1 50); do
    if kill -0 "${TLS_PID}" 2>/dev/null && (echo > /dev/tcp/127.0.0.1/9091) 2>/dev/null; then
        break
    fi
    sleep 0.2
done
kill -0 "${TLS_PID}" || fail "TLS broker exited during startup"

# HTTP admin with token-A must answer.
S1=$(curl -s -o /dev/null -w "%{http_code}" -u admin:token-A "http://${HTTP_ADDR}/metrics" || true)
[ "${S1}" = "200" ] || fail "admin /metrics with token-A returned ${S1} (want 200)"

echo "[rotate] rotating TLS cert + token, sending SIGHUP to ${TLS_PID} ..."
gen_cert cert2
echo -n "token-B" > /data/auth.token
kill -HUP "${TLS_PID}" || fail "cannot SIGHUP broker"
sleep 1
kill -0 "${TLS_PID}" || fail "broker died after SIGHUP rotation"

echo "[rotate] checking new HTTP token is served ..."
S2=$(curl -s -o /dev/null -w "%{http_code}" -u admin:token-B "http://${HTTP_ADDR}/metrics" || true)
[ "${S2}" = "200" ] || fail "admin /metrics with token-B returned ${S2} (rotation did not take effect)"

echo "[rotate] confirming old HTTP token is now rejected, process survives ..."
S3=$(curl -s -o /dev/null -w "%{http_code}" -u admin:token-A "http://${HTTP_ADDR}/metrics" || true)
[ "${S3}" = "401" ] || fail "old token-A still accepted (got ${S3}), rotation failed"
kill -0 "${TLS_PID}" || fail "broker died after rejecting old token"

stop_broker "${TLS_PID}"
wait "${TLS_PID}" 2>/dev/null || true
trap - ERR EXIT

echo "ALL OK: parser fuzz passed, dispatch soak passed, live SIGHUP TLS+token rotation verified"
