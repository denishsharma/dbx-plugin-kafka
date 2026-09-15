#!/usr/bin/env python3
"""Container orchestration for the Kafka smoke test (scenarios S1-S17).

Wraps docker-compose.kafka-test.yml (apache/kafka KRaft single node on
127.0.0.1:9092):

    python3 scripts/smoke_container.py [--keep]

* PLAINTEXT test container: no credentials exist, so nothing sensitive is
  passed through the environment (if a SASL/TLS variant is added, every
  secret must come from the environment via environment variables);
* readiness is probed with a real metadata request (kafka-topics.sh --list
  inside the container), never a bare port probe; the Redpanda Schema
  Registry is probed with GET /subjects so S11/S14 run instead of SKIP; the
  SASL broker (S16) is probed with a PLAIN-authenticated metadata request;
  the TLS broker (S17) is probed with a full mTLS handshake against the
  generated CA;
* topics are seeded via scripts/kafka-seed/seed-topics.sh (idempotent,
  no credentials);
* after smoke_test.py, smoke_mcp.py runs against the same cluster so the
  MCP digest/cursor/two-phase-write scenarios hit a live broker;
* --keep leaves the containers (and the TLS secrets dir) running for
  debugging, default tears them down.
"""

from __future__ import annotations

import argparse
import base64
import os
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
COMPOSE_FILE = REPO / "docker-compose.kafka-test.yml"
# Fixed container_name values from docker-compose.kafka-test.yml. A leftover
# container from an interrupted/kept run (or from a sibling compose project)
# blocks `compose up` with a name conflict, so they are force-removed first.
CONTAINER = "dbx-kafka-test"
SR_CONTAINER = "dbx-kafka-sr-test"
SASL_CONTAINER = "dbx-kafka-sasl-test"
TLS_CONTAINER = "dbx-kafka-tls-test"
HOST = "127.0.0.1"
PORT = 9092
INNER_BOOTSTRAP = "localhost:9092"
READY_TIMEOUT_SECS = 120
# Redpanda SR (Confluent-compatible REST, scenario S11/S14). Without this wait
# the SR scenarios would silently SKIP in CI whenever redpanda loses the
# startup race against apache/kafka + seeding.
SR_HOST = "127.0.0.1"
SR_PORT = 19081
SR_READY_TIMEOUT_SECS = 120
# SASL broker (scenario S16): SASL_PLAINTEXT + PLAIN mechanism cluster on
# its own port, separate KRaft quorum from the PLAINTEXT cluster.
SASL_HOST = "127.0.0.1"
SASL_PORT = 29092
SASL_READY_TIMEOUT_SECS = 180
# TLS broker (scenario S17): SSL listener with ssl.client.auth=required
# (mTLS) on its own port; server + client certificates are throwaway,
# generated per run with openssl into KAFKA_TEST_TLS_SECRETS_DIR.
TLS_HOST = "127.0.0.1"
TLS_PORT = 30092
TLS_READY_TIMEOUT_SECS = 180
TLS_STORE_PASSWORD = "dbx-smoke-store"


def sh(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
    print("+", " ".join(cmd), flush=True)
    return subprocess.run(cmd, **kwargs)


def compose(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return sh(["docker", "compose", "-f", str(COMPOSE_FILE), *args], check=check)


def port_open() -> bool:
    try:
        with socket.create_connection((HOST, PORT), timeout=1.0):
            return True
    except OSError:
        return False


def metadata_probe() -> subprocess.CompletedProcess:
    """Real metadata/list request inside the container (not a port probe)."""
    return subprocess.run(
        [
            "docker", "exec", CONTAINER,
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server", INNER_BOOTSTRAP,
            "--list",
        ],
        capture_output=True,
    )


def wait_ready() -> None:
    deadline = time.monotonic() + READY_TIMEOUT_SECS
    while time.monotonic() < deadline:
        if port_open():
            result = metadata_probe()
            if result.returncode == 0:
                time.sleep(1)
                return
        time.sleep(2)
    raise SystemExit(f"Kafka container not answering metadata requests on {HOST}:{PORT} after {READY_TIMEOUT_SECS}s")


def sr_answered() -> bool:
    try:
        with urllib.request.urlopen(f"http://{SR_HOST}:{SR_PORT}/subjects", timeout=2) as response:
            return response.status == 200
    except (OSError, urllib.error.URLError):
        return False


def wait_sr_ready() -> None:
    deadline = time.monotonic() + SR_READY_TIMEOUT_SECS
    while time.monotonic() < deadline:
        if sr_answered():
            return
        time.sleep(2)
    raise SystemExit(f"Schema Registry container not answering GET /subjects on {SR_HOST}:{SR_PORT} after {SR_READY_TIMEOUT_SECS}s")


def sasl_metadata_probe() -> subprocess.CompletedProcess:
    """Real metadata/list request over SASL/PLAIN (client properties via sh -c).

    The admin password is passed as a positional sh argument (never baked
    into the script text) and lives only in a throwaway container file.
    """
    admin_password = os.environ["KAFKA_TEST_SASL_ADMIN_PASSWORD"]
    script = (
        "printf 'security.protocol=SASL_PLAINTEXT\\n"
        "sasl.mechanism=PLAIN\\n"
        "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule "
        'required username="admin" password="%s";\\n\' "$1" > /tmp/sasl-client.properties && '
        "exec /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 "
        "--command-config /tmp/sasl-client.properties --list"
    )
    return subprocess.run(
        ["docker", "exec", SASL_CONTAINER, "sh", "-c", script, "sh", admin_password],
        capture_output=True,
    )


def wait_sasl_ready() -> None:
    deadline = time.monotonic() + SASL_READY_TIMEOUT_SECS
    while time.monotonic() < deadline:
        if sasl_metadata_probe().returncode == 0:
            return
        time.sleep(2)
    raise SystemExit(f"SASL broker not answering PLAIN-auth metadata requests on {SASL_HOST}:{SASL_PORT} after {SASL_READY_TIMEOUT_SECS}s")


def generate_tls_secrets(secrets_dir: Path) -> None:
    """Throwaway CA + server/client certs for the mTLS broker (S17).

    Everything is regenerated per run and never leaves this temp directory
    (plus the mounted container view); 3-day validity keeps any accidental
    reuse harmless. The server cert SAN must cover both localhost and
    127.0.0.1 because the smoke suite connects by IP while the readiness
    probe verifies by hostname.
    """
    def openssl(*args: str) -> None:
        subprocess.run(["openssl", *args], check=True, capture_output=True)

    ca_key, ca_cert = secrets_dir / "ca.key", secrets_dir / "ca.pem"
    server_key, server_csr, server_cert = (secrets_dir / n for n in ("server.key", "server.csr", "server.pem"))
    client_key, client_csr, client_cert = (secrets_dir / n for n in ("client.key", "client.csr", "client.pem"))
    openssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", str(ca_key),
            "-out", str(ca_cert), "-days", "3", "-subj", "/CN=dbx-kafka-smoke-ca",
            "-addext", "basicConstraints=critical,CA:TRUE",
            "-addext", "keyUsage=critical,keyCertSign,cRLSign,digitalSignature")
    openssl("req", "-newkey", "rsa:2048", "-nodes", "-keyout", str(server_key),
            "-out", str(server_csr), "-subj", "/CN=localhost")
    # EKU includes clientAuth because the broker's inter-broker connection
    # reuses the server keystore as a TLS client; a serverAuth-only cert
    # fails Java's extended-key-usage check on that path.
    (secrets_dir / "server.ext").write_text(
        "subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth,clientAuth\n")
    openssl("x509", "-req", "-in", str(server_csr), "-CA", str(ca_cert), "-CAkey", str(ca_key),
            "-CAcreateserial", "-out", str(server_cert), "-days", "3",
            "-extfile", str(secrets_dir / "server.ext"))
    openssl("req", "-newkey", "rsa:2048", "-nodes", "-keyout", str(client_key),
            "-out", str(client_csr), "-subj", "/CN=dbx-kafka-smoke-client")
    (secrets_dir / "client.ext").write_text("extendedKeyUsage=clientAuth\n")
    openssl("x509", "-req", "-in", str(client_csr), "-CA", str(ca_cert), "-CAkey", str(ca_key),
            "-CAcreateserial", "-out", str(client_cert), "-days", "3",
            "-extfile", str(secrets_dir / "client.ext"))
    # Broker keystore: PKCS12 with server cert + key + CA chain. Java reads
    # this one fine. The truststore is NOT built with openssl: Java's PKCS12
    # reader drops cert-only bundles ("0 entries"), so it is generated by the
    # image's own keytool and pulled back out via base64 (read-only mount +
    # stdout keep this permission-safe on CI runners).
    openssl("pkcs12", "-export", "-inkey", str(server_key), "-in", str(server_cert),
            "-certfile", str(ca_cert), "-name", "broker",
            "-passout", f"pass:{TLS_STORE_PASSWORD}", "-out", str(secrets_dir / "broker.p12"))
    (secrets_dir / "keystore.password").write_text(TLS_STORE_PASSWORD)
    (secrets_dir / "truststore.password").write_text(TLS_STORE_PASSWORD)
    # mkdtemp dirs are 0700 owned by the runner user, but every container
    # involved (keytool one-shot and the broker, uid 1000) reads the mount
    # through a read-only bind: world-traversable dir + world-readable files
    # are required there (macOS Docker Desktop hides this behind its uid
    # mapping, CI Linux runners do not).
    secrets_dir.chmod(0o755)
    for path in secrets_dir.iterdir():
        path.chmod(0o644)
    trust = subprocess.run(
        ["docker", "run", "--rm", "-v", f"{secrets_dir}:/work:ro", "--entrypoint", "sh",
         "apache/kafka:latest", "-c",
         "keytool -importcert -alias ca -file /work/ca.pem -keystore /tmp/truststore.p12 "
         f"-storetype PKCS12 -storepass '{TLS_STORE_PASSWORD}' -noprompt >/dev/null && base64 /tmp/truststore.p12"],
        capture_output=True,
    )
    if trust.returncode != 0:
        raise SystemExit(
            "keytool truststore build failed: "
            + trust.stderr.decode(errors="replace").strip()
        )
    (secrets_dir / "truststore.p12").write_bytes(base64.b64decode(trust.stdout))


def tls_answered() -> bool:
    """Full mTLS handshake against the generated CA (not a port probe)."""
    try:
        context = ssl.create_default_context(cafile=os.environ["KAFKA_TEST_TLS_CA"])
        context.load_cert_chain(os.environ["KAFKA_TEST_TLS_CLIENT_CERT"], os.environ["KAFKA_TEST_TLS_CLIENT_KEY"])
        with socket.create_connection((TLS_HOST, TLS_PORT), timeout=2) as sock:
            with context.wrap_socket(sock, server_hostname="localhost") as tls:
                return tls.version() is not None
    except (OSError, ssl.SSLError, KeyError):
        return False


def wait_tls_ready() -> None:
    deadline = time.monotonic() + TLS_READY_TIMEOUT_SECS
    while time.monotonic() < deadline:
        if tls_answered():
            return
        time.sleep(2)
    raise SystemExit(f"TLS broker not completing mTLS handshakes on {TLS_HOST}:{TLS_PORT} after {TLS_READY_TIMEOUT_SECS}s")


def clear_stale_containers() -> None:
    for name in (CONTAINER, SR_CONTAINER, SASL_CONTAINER, TLS_CONTAINER):
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--keep", action="store_true", help="leave the container running")
    args = parser.parse_args()

    # Throwaway credentials for the SASL variant (S16). The compose file
    # refuses to start the SASL broker without them (${VAR:?} interpolation),
    # so invoking docker compose directly still forces an explicit export;
    # the orchestrator just supplies deterministic test values.
    os.environ.setdefault("KAFKA_TEST_SASL_ADMIN_PASSWORD", "admin-smoke-secret")
    os.environ.setdefault("KAFKA_TEST_SASL_PASSWORD", "smoke-secret")
    os.environ.setdefault("KAFKA_TEST_SASL_USERNAME", "smoke")

    # TLS variant (S17): generate the per-run CA/keystore/truststore and
    # point compose (${KAFKA_TEST_TLS_SECRETS_DIR:?}) at it.
    tls_secrets = Path(tempfile.mkdtemp(prefix="dbx-kafka-tls-secrets-"))
    generate_tls_secrets(tls_secrets)
    os.environ["KAFKA_TEST_TLS_SECRETS_DIR"] = str(tls_secrets)
    os.environ["KAFKA_TEST_TLS_CA"] = str(tls_secrets / "ca.pem")
    os.environ["KAFKA_TEST_TLS_CLIENT_CERT"] = str(tls_secrets / "client.pem")
    os.environ["KAFKA_TEST_TLS_CLIENT_KEY"] = str(tls_secrets / "client.key")
    if args.keep:
        print(f"==> TLS secrets in {tls_secrets} (--keep: not deleted)", flush=True)

    clear_stale_containers()
    compose("up", "-d")
    try:
        wait_ready()
        wait_sr_ready()
        wait_sasl_ready()
        wait_tls_ready()
        sh(["bash", str(REPO / "scripts" / "kafka-seed" / "seed-topics.sh")])
        env = os.environ.copy()
        env.update(
            KAFKA_TEST_HOST=HOST,
            KAFKA_TEST_PORT=str(PORT),
        )
        result = sh([sys.executable, str(REPO / "scripts" / "smoke_test.py")], env=env)
        if result.returncode != 0:
            return result.returncode
        # MCP tool face over stdio (K1-K19): the container-backed scenarios
        # (digest/cursor/two-phase writes/pool churn) need the same cluster.
        mcp = sh([sys.executable, str(REPO / "scripts" / "smoke_mcp.py")], env=env)
        return mcp.returncode
    finally:
        if not args.keep:
            compose("down", "-v", check=False)
            shutil.rmtree(tls_secrets, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
