import os
import subprocess
import sys
import time

import redis

# Import the TestConfig class to access Redis configuration
from test_rapid_analysis import TestConfig  # type: ignore[import-not-found]

from lsst.rubintv.production.predicates import getDoRaise


def check_redis_process(expect_running: bool = False) -> bool:
    """Check if this user has a redis-server running, using pgrep.

    Scoped to ``$USER`` so a colleague's redis-server on a shared dev node
    doesn't affect the result, matching ``RedisManager.is_redis_running`` in
    ``test_rapid_analysis.py``.
    """
    try:
        # Run pgrep to find this user's redis-server processes
        result = subprocess.run(
            ["pgrep", "-u", os.environ["USER"], "-f", "redis-server"], capture_output=True, text=True
        )

        # Get process IDs if any
        redis_pids = result.stdout.strip().split("\n") if result.stdout.strip() else []

        # Check if the expectation matches reality
        is_running = bool(redis_pids and redis_pids[0])
        if is_running != expect_running:
            state = "running" if is_running else "not running"
            expected = "to be running" if expect_running else "not to be running"
            print(f"Redis is {state} but expected {expected}")
            if is_running:
                print(f"Redis PIDs: {redis_pids}")
            return False
        return True
    except Exception as e:
        print(f"Error checking Redis process: {e}")
        return False


def print_redis_server_output(redis_process: subprocess.Popen) -> None:
    """Print the exit code and captured output of a dead redis-server.

    Only call this once the process has exited (``poll()`` is not ``None``),
    otherwise reading the pipes blocks. Without this, a redis-server that
    dies at startup (e.g. port already in use, corrupt dump.rdb in the cwd)
    fails silently because its output goes to unread pipes.
    """
    stdout, stderr = redis_process.communicate()
    print(f"redis-server exited with code {redis_process.returncode}")
    if stdout:
        print(f"redis-server stdout:\n{stdout.decode(errors='replace')}")
    if stderr:
        print(f"redis-server stderr:\n{stderr.decode(errors='replace')}")


def start_test_redis() -> tuple[subprocess.Popen, str, str, str]:
    """Start a Redis server for testing."""
    # Get Redis configuration
    config = TestConfig()
    host = config.redis_host
    port = config.redis_port
    password = config.redis_password

    # Check if Redis is already running
    if check_redis_process(expect_running=True):
        raise RuntimeError("Redis server is already running. Cannot start another instance.")

    # Set environment variables
    os.environ["REDIS_HOST"] = host
    os.environ["REDIS_PORT"] = port
    os.environ["REDIS_PASSWORD"] = password

    print(f"Starting Redis on {host}:{port}")
    # --save "" disables RDB snapshots (including the save-on-SIGTERM),
    # so the test redis never writes a dump.rdb into the launch directory
    # for a later startup to load.
    redis_process = subprocess.Popen(
        ["redis-server", "--port", port, "--bind", host, "--requirepass", password, "--save", ""],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    print(f"Started Redis server with PID: {redis_process.pid}")

    # Wait a maximum of 15 seconds for Redis to start and accept connections.
    # Return regardless, as the real check is elsewhere, this is just so we
    # don't do a blind sleep or an indefinite wait.
    start_time = time.time()
    while time.time() - start_time < 15:
        if redis_process.poll() is not None:
            print("redis-server process exited during startup - aborting wait")
            break
        if check_redis_connection(host, port, password):
            print(f"Redis server came up fully in {time.time() - start_time:.2f} seconds")
            break
        print("Waiting for Redis server to start...")
        time.sleep(0.5)

    return redis_process, host, port, password


def check_redis_connection(host: str, port: str, password: str) -> bool:
    """Check if Redis connection works."""
    try:
        r = redis.Redis(host=host, port=int(port), password=password)

        # Ping Redis
        if not r.ping():
            print("Could not ping Redis")
            return False

        # Set and read back a test key
        r.set("test_key", "test_value")
        raw = r.get("test_key")
        if raw is None:
            print("Could not set and read back a test key in Redis")
            return False
        value = raw.decode("utf-8")
        if value != "test_value":
            print("Could not set and read back a test key in Redis")
            return False

        r.flushall()  # Clear the database
        return True
    except Exception as e:
        print(f"Redis connection error: {e}")
        return False


def stop_redis(process: subprocess.Popen | None) -> None:
    """Stop the Redis server."""
    if process:
        process.terminate()
        process.wait(timeout=5)
        print(f"Terminated Redis process PID: {process.pid}")


def main() -> int:
    failures: list[str] = []  # Collect specific failures

    def fail(msg: str) -> None:
        print(msg)
        failures.append(msg)

    # 1. Check environment variables are correctly set
    print("Checking environment variables...")
    if getDoRaise() is not True:
        fail("ERROR: getDoRaise is not True")

    # 2. Check that no Redis process is running initially
    print("Checking for existing Redis processes...")
    if not check_redis_process(expect_running=False):
        fail("ERROR: Redis already running, this might interfere with tests")

    # 3. Start Redis and verify it's running
    print("Starting Redis server...")
    redis_process, host, port, password = start_test_redis()

    # 4. Check that Redis process is now running. Check the process handle
    # directly rather than via pgrep: a redis-server that died at startup
    # leaves a zombie child whose comm name still matches pgrep -f.
    print("Verifying Redis process is running...")
    if redis_process.poll() is not None:
        print_redis_server_output(redis_process)
        fail(f"ERROR: redis-server exited at startup with code {redis_process.returncode}")
    elif not check_redis_process(expect_running=True):
        fail("ERROR: Redis failed to start or is not running as expected")

    # 5. Test Redis connection
    print("Testing Redis connection...")
    if not check_redis_connection(host, port, password):
        fail("ERROR: Redis connection failed")
    else:
        print("Redis connection successful")

    # 5b. Test trying to start Redis again (should fail). Skipped if the
    # server is dead: the double-start guard relies on pgrep finding the
    # first server, so attempting it would actually spawn a second one.
    if redis_process.poll() is None:
        print("Testing attempt to start Redis when already running...")
        try:
            start_test_redis()
            fail("ERROR: Was able to start Redis again when it should have failed")
        except RuntimeError as e:
            if "Redis server is already running" in str(e):
                print("✅ Correctly failed to start Redis when already running")
            else:
                fail(f"ERROR: Got unexpected error when starting Redis again: {e}")
    else:
        print("Skipping double-start check - redis-server is not running")

    # 6. Stop Redis
    print("Stopping Redis server...")
    stop_redis(redis_process)

    # 7. Verify Redis is no longer running
    print("Verifying Redis process is stopped...")
    time.sleep(1)  # Give it a moment to fully terminate
    if not check_redis_process(expect_running=False):
        fail("ERROR: Redis didn't shut down properly")

    # 8. Verify environment variables match TestConfig
    print("Verifying Redis environment variables...")
    config = TestConfig()
    if host != os.environ["REDIS_HOST"] or host != config.redis_host:
        fail(f"ERROR: Redis host mismatch: {host=}, {os.environ['REDIS_HOST']=}, {config.redis_host=}")

    if port != os.environ["REDIS_PORT"] or port != config.redis_port:
        fail(f"ERROR: Redis port mismatch: {port=}, {os.environ['REDIS_PORT']=}, {config.redis_port=}")

    if password != os.environ["REDIS_PASSWORD"] or password != config.redis_password:
        fail(f"ERROR: Redis password mismatch: {password=}, config.redis_password=<hidden>")

    # Print summary
    print("\n" + "=" * 80)
    if not failures:
        print("✅ All environment and Redis tests passed!")
        return 0
    else:
        print(f"❌ {len(failures)} test(s) failed:")
        for i, failure in enumerate(failures, 1):
            print(f"  {i}. {failure}")
        print("=" * 80)
        return 1


if __name__ == "__main__":
    sys.exit(main())
