import subprocess
import time
import signal
import sys

processes = []

def run(cmd):
    print(f"\n🚀 Starting: {cmd}")
    p = subprocess.Popen(cmd, shell=True)
    processes.append(p)
    return p

def stop_all():
    print("\n🛑 Stopping all services...")
    for p in processes:
        p.terminate()
    sys.exit(0)

# =========================
# HANDLE CTRL+C
# =========================
signal.signal(signal.SIGINT, lambda sig, frame: stop_all())

# =========================
# START SERVICES
# =========================

# 1. Generator
run("python generate/generator.py")
time.sleep(5)

# 2. Bronze
run("spark-submit spark/spark_stream.py")
time.sleep(5)

# 3. Silver
run("spark-submit spark/silver_stream.py")
time.sleep(5)

# 4. Gold
run("spark-submit spark/gold_stream.py")

print("\n🔥 STREAMING PIPELINE RUNNING...")

# =========================
# KEEP ALIVE
# =========================
while True:
    time.sleep(10)