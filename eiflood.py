"""
eif_flood.py

A multi-threaded stress testing utility for Netcool EIF Probes.
Generates unique hostname events with high precision timestamps
to measure propagation delay.
"""

import socket
import threading
import time
import random
import datetime

# --- Configuration ---
TARGET_HOST = "192.168.1.50"  # Replace with your Probe IP
TARGET_PORT = 9998
TOTAL_EVENTS = 120000
THREAD_COUNT = 20
EVENT_CLASS = "ITM_Heartbeat"

# Offset for multi-node execution
NODE_OFFSET = 0


def get_iso_time():
    """Returns current time in ISO 8601 format for logging."""
    return datetime.datetime.now().isoformat()


def generate_payload(unique_id):
    """
    Constructs an EIF formatted event string with timestamping.
    """
    # Simulate a realistic hostname
    hostname = f"SVR-US-OH-{unique_id}"
    
    # Capture the exact time of generation for latency calculation
    # Using Unix Epoch is easiest for Netcool math (Integer/Time fields)
    epoch_time = time.time()
    
    msg = (
        f"{EVENT_CLASS};"
        f"msg=Heartbeat check failed on {hostname};"
        f"hostname={hostname};"
        f"severity={random.choice([2, 3, 4, 5])};"
        f"source=EIF_Stress_Tool;"
        f"sub_source=id_{unique_id};"
        f"origin_node={hostname};"
        f"sent_at={epoch_time};" 
        "END\n"
    )
    return msg.encode('utf-8')


def worker(thread_id, events_per_thread, start_index):
    """
    Thread worker function to send a batch of events.
    """
    print(f"[{get_iso_time()}] [Thread-{thread_id:02d}] "
          f"Starting. Range: {start_index} - {start_index + events_per_thread}")

    sock = None
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(15)
        sock.connect((TARGET_HOST, TARGET_PORT))

        for i in range(events_per_thread):
            current_id = start_index + i
            payload = generate_payload(current_id)
            sock.sendall(payload)

            if i % 50 == 0:
                time.sleep(0.01)

        print(f"[{get_iso_time()}] [Thread-{thread_id:02d}] Batch complete.")

    except ConnectionRefusedError:
        print(f"[{get_iso_time()}] [Thread-{thread_id:02d}] ERROR: "
              f"Connection refused at {TARGET_HOST}:{TARGET_PORT}")
    except OSError as err:
        print(f"[{get_iso_time()}] [Thread-{thread_id:02d}] ERROR: "
              f"Socket error: {err}")
    finally:
        if sock:
            sock.close()


def run_stress_test():
    """
    Main controller to spawn threads.
    """
    print("--- Netcool EIF Stress Test (Latency Enabled) ---")
    print(f"Start Time: {get_iso_time()}")
    print(f"Target:     {TARGET_HOST}:{TARGET_PORT}")
    print(f"Volume:     {TOTAL_EVENTS} events")
    print(f"Threads:    {THREAD_COUNT}")
    print("-------------------------------------------------")

    events_per_thread = TOTAL_EVENTS // THREAD_COUNT
    threads = []

    start_time = time.time()

    for i in range(THREAD_COUNT):
        thread_offset = NODE_OFFSET + (i * events_per_thread)
        t = threading.Thread(
            target=worker,
            args=(i + 1, events_per_thread, thread_offset)
        )
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    duration = time.time() - start_time
    rate = int(TOTAL_EVENTS / duration) if duration > 0 else 0

    print("\n--- Test Complete ---")
    print(f"End Time: {get_iso_time()}")
    print(f"Duration: {duration:.2f} seconds")
    print(f"Rate:     {rate} events/sec")


if __name__ == "__main__":
    run_stress_test()
