#!/usr/bin/env python3
"""
================================================================================
  rocket_server.py  —  Raspberry Pi Rocket Controller Server
  Ground Support Equipment (GSE) Control System
================================================================================

  Run this script on the Raspberry Pi.

  Architecture:
    - One TCP server socket listens for a single Ground Station connection.
    - Two daemon threads run concurrently for sensor acquisition:
        1. load_cell_thread  : reads HX711, logs to loadcell_log.csv
        2. thermo_thread     : reads thermocouple (mock), logs to thermo_log.csv
    - Per-port telemetry streaming threads are spawned on-demand when the
      Ground Station sends a CMD_READ request for a specific sensor port.
    - The main thread accepts one connection then loops waiting for commands.
    - A try/finally safety block guarantees hardware shutdown on ANY exit path.

  Safety guarantee:
    On exit (KeyboardInterrupt, connection loss, or any exception) the finally
    block will:
      • Set every relay GPIO LOW  (kills output power)
      • Return the vent servo to its "Closed / Home" (min) position
      • Call GPIO.cleanup()

  BINARY PACKET PROTOCOL
  ──────────────────────
  Every packet is exactly 5 bytes:
    [float32 (4 bytes, little-endian)] + [type_id (1 byte)]

  type_id layout:  [port (6 bits)] [command (2 bits)]
    type_id = (port << 2) | command

  Port IDs:
    Relay_1 = 1, Relay_2 = 2, Relay_3 = 3
    Servo   = 4
    Thrust  = 5  (value = thrust in Newtons)
    Thermo  = 6  (value = temperature in °C)

  Commands:
    CMD_READ     = 0b00  (request telemetry stream for a port)
    CMD_SET      = 0b01  (set relay/servo on/off)
    CMD_SERVO    = 0b10  (reserved for servo-specific commands)
    CMD_SHUTDOWN = 0b11  (emergency shutdown)

  PIN CONFIGURATION  ← edit this section to match your wiring
  ---------------------------------------------------------------
  HX711 load cell:
    DOUT  = GPIO 5   (BCM)
    PD_SCK = GPIO 6  (BCM)
    REFERENCE_UNIT = 114   ← calibrate for your load cell

  Relay outputs (BCM numbering, active-HIGH):
    RELAY_1_PIN = 17   (e.g. main fuel valve power)
    RELAY_2_PIN = 27   (e.g. oxidiser valve power)
    RELAY_3_PIN = 22   (e.g. igniter power)

  Servo (vent valve):
    SERVO_PIN = 18  (BCM, hardware PWM)

  Thermocouple (MAX31855 via SPI):
    Uses board.SPI() + board.D24 for CS — change in thermo_thread() if needed.

  Network:
    HOST = "0.0.0.0"        # listen on all interfaces
    PORT = 5000
================================================================================
"""

import sys
import os
import json
import csv
import time
import socket
import threading
import datetime
import random
import atexit
import struct
import signal

# ── Conditionally import hardware libraries ──────────────────────────────────
# If running on a Pi with real hardware, these will work normally.
# A stub is used if the import fails so the module can be syntax-checked
# on a non-Pi machine.

try:
    import RPi.GPIO as GPIO
    _GPIO_AVAILABLE = True
except ImportError:
    print("[WARN] RPi.GPIO not found — running in hardware-stub mode")
    _GPIO_AVAILABLE = False

try:
    from gpiozero import Servo as GZServo
    from gpiozero.pins.pigpio import PiGPIOFactory
    _GPIOZERO_AVAILABLE = True
except ImportError:
    print("[WARN] gpiozero not found — servo will be stubbed")
    _GPIOZERO_AVAILABLE = False

try:
    import board
    import digitalio
    import adafruit_max31855
    _THERMO_AVAILABLE = True
except ImportError:
    print("[WARN] Adafruit MAX31855 library not found — using mock thermocouple")
    _THERMO_AVAILABLE = False

# ── Add local hx711py folder to path ────────────────────────────────────────
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_SCRIPT_DIR, "hx711py"))

try:
    from hx711 import HX711
    _HX711_AVAILABLE = True
except ImportError:
    print("[WARN] hx711 not found — load cell will be stubbed")
    _HX711_AVAILABLE = False


# ════════════════════════════════════════════════════════════════════════════
#  ①  HARDWARE CONFIGURATION  ← Edit pin numbers here
# ════════════════════════════════════════════════════════════════════════════

# ── Network ──────────────────────────────────────────────────────────────────
HOST = "0.0.0.0"           # Listen on all network interfaces
PORT = 5000                 # Must match ground_station.py

# ── HX711 Load Cell (BCM pin numbers) ────────────────────────────────────────
HX711_DOUT_PIN   = 5       # Data-out pin
HX711_SCK_PIN    = 6       # Clock pin
HX711_REF_UNIT   = 114     # Calibration value: raw_counts / ref_unit = grams
                            # Set ref_unit=1, load known mass, note raw value,
                            # then ref_unit = raw_value / mass_in_grams

# ── Thermocouple (MAX31855 via SPI) ──────────────────────────────────────────
THERMO_CS_PIN    = board.D24 if _THERMO_AVAILABLE else None  # Chip-select pin

# ── Relay GPIO pins (BCM, active-HIGH) ───────────────────────────────────────
RELAY_PINS = {
    "RELAY_1": 17,          # e.g. Fuel valve relay
    "RELAY_2": 27,          # e.g. Ox valve relay
    "RELAY_3": 22,          # e.g. Igniter relay
}

# ── Servo (vent valve) ────────────────────────────────────────────────────────
SERVO_PIN        = 18       # GPIO 18 supports hardware PWM
# gpiozero Servo pulse widths — adjust to match your servo's datasheet
SERVO_MIN_PULSE  = 0.001   # 1 ms  → "Closed / Home" position (safe default)
SERVO_MAX_PULSE  = 0.002   # 2 ms  → "Open" position

# ── Log file paths ────────────────────────────────────────────────────────────
LOADCELL_LOG = os.path.join(_SCRIPT_DIR, "loadcell_log.csv")
THERMO_LOG   = os.path.join(_SCRIPT_DIR, "thermo_log.csv")

# ── Telemetry rate ────────────────────────────────────────────────────────────
TELEMETRY_HZ     = 20      # How many binary packets per second to stream


# ── Packet Protocol ──────────────────────────────────────────────────────
# Every packet is exactly 5 bytes: [float32 (4 bytes)] + [type_id (1 byte)]
# type_id = (port << 2) | command

# Port IDs:
Relay_1  = 0b000001         # 1
Relay_2  = 0b000010         # 2
Relay_3  = 0b000011         # 3
Servo    = 0b000100         # 4
Thrust   = 0b000101         # 5  — value = thrust in Newtons
Thermo   = 0b000110         # 6  — value = temperature in °C

# Commands (lower 2 bits of type_id):
CMD_READ      = 0b00        # Request telemetry stream for a port
CMD_SET       = 0b01        # Set relay/servo on/off (value > 0 = ON)
CMD_SERVO     = 0b10        # Reserved for servo-specific commands
CMD_SHUTDOWN  = 0b11        # Emergency shutdown

# Map port IDs to _latest keys for telemetry lookups
NAME_REGISTRY = {
    Thrust: "thrust_N",
    Thermo: "temp_C",
}

# Map port IDs to relay names for command dispatch
PORT_TO_RELAY = {
    Relay_1: "RELAY_1",
    Relay_2: "RELAY_2",
    Relay_3: "RELAY_3",
}

# Packet size in bytes
PACKET_SIZE = 5


# ════════════════════════════════════════════════════════════════════════════
#  ②  SHARED STATE  (thread-safe via a Lock)
# ════════════════════════════════════════════════════════════════════════════

# Active per-port telemetry stream threads
_stream_threads = {}

_state_lock = threading.Lock()

# Latest sensor readings — updated by sensor threads, read by telemetry threads
_latest = {
    "thrust_N":  0.0,       # Load cell reading in Newtons (or grams, adjust)
    "temp_C":    0.0,       # Thermocouple reading in °C
    "timestamp": "",        # ISO-format string, updated each read cycle
}

# Relay toggle states — True = relay is energised (ON)
_relay_states = {k: False for k in RELAY_PINS}

# Servo state
_servo_is_open = False

# Flag to signal all threads to stop gracefully
_shutdown_event = threading.Event()

# Active client connection (set by main accept loop)
_client_conn = None
_client_lock = threading.Lock()


# ════════════════════════════════════════════════════════════════════════════
#  ③  HARDWARE INITIALISATION
# ════════════════════════════════════════════════════════════════════════════

def init_gpio():
    """Configure relay GPIO pins as outputs, all starting LOW (safe state)."""
    if not _GPIO_AVAILABLE:
        print("[STUB] GPIO init skipped")
        return
    GPIO.setmode(GPIO.BCM)
    GPIO.setwarnings(False)
    for name, pin in RELAY_PINS.items():
        GPIO.setup(pin, GPIO.OUT, initial=GPIO.LOW)
        print(f"[GPIO] {name} on pin {pin} → LOW (off)")


def init_servo():
    """
    Return a gpiozero Servo instance using the pigpio factory
    (reduces jitter vs software PWM).  Starts in 'min' / closed position.
    Run 'sudo pigpiod' before launching this script.
    """
    if not _GPIOZERO_AVAILABLE:
        print("[STUB] Servo init skipped")
        return None
    try:
        factory = PiGPIOFactory()
        servo = GZServo(
            SERVO_PIN,
            min_pulse_width=SERVO_MIN_PULSE,
            max_pulse_width=SERVO_MAX_PULSE,
            pin_factory=factory,
        )
        servo.min()   # ← Safe home position on startup
        print(f"[SERVO] Initialised on GPIO {SERVO_PIN} → Closed")
        return servo
    except Exception as e:
        print(f"[SERVO] Init failed: {e}")
        return None


def init_thermocouple():
    """Return adafruit MAX31855 sensor object, or None if unavailable."""
    if not _THERMO_AVAILABLE:
        print("[STUB] Thermocouple init skipped — using mock data")
        return None
    try:
        spi = board.SPI()
        cs  = digitalio.DigitalInOut(THERMO_CS_PIN)
        sensor = adafruit_max31855.MAX31855(spi, cs)
        print("[THERMO] MAX31855 initialised")
        return sensor
    except Exception as e:
        print(f"[THERMO] Init failed: {e} — using mock data")
        return None


def init_loadcell():
    """
    Return a tared HX711 instance, or None if unavailable.
    The HX711 library uses GPIO.BCM mode internally.
    """
    if not _HX711_AVAILABLE or not _GPIO_AVAILABLE:
        print("[STUB] Load cell init skipped — using mock data")
        return None
    try:
        hx = HX711(HX711_DOUT_PIN, HX711_SCK_PIN)
        hx.set_reading_format("MSB", "MSB")
        hx.set_reference_unit(HX711_REF_UNIT)
        hx.reset()
        print("[HX711] Taring load cell — remove all weight...")
        hx.tare()
        print("[HX711] Tare complete. Load cell ready.")
        return hx
    except Exception as e:
        print(f"[HX711] Init failed: {e} — using mock data")
        return None


def init_csv_logs():
    """Create CSV log files with headers if they don't already exist."""
    for path, headers in [
        (LOADCELL_LOG, ["Timestamp", "Thrust_N"]),
        (THERMO_LOG,   ["Timestamp", "Temp_C"]),
    ]:
        try:
            with open(path, "x", newline="") as f:
                csv.writer(f).writerow(headers)
            print(f"[LOG] Created {path}")
        except FileExistsError:
            print(f"[LOG] Appending to existing {path}")


# ════════════════════════════════════════════════════════════════════════════
#  ④  SENSOR THREADS
# ════════════════════════════════════════════════════════════════════════════

def load_cell_thread(hx711_instance):
    """
    Continuously reads the load cell and:
      1. Updates _latest["thrust_N"] in shared state.
      2. Appends each reading to loadcell_log.csv.

    No time.sleep() — the HX711 library blocks internally while waiting
    for the DOUT pin to go LOW, which is the hardware-correct idle wait.
    """
    print("[THREAD] Load cell thread started")

    while not _shutdown_event.is_set():
        try:
            # ── Real hardware path ────────────────────────────────────────
            if hx711_instance is not None:
                raw_grams = hx711_instance.get_weight(3)  # average 3 samples
                thrust    = round(raw_grams * 0.00981, 4) # grams → Newtons
                # Power cycle for next read (helps stability)
                hx711_instance.power_down()
                hx711_instance.power_up()
            else:
                # ── Mock / stub path ─────────────────────────────────────
                thrust = round(random.uniform(0.0, 250.0), 2)
                # In mock mode, pace reads to avoid tight CPU loop
                _shutdown_event.wait(0.05)

            ts = datetime.datetime.now().isoformat(timespec="milliseconds")

            # Update shared state (thread-safe)
            with _state_lock:
                _latest["thrust_N"]  = thrust
                _latest["timestamp"] = ts

            # Append to CSV log
            with open(LOADCELL_LOG, "a", newline="") as f:
                csv.writer(f).writerow([ts, thrust])

        except Exception as e:
            print(f"[HX711] Read error: {e}")
            # Brief pause only on error, to prevent a tight error loop
            _shutdown_event.wait(0.1)

    print("[THREAD] Load cell thread stopped")


def thermo_thread(thermo_sensor):
    """
    Continuously reads the thermocouple and:
      1. Updates _latest["temp_C"] in shared state.
      2. Appends each reading to thermo_log.csv.

    Uses a small sleep (0.25 s) because the MAX31855 only updates at ~4 Hz.
    """
    print("[THREAD] Thermocouple thread started")

    while not _shutdown_event.is_set():
        try:
            # ── Real hardware path ────────────────────────────────────────
            if thermo_sensor is not None:
                temp = round(thermo_sensor.temperature, 2)
            else:
                # ── Mock / stub path ─────────────────────────────────────
                # Simulates a slow temperature rise with noise
                temp = round(25.0 + random.uniform(-0.5, 0.5), 2)

            ts = datetime.datetime.now().isoformat(timespec="milliseconds")

            # Update shared state
            with _state_lock:
                _latest["temp_C"]    = temp
                _latest["timestamp"] = ts

            # Append to CSV log
            with open(THERMO_LOG, "a", newline="") as f:
                csv.writer(f).writerow([ts, temp])

        except Exception as e:
            print(f"[THERMO] Read error: {e}")

        # MAX31855 is 4 Hz max; 0.25 s gives one sample per hardware update
        _shutdown_event.wait(0.25)

    print("[THREAD] Thermocouple thread stopped")


def sendall_thread(port, command):
    """
    Per-port telemetry streaming thread.

    Reads the latest value for the given port from shared state, packs it
    into a 5-byte binary packet, and sends it to the connected client at
    TELEMETRY_HZ rate.

    Packet: [float32 value (4 bytes)] + [type_id (1 byte)]
    where type_id = (port << 2) | command
    """
    type_id = (port << 2) | command
    key = NAME_REGISTRY.get(port)
    if key is None:
        print(f"[THREAD] No registry entry for port {port}, aborting stream")
        return

    port_name = key
    print(f"[THREAD] Telemetry stream started for {port_name} (port={port})")

    while not _shutdown_event.is_set():
        with _state_lock:
            value = _latest[key]
        packet = struct.pack('<f', value) + bytes([type_id])
        with _client_lock:
            if _client_conn is None:
                print(f"[THREAD] No client connected, stopping {port_name} stream")
                break
            try:
                _client_conn.sendall(packet)
            except OSError as e:
                print(f"[THREAD] Send error on {port_name}: {e}")
                break
        _shutdown_event.wait(1.0 / TELEMETRY_HZ)

    print(f"[THREAD] {port_name} stream stopped")


# ════════════════════════════════════════════════════════════════════════════
#  ⑤  COMMAND HANDLERS
# ════════════════════════════════════════════════════════════════════════════

def handle_relay_on(relay_name, servo):
    """Energise the named relay (set GPIO HIGH)."""
    if relay_name not in RELAY_PINS:
        print(f"[CMD] Unknown relay: {relay_name}")
        return
    pin = RELAY_PINS[relay_name]
    with _state_lock:
        _relay_states[relay_name] = True
    if _GPIO_AVAILABLE:
        GPIO.output(pin, GPIO.HIGH)
    print(f"[CMD] {relay_name} → ON  (pin {pin})")


def handle_relay_off(relay_name, servo):
    """De-energise the named relay (set GPIO LOW)."""
    if relay_name not in RELAY_PINS:
        print(f"[CMD] Unknown relay: {relay_name}")
        return
    pin = RELAY_PINS[relay_name]
    with _state_lock:
        _relay_states[relay_name] = False
    if _GPIO_AVAILABLE:
        GPIO.output(pin, GPIO.LOW)
    print(f"[CMD] {relay_name} → OFF (pin {pin})")


def handle_servo_open(servo):
    """Open the vent valve servo (move to max position)."""
    global _servo_is_open
    _servo_is_open = True
    if servo is not None:
        servo.max()
    print("[CMD] SERVO → OPEN")


def handle_servo_close(servo):
    """Close the vent valve servo (return to min / home position)."""
    global _servo_is_open
    _servo_is_open = False
    if servo is not None:
        servo.min()
    print("[CMD] SERVO → CLOSED")


# ── Dispatch binary command ──────────────────────────────────────────────

def dispatch_command(packet: bytes, servo):
    """
    Parse a 5-byte binary command packet and route to the appropriate handler.

    Packet layout:
      bytes 0-3: float32 value (little-endian)
      byte 4:    type_id = (port << 2) | command
    """
    if len(packet) != PACKET_SIZE:
        print(f"[CMD] Invalid packet size: {len(packet)} (expected {PACKET_SIZE})")
        return

    value   = struct.unpack('<f', packet[:4])[0]
    type_id = packet[4]
    command = type_id & 0b00000011        # Lower 2 bits
    port    = (type_id >> 2) & 0b00111111  # Upper 6 bits

    print(f"[CMD] Received: port={port}, command={command}, value={value:.4f}")

    if command == CMD_SET:
        # ── Set a relay or servo on/off ──────────────────────────────────
        relay_name = PORT_TO_RELAY.get(port)
        if relay_name is not None:
            if value > 0:
                handle_relay_on(relay_name, servo)
            else:
                handle_relay_off(relay_name, servo)
        elif port == Servo:
            if value > 0:
                handle_servo_open(servo)
            else:
                handle_servo_close(servo)
        else:
            print(f"[CMD] CMD_SET for unknown port: {port}")

    elif command == CMD_READ:
        # ── Start a per-port telemetry stream ────────────────────────────
        name = NAME_REGISTRY.get(port, f"UNKNOWN(port={port})")
        if port not in NAME_REGISTRY:
            print(f"[CMD] CMD_READ for unmapped port {port}, ignoring")
            return

        if port not in _stream_threads or not _stream_threads[port].is_alive():
            print(f"[CMD] Starting telemetry stream for {name}")
            t = threading.Thread(
                target=sendall_thread,
                args=(port, CMD_READ),
                name=f"Stream-{name}",
                daemon=True,
            )
            _stream_threads[port] = t
            t.start()
        else:
            print(f"[CMD] Stream already active for {name}, ignoring")

    elif command == CMD_SHUTDOWN:
        print("[CMD] SHUTDOWN command received — initiating emergency shutdown")
        _shutdown_event.set()

    else:
        print(f"[CMD] Unsupported command: {command}")


# ════════════════════════════════════════════════════════════════════════════
#  ⑥  COMMAND RECEIVE LOOP  (runs in main thread after client connects)
# ════════════════════════════════════════════════════════════════════════════

def recv_exact(conn, n):
    """Read exactly n bytes from conn, blocking until all arrive."""
    buf = b""
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("Client disconnected")
        buf += chunk
    return buf


def command_receive_loop(conn, servo):
    """
    Blocking loop that reads 5-byte binary command packets from the
    Ground Station connection and dispatches them to the hardware handlers.

    Returns when the connection is closed or an error occurs.
    """
    print("[SERVER] Waiting for commands (binary protocol)...")

    while not _shutdown_event.is_set():
        try:
            data = recv_exact(conn, PACKET_SIZE)
            dispatch_command(data, servo)

        except ConnectionError as e:
            print(f"[SERVER] {e}")
            break
        except (OSError, struct.error) as e:
            print(f"[SERVER] Receive error: {e}")
            break

    _shutdown_event.set()


# ════════════════════════════════════════════════════════════════════════════
#  ⑦  SAFETY SHUTDOWN  (called by finally block AND atexit)
# ════════════════════════════════════════════════════════════════════════════

def emergency_shutdown(servo):
    """
    CRITICAL SAFETY FUNCTION
    ─────────────────────────
    Called on any exit path (normal, KeyboardInterrupt, exception, SIGTERM).

    Actions (in order):
      1. Signal all threads to stop.
      2. Set every relay GPIO LOW  → removes output power from all components.
      3. Return servo to min (Closed / Home) position.
      4. Call GPIO.cleanup() to release all pins.

    Sensor power is intentionally NOT cut here (relays are assumed to only
    control output/ignition circuits, not the Pi's own 5 V rail or sensors).
    """
    print("\n" + "═" * 60)
    print("  ⚠  EMERGENCY SHUTDOWN INITIATED")
    print("═" * 60)
    _shutdown_event.set()

    # ── Kill all relays ───────────────────────────────────────────────────
    if _GPIO_AVAILABLE:
        for name, pin in RELAY_PINS.items():
            GPIO.output(pin, GPIO.LOW)
            print(f"  [SAFETY] {name} pin {pin} → LOW")
        print("  [SAFETY] All relay outputs cut")
    else:
        print("  [SAFETY STUB] All relays would be set LOW")

    # ── Return servo to closed/home ───────────────────────────────────────
    if servo is not None:
        try:
            servo.min()
            print("  [SAFETY] Servo returned to CLOSED / HOME position")
        except Exception as e:
            print(f"  [SAFETY] Servo close failed: {e}")
    else:
        print("  [SAFETY STUB] Servo would be returned to CLOSED")

    # ── GPIO cleanup ──────────────────────────────────────────────────────
    if _GPIO_AVAILABLE:
        try:
            GPIO.cleanup()
            print("  [SAFETY] GPIO cleanup complete")
        except Exception as e:
            print(f"  [SAFETY] GPIO cleanup error: {e}")

    print("═" * 60)
    print("  Shutdown complete. Safe to power off.")
    print("═" * 60 + "\n")


# ════════════════════════════════════════════════════════════════════════════
#  ⑧  MAIN ENTRY POINT
# ════════════════════════════════════════════════════════════════════════════

def main():
    print("=" * 60)
    print("  GSE Rocket Controller Server  —  rocket_server.py")
    print(f"  Binding to {HOST}:{PORT}")
    print("  Protocol: 5-byte binary packets")
    print("=" * 60)

    # ── Hardware init ─────────────────────────────────────────────────────
    init_gpio()
    servo    = init_servo()
    thermo   = init_thermocouple()
    hx711    = init_loadcell()
    init_csv_logs()

    # ── Register atexit safety handler ───────────────────────────────────
    # atexit runs on normal interpreter exit (including sys.exit())
    atexit.register(emergency_shutdown, servo)

    # ── SIGTERM handler (e.g. systemd stop) ──────────────────────────────
    def _sigterm_handler(signum, frame):
        print("\n[SERVER] SIGTERM received")
        raise SystemExit(0)
    signal.signal(signal.SIGTERM, _sigterm_handler)

    # ── Create TCP server socket ──────────────────────────────────────────
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind((HOST, PORT))
    server_sock.listen(1)   # Only accept ONE ground station
    print(f"[SERVER] Listening on {HOST}:{PORT} — waiting for Ground Station…\n")

    # ── Start sensor threads (daemon so they die with the process) ────────
    t_lc = threading.Thread(
        target=load_cell_thread, args=(hx711,), name="LoadCell", daemon=True
    )
    t_th = threading.Thread(
        target=thermo_thread, args=(thermo,), name="Thermo", daemon=True
    )
    t_lc.start()
    t_th.start()

    # ── Main loop: accept one connection at a time ────────────────────────
    try:
        while True:
            _shutdown_event.clear()          # Reset for a new connection
            print("[SERVER] Waiting for connection…")

            try:
                server_sock.settimeout(2.0)  # Allow KeyboardInterrupt check
                while True:
                    try:
                        conn, addr = server_sock.accept()
                        global _client_conn
                        with _client_lock:
                            _client_conn = conn
                        break
                    except socket.timeout:
                        if _shutdown_event.is_set():
                            raise KeyboardInterrupt
            except KeyboardInterrupt:
                raise

            print(f"[SERVER] Ground Station connected from {addr}")

            # ── Block here receiving commands ─────────────────────────────
            command_receive_loop(conn, servo)

            # ── Cleanup after client disconnects ──────────────────────────
            # Signal stream threads to stop, then wait briefly
            _shutdown_event.set()
            for port_id, t in list(_stream_threads.items()):
                t.join(timeout=1.0)
            _stream_threads.clear()

            try:
                conn.close()
            except Exception:
                pass

            with _client_lock:
                _client_conn = None

            print("[SERVER] Session ended — ready for next connection\n")

    except KeyboardInterrupt:
        print("\n[SERVER] KeyboardInterrupt — shutting down")

    finally:
        # ── SAFETY SHUTDOWN ───────────────────────────────────────────────
        # This block runs on KeyboardInterrupt, SystemExit, or any unhandled
        # exception.  The atexit handler also calls emergency_shutdown, but
        # having it here ensures it runs BEFORE the server socket closes.
        emergency_shutdown(servo)
        try:
            server_sock.close()
        except Exception:
            pass
        print("[SERVER] Server socket closed. Goodbye.")


if __name__ == "__main__":
    main()
