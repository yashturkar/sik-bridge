# Repository Guide for Coding Agents

## Purpose

This repository implements a point-to-point application messaging link over two
SiK radios in transparent serial mode. The same Python daemon runs on the Linux
base station and the Jetson. It owns the serial port, exchanges framed
MessagePack data with the remote daemon, and exposes a local Unix-socket API.

Start with [README.md](README.md) for setup and operator commands. Read
[docs/PROTOCOL.md](docs/PROTOCOL.md) before changing any wire or socket schema.

## Non-goals and safety boundaries

- Do not introduce MAVLink, ROS transport, PPP, or IP networking.
- RSSI and SiK AT commands are intentionally out of scope.
- Do not send `+++`, `ATI`, `ATS`, `AT&T`, or `RT...` commands in the daemon.
- Do not make the daemon reconfigure a radio at startup.
- Only one process may open a radio. Stop the daemon before running
  `serial-test` or any external radio configuration program.
- Treat `/dev/serial/by-id/...` as the stable device identity; `/dev/ttyUSB*`
  and `/dev/ttyACM*` names may change across reconnects.

## Architecture

- `siklink/framing.py`: versioned binary frame encoder and incremental decoder.
- `siklink/messages.py`: message type IDs and MessagePack payload validation.
- `siklink/protocol.py`: heartbeat, ACK/retry, duplicate suppression, and events.
- `siklink/serial_transport.py`: serial discovery, exclusive ownership, queues,
  and reconnect loop.
- `siklink/daemon.py`: protocol/serial orchestration and local Unix socket.
- `siklink/client.py`: supported async application API.
- `siklink/cli.py`: daemon and operator commands.
- `config/`: laptop and Jetson example configurations.
- `systemd/`: production service definition.
- `tests/`: protocol, pseudo-terminal, socket, and two-daemon integration tests.

The dependency direction is framing/messages/config → protocol → transport and
daemon → client/CLI. Keep framing and protocol logic independent of asyncio and
real hardware so it remains deterministic in tests.

## Required invariants

- Wire integers and local socket lengths are big-endian.
- CRC is CRC-16/CCITT-FALSE over `VERSION` through the payload, excluding magic
  and the CRC field.
- Never ACK a frame whose application payload did not validate.
- A duplicate reliable message is ACKed again but delivered locally only once.
- An ACK means acceptance by the remote daemon, not application processing.
- Heartbeats and ACK/NACK frames have higher queue priority than user traffic.
- When disconnected, new sends must fail cleanly instead of accumulating stale
  traffic for a later reconnect.
- Parser corruption must never crash or permanently desynchronize the daemon.
- Wire-schema changes require a protocol version decision and updates to
  `docs/PROTOCOL.md` and compatibility tests.

## Development workflow

Use Python 3.10 or newer and `uv`:

```sh
uv sync --extra test
PYTHONPATH= uv run --extra test pytest -q
PYTHONPATH= uvx ruff check .
uv build
```

Clearing `PYTHONPATH` prevents globally installed ROS pytest plugins from
contaminating this environment. Add deterministic unit tests for protocol
changes. For lifecycle or I/O changes, also add a pseudo-terminal or Unix-socket
integration test. Tests must not require physical radios.

## Hardware workflow

With one radio, validate discovery and host-side serial I/O:

```sh
uv run sik-link serial-test --list
uv run sik-link serial-test --device /dev/serial/by-id/DEVICE --duration 5
```

With both radios, start one daemon per host, run `link-test --echo` on one side,
and run probes from the other. Confirm `CONNECTED`, reliable delivery, heartbeat
RTT/loss, and automatic recovery after a physical unplug/replug. A single radio
cannot validate RF reception or pairing.

## Change checklist

Before handing work off:

1. Confirm no daemon and test utility compete for the serial port.
2. Run the tests, Ruff, and package build shown above.
3. If hardware was used, report the exact device, baud, transmitted/received
   byte counts, and whether a paired radio was present.
4. Keep generated environments, caches, and build products untracked.
