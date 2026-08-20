# sik-link

`sik-link` is a small Linux-to-Linux messaging layer for a pair of SiK radios in
transparent serial mode. One daemon owns the radio on each computer. Local
applications communicate with that daemon over a Unix socket and never open the
serial device themselves.

The radio protocol provides binary framing, CRC-16 validation, MessagePack
messages, heartbeats, link health, and optional ACK/retry delivery. It does not
use MAVLink, PPP, IP, or ROS, and it does not query RSSI or alter radio settings.

## Documentation map

- This README: installation, operation, deployment, and hardware bring-up.
- [`docs/PROTOCOL.md`](docs/PROTOCOL.md): byte-level radio format, every payload
  and Unix-socket schema, reliability semantics, and application topic guidance.
- [`AGENTS.md`](AGENTS.md): architecture, invariants, safety boundaries, and the
  required workflow for coding agents.

## Install

Python 3.10 or newer is required. With `uv`:

```sh
uv sync --extra test
uv run sik-link --help
```

For a system installation, create a virtual environment or install the wheel,
then ensure the resulting `sik-link` executable is at the path used by the
systemd unit.

## Find and validate the radio

Connect a radio and inspect stable device names:

```sh
ls -l /dev/serial/by-id/
uv run sik-link serial-test --list
```

Automatic discovery is safe only when exactly one matching device exists. When
several serial devices are connected, put the full `/dev/serial/by-id/...` path
in the YAML file or set `serial.match` to a unique substring.

Before starting a daemon on either machine, test raw transport. Run this on both
ends, changing the text so its origin is obvious:

```sh
uv run sik-link serial-test --send base --duration 60
uv run sik-link serial-test --send jetson --duration 60
```

The tool prints raw bytes, connect/disconnect events, and totals. A single radio
can validate enumeration, opening, transmit writes, and unplug/replug handling;
RF reception and bidirectional behavior require the paired radio.

Both radios must already have compatible frequency band, network ID, serial
baud, air speed, channel, and RF settings. Stop the daemon before using a
separate radio configuration tool because only one process may own the port.

## Configure and run

Copy `config/base.yaml` on the laptop and `config/jetson.yaml` on the Jetson.
Set each `serial.device` to its stable by-id path when possible. The default baud
is 57600; it must match the radio's configured serial baud.

For development, start the daemon in a terminal:

```sh
sudo install -d -o "$USER" -g "$(id -gn)" -m 0755 /run/sik-link
uv run sik-link daemon --config config/base.yaml
```

In another terminal:

```sh
uv run sik-link status
uv run sik-link monitor
uv run sik-link send test '{"command":"hello","value":42}' --reliable
```

An unreliable send reports `SENT` once queued locally. A reliable send waits for
the other daemon to validate and ACK it, or reports a timeout after the
configured retries. ACK means remote-daemon delivery, not processing by a
remote application.

The state is `DISCONNECTED` until a valid radio frame arrives and when no valid
frame has arrived for three seconds. It is `DEGRADED` when the configured RTT or
heartbeat-loss threshold is exceeded, and `CONNECTED` otherwise. Serial-port
connection and radio-link state are reported separately.

If an open USB serial device delivers no valid RF frames for 15 seconds, the
daemon automatically closes and reopens it. This recovers FTDI/USB stalls that
do not raise a normal pyserial disconnect. Configure the interval with
`serial.rf_silence_reopen_after`, or set it to zero to disable forced reopening.
If a tty-only reopen does not recover valid frames, the daemon can reset the
individual FT230X device without resetting its parent hub. Install the required
device permission once on each host:

```sh
./scripts/install-usb-reset-permissions.sh
```

The configured user must belong to `dialout`; reconnect the radio or reboot if
an existing device node did not receive the updated rule immediately.

## End-to-end link test

With both daemons running, start an echo client on the Jetson:

```sh
uv run sik-link link-test --echo
```

Then run probes on the laptop:

```sh
uv run sik-link link-test --count 100 --interval 0.1
```

This reports application-level replies, loss, and round-trip timing. To test
recovery, unplug either radio for more than three seconds, confirm
`DISCONNECTED`, reconnect it, and confirm that the serial worker and radio link
recover without restarting either daemon.

## Python application API

The supported local API is asynchronous:

```python
import asyncio
from siklink import SikLinkClient

async def main():
    async with SikLinkClient() as client:
        result = await client.send(
            "illumination",
            {"mode": "manual", "intensity": 0.65},
            reliable=True,
        )
        print(result)

asyncio.run(main())
```

`data` may be any value supported by MessagePack, including nested maps, arrays,
strings, integers, floats, booleans, `None`, and bytes. The complete encoded
`{"topic": ..., "data": ...}` map must fit within `protocol.max_payload`
(256 bytes by default). A reliable call waits for a remote-daemon ACK; use an
application response topic when confirmation of actual command execution is
required.

To receive messages:

```python
async with SikLinkClient() as client:
    await client.subscribe(["robot_status"], status=True)
    async for event in client.events():
        print(event)
```

The socket protocol is a four-byte big-endian payload length followed by one
MessagePack map. Requests contain `id` and `op`; supported operations are
`send`, `status`, and `subscribe`. The packaged client should normally be used
instead of implementing this protocol directly. See
[`docs/PROTOCOL.md`](docs/PROTOCOL.md) for exact request, response, event, and
status schemas.

## systemd deployment

The included unit expects a `sik-link` system user, membership in `dialout`, a
configuration at `/etc/sik-link/config.yaml`, and the executable at
`/usr/local/bin/sik-link`:

```sh
sudo useradd --system --home-dir /nonexistent --shell /usr/sbin/nologin sik-link
sudo usermod -a -G dialout sik-link
sudo install -d -m 0755 /etc/sik-link
sudo install -m 0644 config/base.yaml /etc/sik-link/config.yaml
sudo install -m 0644 systemd/sik-link.service /etc/systemd/system/sik-link.service
sudo systemctl daemon-reload
sudo systemctl enable --now sik-link
systemctl status sik-link
journalctl -u sik-link -f
```

Use `jetson.yaml` on the Jetson. Logs go to journald, which handles retention and
rotation. The service creates `/run/sik-link/`; the socket itself defaults to
mode `0660` (written as decimal `432` in YAML). Add application users that need
socket access to the `sik-link` group.

## Development and virtual testing

Run the automated suite with:

```sh
PYTHONPATH= uv run --extra test pytest -q
```

Clearing `PYTHONPATH` keeps globally installed ROS pytest plugins from leaking
into the isolated project environment. The tests cover known CRC vectors,
parser resynchronization and arbitrary read
chunking, retries and timeouts, duplicate suppression, heartbeat state, config
validation, local socket behavior, and serial I/O through a Linux pseudo-terminal.

## Wire format

Version 1 frames are:

```text
AA 55 | VERSION | TYPE | FLAGS | SEQ:u16 | LENGTH:u16 | PAYLOAD | CRC16:u16
```

Integers are big-endian. CRC-16/CCITT-FALSE covers `VERSION` through the final
payload byte. The default maximum payload is 256 bytes. The `ACK_REQUIRED` flag
is `0x01`; sequence numbers wrap at 65535. Corrupt or unknown frames are dropped,
and the streaming parser searches for the next valid magic sequence.
