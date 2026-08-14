# SiK Link Protocol and Local API

This document is the compatibility reference for version 1 of the radio wire
protocol and the daemon's local Unix-socket API. All payload objects are encoded
with MessagePack using string keys and binary-safe values.

## Radio frame

```text
Offset  Size  Field
0       2     magic = AA 55
2       1     version = 01
3       1     message type
4       1     flags
5       2     sequence number, unsigned big-endian
7       2     payload length N, unsigned big-endian
9       N     MessagePack payload
9+N     2     CRC-16, unsigned big-endian
```

CRC uses CRC-16/CCITT-FALSE: polynomial `0x1021`, initial value `0xffff`, no
reflection, no final XOR. It covers bytes from `version` through the final
payload byte. It does not cover the two magic bytes or the CRC field.

The configured payload maximum defaults to 256 bytes and may not exceed the
implementation hard limit of 4096 bytes. A frame exceeding the local configured
maximum is invalid. The decoder accepts fragmented reads, multiple frames per
read, and arbitrary garbage; after an invalid version, length, or CRC it searches
for the next `AA 55` marker.

Sequence numbers are unsigned 16-bit values, start from a random value at daemon
startup, apply to every transmitted frame, and wrap from 65535 to 0.

### Flags

| Bit | Name | Meaning |
| --- | --- | --- |
| `0x01` | `ACK_REQUIRED` | Receiver must ACK or NACK this frame. Used for reliable `USER_MESSAGE` frames. |

All other bits are reserved and must be transmitted as zero in version 1.

### Message types and payloads

| Value | Name | MessagePack payload |
| --- | --- | --- |
| `0x01` | `HEARTBEAT` | Heartbeat map described below. |
| `0x02` | `HEARTBEAT_ACK` | `{"seq": uint16, "timestamp_ms": int}` copied from the heartbeat. |
| `0x10` | `USER_MESSAGE` | `{"topic": non-empty string, "data": any MessagePack value}`. |
| `0x11` | `ACK` | `{"seq": uint16}` identifying the accepted reliable frame. |
| `0x12` | `NACK` | `{"seq": uint16, "error": string}`. Current errors are `unknown_type` and `invalid_payload`. |
| `0x20` | `LINK_STATUS` | Reserved for future use; not emitted in version 1. |

A heartbeat payload is:

```text
{
  "uptime_ms": int,
  "timestamp_ms": int,
  "last_rx_seq": uint16 | nil,
  "tx_frames": int,
  "rx_frames": int,
  "crc_errors": int
}
```

`timestamp_ms` is Unix epoch milliseconds and is echoed unchanged. RTT is
measured locally with a monotonic clock; clock synchronization is not required.

## Reliable-delivery behavior

Reliability is optional per user message. An unreliable send succeeds once its
frame enters the local serial queue. A reliable send succeeds only after the
remote daemon validates the `USER_MESSAGE` and returns an ACK.

Defaults are a 300 ms ACK timeout and three retransmissions after the initial
attempt. A receiver retains a bounded, expiring sequence cache. A duplicate is
ACKed again but is not delivered to local subscribers again. A malformed
reliable message is NACKed. CRC failures are silently discarded because the
associated header cannot be trusted.

Delivery is daemon-to-daemon. Applications needing confirmation that a command
was executed must define a separate application-level response topic.

## Heartbeat and status behavior

Each daemon sends a heartbeat once per second and immediately ACKs valid remote
heartbeats. Heartbeat replies provide RTT and rolling heartbeat-loss metrics.

- `DISCONNECTED`: no valid radio frame has arrived yet or the last valid frame
  is at least `disconnect_after` seconds old (default 3 seconds).
- `DEGRADED`: frames are current, but RTT exceeds `degraded_rtt_ms` (default 500)
  or rolling heartbeat loss exceeds `degraded_loss` (default 0.20).
- `CONNECTED`: frames are current and neither degraded threshold is exceeded.

Serial-device connection is separate from RF link state. A daemon can report
`serial_connected=true` and `state=DISCONNECTED` when its local radio is open
but no valid frames arrive from the remote radio.

## Local Unix-socket transport

The default socket is `/run/sik-link/sik-link.sock`. Every local packet is:

```text
4-byte unsigned big-endian MessagePack length | MessagePack map
```

The default local packet maximum is 1 MiB. Requests use a client-selected integer
`id`; direct responses repeat that `id`. Events contain `event` and no request
ID. Multiple requests may be outstanding concurrently.

### Send

Request:

```text
{"id": 1, "op": "send", "topic": "illumination",
 "data": {"intensity": 0.65}, "reliable": true}
```

Successful unreliable response:

```text
{"id": 1, "seq": 42, "ok": true, "reliable": false}
```

Successful reliable response, returned after the radio ACK:

```text
{"id": 1, "seq": 42, "ok": true, "reliable": true, "error": nil}
```

Failure responses set `ok=false` and provide `error`, such as `timeout`, a queue
capacity error, an invalid topic, or an oversized encoded radio payload.

### Status

Request and response:

```text
{"id": 2, "op": "status"}
{"id": 2, "ok": true, "status": STATUS_MAP}
```

`STATUS_MAP` contains:

| Key | Type | Meaning |
| --- | --- | --- |
| `state` | string | `CONNECTED`, `DEGRADED`, or `DISCONNECTED`. |
| `serial_connected` | bool | Whether the local serial port is currently open. |
| `serial_device` | string or nil | Currently open stable/path device name. |
| `uptime_ms` | int | Daemon protocol-engine uptime. |
| `last_rx_age_s` | float or nil | Age of the last valid radio frame. |
| `rtt_ms` | float or nil | Most recently measured heartbeat RTT. |
| `heartbeat_loss` | float | Lost fraction in the configured rolling window, from 0 to 1. |
| `tx_frames`, `rx_frames` | int | Valid protocol frames transmitted and received. |
| `tx_bytes`, `rx_bytes` | int | Encoded transmit bytes and received payload bytes counted by the engine. |
| `crc_errors`, `parser_errors` | int | Detected CRC and structural/payload errors. |
| `retries`, `duplicates`, `reliable_timeouts` | int | Reliability counters. |
| `serial_disconnects` | int | Transitions from an open to unavailable serial device. |
| `tx_queue_depth` | int | Frames currently awaiting serial transmission. |

### Subscribe and events

Request:

```text
{"id": 3, "op": "subscribe", "topics": ["robot_status"], "status": true}
```

Use `topics=["*"]` for all application topics. `status=true` enables link and
serial lifecycle events. The response is `{"id": 3, "ok": true}`.

Application event:

```text
{"event": "message", "topic": "robot_status", "data": {...},
 "seq": 91, "reliable": false}
```

`link_status` events contain the full status map and are emitted when the link
state changes. `serial_status` events contain `connected`, `device`, and `error`.
Subscriptions are connection-scoped and are discarded when the client closes.

## Application schema guidance

Topics are non-empty UTF-8 strings. Keep them stable, lowercase, and grouped by
domain, for example `illumination/command` and `robot/status`. The transport does
not validate the contents of `data`; cooperating applications own that schema.

For each application topic, document required keys, units, ranges, and response
topics in application code or a future schema document. Avoid embedding transport
metadata such as retries or sequence numbers inside `data`. Keep the encoded
`USER_MESSAGE` map within `protocol.max_payload`.

Example command and application-level completion response:

```text
topic: illumination/command
data:  {"request_id": "9f31", "mode": "manual", "intensity": 0.65}

topic: illumination/result
data:  {"request_id": "9f31", "accepted": true, "applied_intensity": 0.65}
```

The radio ACK for the first message only confirms remote-daemon receipt. The
second topic confirms application processing.
