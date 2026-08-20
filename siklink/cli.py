"""Command-line interface for daemon, clients, and hardware validation."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from typing import Any

import serial

from .client import SikLinkClient
from .config import SerialConfig, load_config
from .daemon import run_daemon
from .serial_transport import discover_devices, resolve_device

DEFAULT_SOCKET = "/run/sik-link/sik-link.sock"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="sik-link", description="SiK transparent serial link")
    sub = parser.add_subparsers(dest="command", required=True)

    daemon = sub.add_parser("daemon", help="run the link daemon")
    daemon.add_argument("--config", required=True)

    status = sub.add_parser("status", help="show current link status")
    status.add_argument("--socket", default=DEFAULT_SOCKET)
    status.add_argument("--json", action="store_true")

    send = sub.add_parser("send", help="send a structured message")
    send.add_argument("topic")
    send.add_argument("message", help="JSON value")
    send.add_argument("--reliable", action="store_true")
    send.add_argument("--socket", default=DEFAULT_SOCKET)

    monitor = sub.add_parser("monitor", help="monitor messages and link changes")
    monitor.add_argument("--socket", default=DEFAULT_SOCKET)
    monitor.add_argument("--topic", action="append", dest="topics")
    monitor.add_argument("--json", action="store_true")

    raw = sub.add_parser("serial-test", help="exercise a serial device without the daemon")
    raw.add_argument("--device", default="auto")
    raw.add_argument("--match", default="")
    raw.add_argument("--baud", type=int, default=57600)
    raw.add_argument("--send", default="")
    raw.add_argument("--send-hex", default="")
    raw.add_argument("--interval", type=float, default=1.0)
    raw.add_argument("--duration", type=float, default=0.0, help="seconds; zero runs until Ctrl-C")
    raw.add_argument("--list", action="store_true")

    link = sub.add_parser("link-test", help="measure an end-to-end daemon link")
    link.add_argument("--socket", default=DEFAULT_SOCKET)
    link.add_argument("--echo", action="store_true", help="reply to link-test probes")
    link.add_argument("--count", type=int, default=20)
    link.add_argument("--interval", type=float, default=0.25)
    link.add_argument("--timeout", type=float, default=2.0)
    return parser


def _print_status(status: dict[str, Any]) -> None:
    age = status.get("last_rx_age_s")
    rtt = status.get("rtt_ms")
    print(f"LINK: {status['state']}")
    print(f"Serial: {'connected' if status.get('serial_connected') else 'disconnected'}")
    print(f"Device: {status.get('serial_device') or 'unavailable'}")
    print(f"RTT: {'unavailable' if rtt is None else f'{rtt:.1f} ms'}")
    print(f"Heartbeat loss: {status.get('heartbeat_loss', 0) * 100:.1f}%")
    print(f"RX age: {'unavailable' if age is None else f'{age:.2f} s'}")
    print(f"Frames: tx={status.get('tx_frames', 0)} rx={status.get('rx_frames', 0)}")
    print(f"Errors: crc={status.get('crc_errors', 0)} parser={status.get('parser_errors', 0)}")
    print(
        f"Recovery: port_reopens={status.get('serial_recoveries', 0)} "
        f"usb_resets={status.get('usb_resets', 0)} "
        f"usb_reset_failures={status.get('usb_reset_failures', 0)} "
        f"serial_disconnects={status.get('serial_disconnects', 0)}"
    )
    if status.get("last_serial_error"):
        print(f"Last serial event: {status['last_serial_error']}")
    if status.get("last_usb_reset_error"):
        print(f"Last USB reset error: {status['last_usb_reset_error']}")


async def _status(args: argparse.Namespace) -> None:
    async with SikLinkClient(args.socket) as client:
        value = await client.status()
    if args.json:
        print(json.dumps(value, separators=(",", ":")))
    else:
        _print_status(value)


async def _send(args: argparse.Namespace) -> None:
    try:
        message = json.loads(args.message)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"invalid JSON message: {exc}") from exc
    async with SikLinkClient(args.socket) as client:
        result = await client.send(args.topic, message, args.reliable)
    if result.get("ok"):
        if args.reliable:
            print(f"ACK seq={result['seq']}")
        else:
            print(f"SENT seq={result['seq']}")
    else:
        raise SystemExit(f"send failed seq={result.get('seq', '?')}: {result.get('error', 'unknown error')}")


async def _monitor(args: argparse.Namespace) -> None:
    async with SikLinkClient(args.socket) as client:
        await client.subscribe(args.topics or ["*"], status=True)
        _print_status(await client.status())
        async for event in client.events():
            if args.json:
                print(json.dumps(event, separators=(",", ":")), flush=True)
            elif event["event"] == "message":
                print(f"RX topic={event['topic']} seq={event['seq']} data={event['data']!r}", flush=True)
            else:
                print(f"{event['event']}: {event}", flush=True)


def _serial_test(args: argparse.Namespace) -> None:
    if args.list:
        devices = discover_devices(args.match)
        if not devices:
            print("No matching devices found")
        else:
            print("\n".join(devices))
        return
    config = SerialConfig(device=args.device, match=args.match, baud=args.baud)
    payload = bytes.fromhex(args.send_hex) if args.send_hex else args.send.encode()
    started = time.monotonic()
    next_send = started
    rx_bytes = tx_bytes = sends = 0
    interrupted = False
    print("Press Ctrl-C to stop")
    while not interrupted:
        if args.duration and time.monotonic() - started >= args.duration:
            break
        try:
            device = resolve_device(config)
            print(f"CONNECTED {device} at {config.baud} baud", flush=True)
            with serial.Serial(device, config.baud, timeout=0.1, write_timeout=1.0) as port:
                while not args.duration or time.monotonic() - started < args.duration:
                    now = time.monotonic()
                    if payload and now >= next_send:
                        packet = payload + (f" #{sends}\n".encode() if not args.send_hex else b"")
                        port.write(packet)
                        tx_bytes += len(packet)
                        sends += 1
                        next_send = now + args.interval
                    data = port.read(port.in_waiting or 1)
                    if data:
                        rx_bytes += len(data)
                        print(f"RX {len(data):4d} B hex={data.hex(' ')} text={data!r}", flush=True)
        except KeyboardInterrupt:
            interrupted = True
        except (OSError, RuntimeError, ValueError, serial.SerialException) as exc:
            print(f"DISCONNECTED {exc}; retrying", file=sys.stderr, flush=True)
            time.sleep(0.5)
    elapsed = max(time.monotonic() - started, 0.001)
    print(f"SUMMARY duration={elapsed:.1f}s tx={tx_bytes}B rx={rx_bytes}B sends={sends}")


async def _link_test(args: argparse.Namespace) -> None:
    async with SikLinkClient(args.socket) as client:
        if args.echo:
            await client.subscribe(["_siklink/link_test"], status=False)
            print("Echoing link-test probes; press Ctrl-C to stop")
            async for event in client.events():
                await client.send("_siklink/link_test_reply", event["data"], reliable=False)
            return
        await client.subscribe(["_siklink/link_test_reply"], status=False)
        received: dict[int, float] = {}

        async def collect() -> None:
            async for event in client.events():
                data = event.get("data")
                if isinstance(data, dict) and isinstance(data.get("probe"), int):
                    received[data["probe"]] = (time.monotonic() - data["sent_monotonic"]) * 1000

        collector = asyncio.create_task(collect())
        started = time.monotonic()
        for probe in range(args.count):
            await client.send(
                "_siklink/link_test", {"probe": probe, "sent_monotonic": time.monotonic()}, reliable=False
            )
            await asyncio.sleep(args.interval)
        deadline = time.monotonic() + args.timeout
        while len(received) < args.count and time.monotonic() < deadline:
            await asyncio.sleep(0.05)
        collector.cancel()
        elapsed = time.monotonic() - started
        loss = 1 - len(received) / args.count if args.count else 0
        values = list(received.values())
        print(f"probes={args.count} replies={len(received)} loss={loss * 100:.1f}% duration={elapsed:.1f}s")
        if values:
            print(f"rtt_ms min={min(values):.1f} avg={sum(values)/len(values):.1f} max={max(values):.1f}")


def main() -> None:
    args = _parser().parse_args()
    try:
        if args.command == "daemon":
            config = load_config(args.config)
            logging.basicConfig(
                level=getattr(logging, config.log_level, logging.INFO),
                format="%(asctime)s %(levelname)s %(name)s: %(message)s",
            )
            asyncio.run(run_daemon(config))
        elif args.command == "status":
            asyncio.run(_status(args))
        elif args.command == "send":
            asyncio.run(_send(args))
        elif args.command == "monitor":
            asyncio.run(_monitor(args))
        elif args.command == "serial-test":
            _serial_test(args)
        elif args.command == "link-test":
            asyncio.run(_link_test(args))
    except (ConnectionError, FileNotFoundError) as exc:
        raise SystemExit(str(exc)) from exc


if __name__ == "__main__":
    main()
