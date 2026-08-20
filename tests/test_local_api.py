import asyncio
import os
import pty
import select
import threading
from pathlib import Path

from siklink.client import SikLinkClient
from siklink.config import AppConfig, ProtocolConfig, SerialConfig, SocketConfig
from siklink.daemon import SikLinkDaemon


def test_status_subscribe_and_unreliable_send(tmp_path: Path):
    async def scenario():
        socket_path = tmp_path / "sik.sock"
        config = AppConfig(
            "test",
            SerialConfig(device=str(tmp_path / "missing"), reconnect_initial=0.01, reconnect_max=0.02),
            ProtocolConfig(heartbeat_interval=10),
            SocketConfig(path=str(socket_path)),
        )
        daemon = SikLinkDaemon(config)
        # Keep the test independent of serial hardware while exercising the full socket API.
        daemon.transport.send = lambda *_: True
        task = asyncio.create_task(daemon.run())
        for _ in range(100):
            if socket_path.exists():
                break
            await asyncio.sleep(0.01)
        try:
            async with SikLinkClient(str(socket_path)) as client:
                status = await client.status()
                assert status["state"] == "DISCONNECTED"
                await client.subscribe(["robot_status"], status=True)
                sent = await client.send("command", {"go": True}, reliable=False)
                assert sent["ok"] is False
                assert "disconnected" in sent["error"]
                daemon._event("message", {"topic": "robot_status", "data": {"battery": 80}, "seq": 1})
                event = await asyncio.wait_for(anext(client.events()), 1)
                assert event["topic"] == "robot_status"
                assert event["data"] == {"battery": 80}
        finally:
            daemon.stop()
            await asyncio.wait_for(task, 2)

    asyncio.run(scenario())


def test_two_daemons_exchange_reliable_message_over_virtual_serial(tmp_path: Path):
    async def scenario():
        master_a, slave_a = pty.openpty()
        master_b, slave_b = pty.openpty()
        stop_bridge = threading.Event()

        def bridge():
            while not stop_bridge.is_set():
                readable, _, _ = select.select([master_a, master_b], [], [], 0.05)
                for source in readable:
                    target = master_b if source == master_a else master_a
                    try:
                        data = os.read(source, 4096)
                        if data:
                            os.write(target, data)
                    except OSError:
                        return

        protocol = ProtocolConfig(heartbeat_interval=0.2, ack_timeout=0.1, max_retries=3)
        daemon_a = SikLinkDaemon(
            AppConfig("a", SerialConfig(device=os.ttyname(slave_a), read_timeout=0.01), protocol,
                      SocketConfig(path=str(tmp_path / "a.sock")))
        )
        daemon_b = SikLinkDaemon(
            AppConfig("b", SerialConfig(device=os.ttyname(slave_b), read_timeout=0.01), protocol,
                      SocketConfig(path=str(tmp_path / "b.sock")))
        )
        task_a = asyncio.create_task(daemon_a.run())
        task_b = asyncio.create_task(daemon_b.run())
        bridge_thread = threading.Thread(target=bridge, daemon=True)
        try:
            for _ in range(100):
                if daemon_a.engine.metrics.serial_connected and daemon_b.engine.metrics.serial_connected:
                    break
                await asyncio.sleep(0.01)
            bridge_thread.start()
            async with SikLinkClient(str(tmp_path / "a.sock")) as client_a, SikLinkClient(
                str(tmp_path / "b.sock")
            ) as client_b:
                await client_b.subscribe(["commands"], status=False)
                for _ in range(100):
                    if (await client_a.status())["state"] != "DISCONNECTED":
                        break
                    await asyncio.sleep(0.01)
                result = await asyncio.wait_for(client_a.send("commands", {"go": True}, reliable=True), 1)
                event = await asyncio.wait_for(anext(client_b.events()), 1)
                assert result["ok"] is True
                assert event["topic"] == "commands"
                assert event["data"] == {"go": True}
        finally:
            daemon_a.stop()
            daemon_b.stop()
            await asyncio.gather(task_a, task_b)
            stop_bridge.set()
            bridge_thread.join(timeout=1)
            for descriptor in (master_a, slave_a, master_b, slave_b):
                os.close(descriptor)

    asyncio.run(scenario())
