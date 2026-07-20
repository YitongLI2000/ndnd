#!/usr/bin/env python3
"""Expose a local TCP port through an isolated userspace Tailscale socket."""

import argparse
import asyncio
import sys


async def relay(reader, writer, close_writer):
    try:
        while True:
            data = await reader.read(65536)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    finally:
        if close_writer:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
        else:
            try:
                writer.write_eof()
            except Exception:
                pass


async def copy_stderr(process, peer):
    if process.stderr is None:
        return

    while True:
        line = await process.stderr.readline()
        if not line:
            return
        message = line.decode(errors="replace").rstrip()
        print(f"[bridge] tailscale nc stderr for {peer}: {message}", flush=True)


async def handle_connection(args, local_reader, local_writer):
    peer = local_writer.get_extra_info("peername")
    process = None
    relay_tasks = []
    stderr_task = None
    print(f"[bridge] accepted local connection from {peer}", flush=True)

    try:
        process = await asyncio.create_subprocess_exec(
            args.tailscale_bin,
            f"--socket={args.tailscale_socket}",
            "nc",
            args.target_host,
            str(args.target_port),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        relay_tasks = [
            asyncio.create_task(
                relay(local_reader, process.stdin, close_writer=False)
            ),
            asyncio.create_task(
                relay(process.stdout, local_writer, close_writer=True)
            ),
        ]
        stderr_task = asyncio.create_task(copy_stderr(process, peer))

        done, pending = await asyncio.wait(
            relay_tasks, return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        for task in done:
            task.result()
    except Exception as error:
        print(f"[bridge] connection from {peer} failed: {error}", flush=True)
        try:
            local_writer.close()
            await local_writer.wait_closed()
        except Exception:
            pass
    finally:
        for task in relay_tasks:
            if not task.done():
                task.cancel()
        if relay_tasks:
            await asyncio.gather(*relay_tasks, return_exceptions=True)

        if process is not None and process.returncode is None:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=3)
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()

        if stderr_task is not None and not stderr_task.done():
            stderr_task.cancel()
            await asyncio.gather(stderr_task, return_exceptions=True)


async def probe(args):
    process = await asyncio.create_subprocess_exec(
        args.tailscale_bin,
        f"--socket={args.tailscale_socket}",
        "nc",
        args.target_host,
        str(args.target_port),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    # tailscale nc may defer the remote dial until stdin receives data.
    if process.stdin is not None:
        process.stdin.write(b"\x00")
        await process.stdin.drain()

    try:
        await asyncio.wait_for(process.wait(), timeout=args.probe_wait)
    except asyncio.TimeoutError:
        process.terminate()
        await process.communicate()
        print(
            f"[probe] {args.target_host}:{args.target_port} accepted "
            "a Tailscale TCP connection",
            flush=True,
        )
        return

    _, stderr = await process.communicate()
    if process.returncode != 0:
        message = stderr.decode(errors="replace").strip()
        raise RuntimeError(message or f"tailscale nc exited {process.returncode}")

    print(
        f"[probe] {args.target_host}:{args.target_port} accepted and closed "
        "a Tailscale TCP connection",
        flush=True,
    )


async def run(args):
    server = await asyncio.start_server(
        lambda reader, writer: handle_connection(args, reader, writer),
        args.listen_host,
        args.listen_port,
    )
    listeners = ", ".join(
        str(sock.getsockname()) for sock in (server.sockets or [])
    )
    print(
        f"[bridge] listening on {listeners}; "
        f"target={args.target_host}:{args.target_port}; "
        f"socket={args.tailscale_socket}",
        flush=True,
    )

    async with server:
        await server.serve_forever()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Forward local TCP connections with `tailscale nc`."
    )
    parser.add_argument("--listen-host", default="127.0.0.1")
    parser.add_argument("--listen-port", type=int, default=16363)
    parser.add_argument("--target-host", required=True)
    parser.add_argument("--target-port", type=int, default=6363)
    parser.add_argument("--tailscale-socket", required=True)
    parser.add_argument("--tailscale-bin", default="/usr/bin/tailscale")
    parser.add_argument(
        "--probe",
        action="store_true",
        help="check the target listener and exit instead of serving",
    )
    parser.add_argument(
        "--probe-wait",
        type=float,
        default=3.0,
        help="seconds a live tailscale-nc process must remain open",
    )
    return parser.parse_args()


if __name__ == "__main__":
    try:
        parsed_args = parse_args()
        if parsed_args.probe_wait <= 0:
            raise ValueError("--probe-wait must be greater than zero")
        asyncio.run(probe(parsed_args) if parsed_args.probe else run(parsed_args))
    except KeyboardInterrupt:
        pass
    except (OSError, RuntimeError, ValueError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(1) from error
