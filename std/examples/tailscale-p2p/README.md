# NDNd Tailscale P2P Smoke Test

This example verifies the smallest useful real-machine NDNd deployment:

```text
Linux ping client
  -> local NDNd forwarder
  -> NDN-over-TCP through Tailscale
  -> macOS NDNd forwarder
  -> macOS ping server
```

It uses only the `ndnd` binary, the built-in `ping` and `pingserver` tools,
and one static route. It does not use Docker, DV routing, or a custom app.

## 1. Prerequisites

- Linux and macOS are online in the same Tailscale tailnet.
- Tailscale policy and the macOS firewall allow Linux to reach TCP port 6363
  on the Mac.
- A native `ndnd` binary is available on both machines.
- Run every command from the repository root.

Build the binary locally if needed (Go 1.24.3 or newer):

```sh
CGO_ENABLED=0 go build -o ndnd ./cmd/ndnd
```

Use a binary built for the local OS and architecture. Do not copy the Linux
binary to macOS.

Find the Mac Tailscale IPv4 address and verify the overlay path from Linux:

```sh
# Run on macOS and record the 100.x.y.z address.
tailscale ip -4

# Run on Linux, replacing the example address.
tailscale ping 100.x.y.z
```

Use the numeric Tailscale IPv4 address for this first test instead of a DNS
name. The rest of this guide calls it `MAC_TS_IP`.

## 2. Start the macOS side

Open terminal 1 on the Mac and start the forwarder:

```sh
export NDN_CLIENT_TRANSPORT=tcp://127.0.0.1:6363
./ndnd fw run std/examples/tailscale-p2p/fw.yml
```

Keep it running. This example intentionally uses `ndnd fw run`, not
`ndnd daemon`, so that every OS uses TCP for the local application connection.

Open terminal 2 on the Mac, set the same environment variable, verify the
forwarder, and start the built-in server:

```sh
export NDN_CLIENT_TRANSPORT=tcp://127.0.0.1:6363
./ndnd fw status
./ndnd pingserver /p2p/mac
```

The server registers `/p2p/mac` with the local Mac forwarder and then waits for
Interests.

## 3. Start the Linux side

Open terminal 1 on Linux and start the forwarder with the same configuration:

```sh
export NDN_CLIENT_TRANSPORT=tcp://127.0.0.1:6363
./ndnd fw run std/examples/tailscale-p2p/fw.yml
```

Open terminal 2 on Linux. Set the local client transport and enter the actual
Mac Tailscale IPv4 address:

```sh
export NDN_CLIENT_TRANSPORT=tcp://127.0.0.1:6363
MAC_TS_IP=100.x.y.z
```

Install a static route. This command also creates a permanent TCP face to the
Mac forwarder:

```sh
./ndnd fw route-add \
  prefix=/p2p/mac \
  persistency=permanent \
  face=tcp4://${MAC_TS_IP}:6363
```

Verify the resulting face and route:

```sh
./ndnd fw face-list
./ndnd fw route-list
```

The face list should contain `remote=tcp4://<MAC_TS_IP>:6363`, and the route
list should contain `prefix=/p2p/mac`.

## 4. Run the test

On Linux:

```sh
./ndnd ping /p2p/mac -c 5 -t 2000
```

Success has two visible results:

- Linux prints five `content from /p2p/mac` responses and RTT values.
- The Mac ping server prints five `interest received` lines.

Data follows the reverse PIT path, so the Mac does not need a static route back
to Linux for this one-way ping test.

## 5. Troubleshooting

If a local command reports `Unable to start engine`:

- Confirm the local forwarder is still running.
- Confirm `NDN_CLIENT_TRANSPORT=tcp://127.0.0.1:6363` is set in that terminal.

If the ping times out, check the underlay before NDNd:

```sh
# Linux to Mac
tailscale ping ${MAC_TS_IP}
nc -vz ${MAC_TS_IP} 6363

# macOS: confirm the NDNd TCP listener
lsof -nP -iTCP:6363 -sTCP:LISTEN
```

Then inspect NDNd on Linux:

```sh
./ndnd fw face-list
./ndnd fw route-list
```

Also check the Tailscale policy, the macOS firewall prompt, and any host
firewall rules. The NDNd TCP listener binds all host interfaces, so port 6363
should be restricted to the Tailscale peers during this test.

On Linux, the current forwarder may still log a UDP listener even though UDP is
disabled in `fw.yml`. This does not affect the TCP path in this smoke test.

## 6. Stop and reset

Press Ctrl-C in the pingserver and forwarder terminals. Faces and routes in this
example are runtime state and disappear when the Linux forwarder exits, so the
`route-add` command must be run again after a restart.
