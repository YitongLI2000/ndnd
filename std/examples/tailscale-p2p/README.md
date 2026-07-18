# NDNd Tailscale P2P Smoke Test

This example is the smallest real-machine NDNd test between Agent A (Linux)
and a Mac. It uses only two NDNd forwarders, the built-in `ping` tools, one
static route, and Tailscale. It does not start IOA, Docker, DV routing, or any
MARS component.

```text
Linux ndnd ping
  -> Linux NDNd (TCP 127.0.0.1:6363)
  -> TCP face tcp4://127.0.0.1:16363
  -> tailscale-nc bridge
  -> Agent A isolated userspace Tailscale
  -> Mac Tailscale address:6363
  -> Mac NDNd
  -> ndnd pingserver /p2p/mac
```

Both Tailscale peers must be online at the same time for the cross-machine
test. The Linux script starts only Agent A's isolated Tailscale daemon; the
rest of IOA remains stopped. The Mac must have its normal Tailscale client
online before the test starts.

## Why Agent A needs a bridge

Agent A runs `tailscaled` with `--tun=userspace-networking` and a private Unix
socket. It therefore has no `tailscale0` interface or host route to a Mac
`100.x.y.z` address. A direct face such as
`tcp4://100.x.y.z:6363` cannot work on this host.

`tailscale-nc-bridge.py` exposes `127.0.0.1:16363` and opens each outgoing
connection with `tailscale --socket=... nc`. NDNd still uses an ordinary TCP
face, while the bridge selects the correct userspace Tailscale instance. It is
normal for `fw face-list` to show the remote endpoint as
`tcp4://127.0.0.1:16363` in this smoke test.

On a different Linux host with a kernel-mode Tailscale interface and a working
route to `100.64.0.0/10`, the bridge is unnecessary; that host can create an
NDNd TCP face directly to the Mac Tailscale address.

## Prerequisites

- The repository is on the same revision on Linux and macOS.
- Native `ndnd` binaries can be built on both machines with Go 1.24.3 or newer,
  or an existing native binary is available.
- The Mac Tailscale client is online and the Mac firewall allows its NDNd TCP
  listener on port 6363.
- Commands are run from the repository root.

Build a native binary manually when needed:

```sh
CGO_ENABLED=0 go build -o ndnd ./cmd/ndnd
```

Do not copy a Linux binary to macOS. If Linux cannot build locally, the Mac can
cross-compile a Linux/amd64 binary and transfer it:

```sh
mkdir -p _build
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go build -o _build/ndnd-linux-amd64 ./cmd/ndnd
```

## Recommended two-command test

### 1. Start the Mac

On the Mac, keep this command running:

```sh
std/examples/tailscale-p2p/run-macos.sh serve
```

The runner detects the Mac Tailscale IPv4 address, builds `ndnd`, starts the
Mac forwarder, and registers `/p2p/mac` with `pingserver`. It then prints the
Linux command. Use `SKIP_BUILD=1` to reuse an existing `./ndnd` binary.

Every actual run creates a timestamped checkpoint under `logs/`:

```text
std/examples/tailscale-p2p/logs/
└── checkpoint-20260719-153045-serve/
    ├── checkpoint.txt
    ├── forwarder.log
    └── pingserver.log
```

`local-test` checkpoints also contain `client.log`. `checkpoint.txt` records
the start and finish times, mode, exit status, Git commit, Go version, prefix,
and peer addresses. Checkpoints are kept after exit and ignored by Git. Set
`NDN_LOG_DIR=/another/path` to override the checkpoint root.

Before involving Linux, the Mac side can be checked entirely locally:

```sh
std/examples/tailscale-p2p/run-macos.sh local-test
```

If a Go module proxy is unavailable, select another proxy only for this run:

```sh
GOPROXY=https://goproxy.cn,direct \
  std/examples/tailscale-p2p/run-macos.sh serve
```

### 2. Run the Linux test

With the Mac runner still active, run this on Agent A:

```sh
std/examples/tailscale-p2p/run-linux.sh
```

The default target is the Tailscale name `castermacbook`. To use the numeric
address printed by the Mac runner instead:

```sh
MAC_TS_HOST=100.77.224.3 \
  std/examples/tailscale-p2p/run-linux.sh
```

The Linux runner performs the complete one-shot test:

1. Reuses or starts only Agent A's private userspace Tailscale daemon.
2. Runs `tailscale ping` through its private socket.
3. Builds the Linux `ndnd` binary unless `SKIP_BUILD=1` is set.
4. Starts the local bridge and Linux NDNd forwarder.
5. Adds `/p2p/mac` through `tcp4://127.0.0.1:16363`.
6. Prints the face and route tables and sends five NDN pings.
7. Stops the forwarder and bridge. If it started Tailscale, it stops that too.

The runner derives `GOROOT` from the selected Go executable, so an unrelated
inherited `GOROOT` does not mix standard libraries from different Go versions.

Use a prebuilt binary without rebuilding:

```sh
SKIP_BUILD=1 NDND_BINARY=/path/to/ndnd \
  std/examples/tailscale-p2p/run-linux.sh
```

Set `KEEP_TAILSCALE=1` only when the private Agent A Tailscale daemon should
remain running after the test. A daemon that was already running before the
script is always left running.

## Expected result

Linux should print five lines containing `content from /p2p/mac` and finish
with `P2P NDN ping test passed.` The Mac terminal should print five
`interest received` lines.

Only Linux needs the static route for this test. The returned Data follows the
reverse PIT path, so the Mac does not need a route back to Linux.

## Manual Linux workflow

The automated runner is preferred, but these commands expose the exact Agent A
path for diagnosis. Start only its private Tailscale instance:

```sh
/srv/yitong/ioa/scripts/agent_a_tailscale_nc/start_tailscaled.sh

TS_SOCKET=/srv/yitong/ioa/.ioa_runtime/agent_a/tailscale/run/tailscaled.sock
/usr/bin/tailscale --socket="${TS_SOCKET}" status
/usr/bin/tailscale --socket="${TS_SOCKET}" ping castermacbook
```

In a second Linux terminal, start the bridge:

```sh
TS_SOCKET=/srv/yitong/ioa/.ioa_runtime/agent_a/tailscale/run/tailscaled.sock
python3 std/examples/tailscale-p2p/tailscale-nc-bridge.py \
  --listen-host 127.0.0.1 \
  --listen-port 16363 \
  --target-host castermacbook \
  --target-port 6363 \
  --tailscale-socket "${TS_SOCKET}"
```

In a third terminal, start the Linux forwarder:

```sh
export NDN_CLIENT_TRANSPORT=tcp://127.0.0.1:6363
./ndnd fw run std/examples/tailscale-p2p/fw.yml
```

In a fourth terminal, add the bridged face and run the ping:

```sh
export NDN_CLIENT_TRANSPORT=tcp://127.0.0.1:6363
./ndnd fw route-add \
  prefix=/p2p/mac \
  persistency=permanent \
  face=tcp4://127.0.0.1:16363
./ndnd fw face-list
./ndnd fw route-list
./ndnd ping /p2p/mac -c 5 -t 2000
```

Stop the manually started Agent A Tailscale daemon after the test:

```sh
/srv/yitong/ioa/scripts/agent_a_tailscale_nc/stop_tailscaled.sh
```

## Troubleshooting

If Linux reports that the Mac Tailscale peer is unreachable, confirm that the
Mac client and the Agent A private daemon are both online. Always pass the
private socket on Agent A; a plain `tailscale status` may inspect an unrelated
system or another user's daemon.

If Tailscale ping succeeds but NDN ping times out, check the Mac while its
runner is active:

```sh
lsof -nP -iTCP:6363 -sTCP:LISTEN
```

Then inspect the runtime directory printed by the Linux runner. `bridge.log`
contains errors from `tailscale nc`, while `forwarder.log` contains NDNd face
and forwarding events. Also check the Mac firewall and Tailscale policy for
TCP port 6363.

If a local NDNd command reports `Unable to start engine`, confirm that the
local forwarder is running and that this variable is set in the same terminal:

```sh
export NDN_CLIENT_TRANSPORT=tcp://127.0.0.1:6363
```

The forwarder listener binds host interfaces, not only the Tailscale address.
Restrict TCP port 6363 to trusted peers during experiments beyond this smoke
test.
