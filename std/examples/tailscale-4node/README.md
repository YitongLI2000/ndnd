# Four-Node NDNd over Tailscale: Mac-Hub Test Plan

This directory extends the successful Linux-to-Mac P2P test to four physical
machines. The first milestone intentionally stays small:

- Tailscale provides the IP underlay.
- NDNd uses unicast NDN-over-TCP faces.
- The Mac is the only NDN transit hub.
- Static NDN routes select the next hop.
- The built-in `ping` and `pingserver` commands are the only applications.

Docker, DV routing, IOA services, and MARS applications are not required for
this experiment. Native binaries keep the first cross-platform diagnosis as
simple as possible.

## 1. Nodes and roles

All four machines must be online in the same Tailscale tailnet while the test
is running.

| Node ID | NDN role | OS | Tailscale name | Tailscale IPv4 | Link to Mac |
|---|---|---|---|---|---|
| `mac` | Central forwarding hub | macOS | `castermacbook` | `100.77.224.3` | Local |
| `agent-a` | Edge | Linux | `dd-yitong-ioa-agent-a` | `100.81.98.57` | Isolated userspace Tailscale plus `tailscale nc` bridge |
| `coordinator` | Edge / application coordinator | Windows | `desktop-coordinator` | `100.92.115.14` | Native Tailscale TCP |
| `agent-b` | Edge | Linux | `ioa-agent-b-shengyi` | `100.85.242.9` | Native Tailscale TCP, to be confirmed on Agent B |

The word `coordinator` remains the Windows node ID and application role. It
does **not** mean that Windows is the NDN forwarding center in this test.

Each node serves one prefix with `ndnd pingserver`:

| Node | Served prefix |
|---|---|
| Mac | `/ndn/4node/mac` |
| Agent A | `/ndn/4node/agent-a` |
| Windows coordinator | `/ndn/4node/coordinator` |
| Agent B | `/ndn/4node/agent-b` |

## 2. Logical topology

The Tailscale tailnet permits IP reachability among all four machines, but the
NDN topology is deliberately a Mac-centered star:

```text
                         Mac hub
                  100.77.224.3:6363
                  /        |        \
                 /         |         \
          NDN/TCP       NDN/TCP      NDN/TCP
               /           |            \
              /            |             \
       Linux Agent A     Windows       Linux Agent B
       100.81.98.57      coordinator   100.85.242.9
                         100.92.115.14
```

There are exactly three long-lived inter-forwarder connections. Agent A,
Windows, and Agent B each initiate one connection to the Mac. No edge installs
a direct NDN face or route to another edge. Therefore an Interest from Agent A
to Agent B follows:

```text
Agent A forwarder -> Mac forwarder -> Agent B forwarder
```

Tailscale is a routed Layer-3 overlay, not a shared Layer-2 broadcast domain.
This experiment relies only on unicast addresses and does not require
multicast discovery.

## 3. Static routing design

Each edge has one parent route toward the Mac:

| Forwarder | Prefix | Next hop |
|---|---|---|
| Agent A | `/ndn/4node` | Local bridge `tcp4://127.0.0.1:16363`, then `tailscale nc` to Mac |
| Windows | `/ndn/4node` | Mac `tcp4://100.77.224.3:6363` |
| Agent B | `/ndn/4node` | Mac `tcp4://100.77.224.3:6363` |

The Mac installs one specific route for every remote edge:

| Forwarder | Prefix | Next hop |
|---|---|---|
| Mac | `/ndn/4node/agent-a` | Agent A's incoming TCP FaceId |
| Mac | `/ndn/4node/coordinator` | Windows' incoming TCP FaceId |
| Mac | `/ndn/4node/agent-b` | Agent B's incoming TCP FaceId |

The Mac ping server registers `/ndn/4node/mac` locally. The Mac must **not**
install a `/ndn/4node` parent route: an unknown name could otherwise be sent
back to an edge and loop. On every edge, its local producer route is more
specific than the parent route, so its own prefix remains local.

### Why the Mac routes use incoming FaceIds

All edges initiate their connection toward the Mac. This is mandatory for
Agent A because its isolated `--tun=userspace-networking` Tailscale daemon
does not expose a host `tailscale0` interface or an inbound NDNd listener.

TCP faces are bidirectional. After an edge sends its warm-up Interests, the
Mac can send Interests back over that edge's existing incoming face. A Mac
`fw face-list` entry should contain a remote URI similar to:

```text
remote=tcp4://100.81.98.57:<ephemeral-port>
```

The numeric FaceId is runtime state. It can change after a disconnect, so the
corresponding Mac route must be rebound after an edge reconnects.

## 4. Included files

```text
tailscale-4node/
|-- README.md
|-- fw.yml
|-- ping-matrix.sh
|-- run-agent-a.sh
|-- run-agent-b.sh
|-- run-edge.sh
|-- run-macos.sh
`-- tailscale-nc-bridge.py
```

- `fw.yml` is the tested TCP-only NDNd configuration: TCP 6363, one
  forwarding thread, and UDP/Unix/WebSocket/HTTP3 disabled.
- `run-macos.sh` builds and runs the Mac hub and its local ping server.
- `run-edge.sh` implements the common Linux edge lifecycle and the parent
  route to the Mac.
- `run-agent-a.sh` selects the tested isolated userspace-Tailscale bridge.
- `run-agent-b.sh` assumes normal kernel/native Tailscale routing by default.
- `ping-matrix.sh` requires actual Data lines rather than trusting only the
  ping process exit status.
- `tailscale-nc-bridge.py` is inherited from the passed Linux-to-Mac P2P path.

There is no Windows automation script yet. The Windows owner should first
confirm the native build, firewall behavior, and commands in this guide. A
PowerShell runner can be added after that path is known to work.

### Current validation status (2026-07-20)

| Component | Status |
|---|---|
| Shared config and Agent A `local-test` | Passed with 5/5 Data |
| Agent A userspace-Tailscale bridge | Inherited from passed Linux-to-Mac P2P test |
| Mac native Tailscale/NDNd path | P2P path passed; new Mac-hub runner still needs Mac validation |
| Agent B native build and runner | Pending on Agent B |
| Windows build and edge setup | Pending on Windows |
| Full 12-direction matrix | Pending all four nodes |

## 5. Preflight

Before starting the four-node run:

1. Synchronize the same Git commit to all machines and record
   `git rev-parse HEAD` on each one.
2. Install Go 1.24.3 or newer, or provide a native prebuilt `ndnd` binary.
3. Confirm all four peers are online with `tailscale status`.
4. Confirm the three edges can `tailscale ping castermacbook`.
5. Confirm TCP 6363 is free on every machine.
6. Permit inbound TCP 6363 on the Mac's Tailscale path and keep it restricted
   to the tailnet.

Agent A must address its isolated daemon through its private socket:

```sh
/usr/bin/tailscale \
  --socket=/srv/yitong/ioa/.ioa_runtime/agent_a/tailscale/run/tailscaled.sock \
  ping castermacbook
```

`run-agent-a.sh serve` starts that isolated daemon if necessary, checks the
Mac, and stops the daemon on cleanup only if this run started it. It does not
start IOA.

## 6. Phase 0: local validation

This phase does not need Tailscale. Run it independently before assembling the
star.

Agent A:

```sh
std/examples/tailscale-4node/run-agent-a.sh local-test
```

Mac:

```sh
std/examples/tailscale-4node/run-macos.sh local-test
```

Agent B:

```sh
std/examples/tailscale-4node/run-agent-b.sh local-test
```

Each script must report five actual Data replies.

On Windows, build from the repository root:

```powershell
$env:CGO_ENABLED = "0"
go build -o ndnd.exe ./cmd/ndnd
```

Start the local forwarder in PowerShell window 1:

```powershell
$env:NDN_CLIENT_TRANSPORT = "tcp://127.0.0.1:6363"
.\ndnd.exe fw run std/examples/tailscale-4node/fw.yml
```

Start the local producer in window 2:

```powershell
$env:NDN_CLIENT_TRANSPORT = "tcp://127.0.0.1:6363"
.\ndnd.exe pingserver /ndn/4node/coordinator
```

Verify it in window 3:

```powershell
$env:NDN_CLIENT_TRANSPORT = "tcp://127.0.0.1:6363"
.\ndnd.exe fw status
.\ndnd.exe fw route-list
.\ndnd.exe ping /ndn/4node/coordinator -c 5 -t 2000
```

Do not continue unless the output contains five
`content from /ndn/4node/coordinator:` lines.

## 7. Phase 1: start the Mac hub first

Make sure native Tailscale is online on the Mac, then run from the repository
root and keep the process open:

```sh
std/examples/tailscale-4node/run-macos.sh serve
```

The runner builds `ndnd`, starts the TCP listener on port 6363, starts
`pingserver /ndn/4node/mac`, and prints the Mac Tailscale address. It does not
install any remote route yet because the edge faces do not exist.

From a native-Tailscale edge, optional transport checks are:

```sh
tailscale ping castermacbook
nc -vz 100.77.224.3 6363
```

## 8. Phase 2: connect all three edges to the Mac

Start each edge only after the Mac listener is ready. Keep every process open.

### Agent A

```sh
std/examples/tailscale-4node/run-agent-a.sh serve
```

The runner creates this tested data path:

```text
Agent A NDNd
  -> tcp4://127.0.0.1:16363
  -> tailscale-nc-bridge.py
  -> tailscale nc castermacbook 6363
  -> Mac NDNd
```

### Agent B

```sh
std/examples/tailscale-4node/run-agent-b.sh serve
```

The default is a direct face to `tcp4://100.77.224.3:6363`. If Agent B also
uses an isolated userspace daemon, supply its own socket and lifecycle scripts
and select the bridge mode:

```sh
TRANSPORT_MODE=tailscale-nc \
TAILSCALE_SOCKET=/path/to/agent-b/tailscaled.sock \
TAILSCALE_PID_FILE=/path/to/agent-b/tailscaled.pid \
TAILSCALE_START_SCRIPT=/path/to/start_tailscaled.sh \
TAILSCALE_STOP_SCRIPT=/path/to/stop_tailscaled.sh \
  std/examples/tailscale-4node/run-agent-b.sh serve
```

### Windows coordinator edge

Keep the Windows forwarder and ping server from Phase 0 running. In the third
PowerShell window, install the parent route to the Mac:

```powershell
$env:NDN_CLIENT_TRANSPORT = "tcp://127.0.0.1:6363"
.\ndnd.exe fw route-add `
  prefix=/ndn/4node `
  persistency=permanent `
  face=tcp4://100.77.224.3:6363
```

Warm the bidirectional connection and verify two actual replies:

```powershell
.\ndnd.exe ping /ndn/4node/mac -c 2 -t 3000
.\ndnd.exe fw face-list
.\ndnd.exe fw route-list
```

The Agent A and Agent B runners perform the same parent-route setup and
two-ping warm-up automatically. At this point every edge should have exactly
one `/ndn/4node` next hop, and that next hop must be the Mac.

## 9. Phase 3: bind the three routes on the Mac

After all edges complete their warm-up, use another Mac terminal:

```sh
export NDN_CLIENT_TRANSPORT=tcp://127.0.0.1:6363
./ndnd fw face-list
```

Identify the three long-lived incoming faces by their remote Tailscale IPv4.
Ignore local `127.0.0.1` application faces and any already-closed preflight
connection.

| Remote IPv4 | Prefix to bind |
|---|---|
| `100.81.98.57` | `/ndn/4node/agent-a` |
| `100.92.115.14` | `/ndn/4node/coordinator` |
| `100.85.242.9` | `/ndn/4node/agent-b` |

Suppose the actual face IDs are 12, 13, and 14. Install the routes with the
IDs shown by this run, not necessarily these example numbers:

```sh
./ndnd fw route-add prefix=/ndn/4node/agent-a face=12
./ndnd fw route-add prefix=/ndn/4node/coordinator face=13
./ndnd fw route-add prefix=/ndn/4node/agent-b face=14
./ndnd fw route-list
./ndnd fw fib-list
```

The Mac route table must contain all three remote prefixes and the local
`/ndn/4node/mac` registration. It must not contain a Mac parent route for
`/ndn/4node`.

## 10. Phase 4: run the 12-direction matrix

Four nodes produce 12 ordered source-to-destination tests. Run the matching
command on each Unix machine:

Agent A:

```sh
std/examples/tailscale-4node/ping-matrix.sh agent-a
```

Mac:

```sh
std/examples/tailscale-4node/ping-matrix.sh mac
```

Agent B:

```sh
std/examples/tailscale-4node/ping-matrix.sh agent-b
```

On Windows:

```powershell
.\ndnd.exe ping /ndn/4node/agent-a -c 5 -t 3000
.\ndnd.exe ping /ndn/4node/mac -c 5 -t 3000
.\ndnd.exe ping /ndn/4node/agent-b -c 5 -t 3000
```

Expected result:

| Source | Agent A | Mac | Windows | Agent B |
|---|---:|---:|---:|---:|
| Agent A | local | 5/5 | 5/5 | 5/5 |
| Mac | 5/5 | local | 5/5 | 5/5 |
| Windows | 5/5 | 5/5 | local | 5/5 |
| Agent B | 5/5 | 5/5 | 5/5 | local |

The total is 60 returned Data packets. Do not accept only a zero process exit
status: each test must contain five matching `content from ...` lines.
`ping-matrix.sh` enforces this check on Unix.

### Prove that edge-to-edge traffic traverses the Mac

Capture `./ndnd fw face-list` on the Mac before and after an edge-to-edge test,
for example Agent A to Agent B. Counters on both the Agent A and Agent B
inter-node faces must increase. The edges should have no direct NDN face
between them.

## 11. Acceptance criteria

The milestone passes only when all of these are true:

- All four nodes use the same source commit.
- The Mac listens on Tailscale TCP 6363 and has three stable edge faces.
- Every edge has `/ndn/4node` routed only toward the Mac.
- The Mac has the three specific edge routes on the correct FaceIds and no
  `/ndn/4node` parent route.
- All 12 ordered tests return 5/5 Data, for a total of 60/60.
- Mac face counters prove that an edge-to-edge Interest used two distinct
  inter-node faces.
- Each node preserves its Git commit, face list, route list, and test logs.

## 12. Failure and recovery check

After the 60/60 baseline succeeds:

1. Stop Agent B.
2. Confirm Agent B Interests fail while the other three nodes still work.
3. Restart Agent B and complete its warm-up to the Mac.
4. Find Agent B's new incoming FaceId on the Mac.
5. Reinstall `/ndn/4node/agent-b` on that FaceId on the Mac.
6. Repeat the affected matrix entries.

This exposes the operational cost of static FaceId routes. DV becomes useful
later when automatic prefix advertisement and reconvergence are required; it
is not needed to prove this first topology.

## 13. Troubleshooting checkpoints

- If an edge cannot reach TCP 6363, check Mac Tailscale state and the macOS
  firewall before investigating NDN routes.
- If an edge can ping `/ndn/4node/mac` but the Mac cannot reach that edge,
  check that the edge's current incoming FaceId is bound to the right prefix.
- If one edge reconnects, assume its old FaceId is stale and inspect/rebind it
  on the Mac.
- If an edge-to-edge test times out, inspect both the edge parent route and the
  destination-specific route on the Mac.
- If `ndnd ping` prints timeouts but exits successfully, treat the test as a
  failure; count the `content from ...` lines.
- Checkpoint logs from Unix runners are written under
  `std/examples/tailscale-4node/logs/` by default.

## 14. Cleanup

Stop the three edges first, then stop the Mac hub:

1. Press Ctrl-C in Agent A and Agent B runners.
2. Stop the Windows ping server and forwarder.
3. Press Ctrl-C in the Mac runner.

The Unix runners stop their local forwarder, ping server, and bridge. Agent A
also stops its isolated Tailscale daemon if that runner started it; a daemon
that was already running is left online. Routes and FaceIds are forwarder
runtime state and disappear when the corresponding forwarder exits.

## 15. Later extensions

Keep this static Mac-centered star until the matrix and recovery check are
repeatable. Possible follow-ups are:

1. Automate the Windows edge after its manual path is verified.
2. Compare against a full-mesh NDN topology over the same Tailscale tailnet.
3. Test NDN-over-UDP where every node has a suitable UDP-capable Tailscale
   path; the current Agent A `tailscale nc` bridge is TCP-only.
4. Add DV routing and compare convergence with this static baseline.
5. Replace `pingserver` with the smallest IOA-oriented application while
   retaining the same forwarders and faces.
