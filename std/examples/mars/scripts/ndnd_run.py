#!/usr/bin/env python3

from mininet.net import Mininet
from mininet.node import Node, Host
from mininet.link import TCLink
from mininet.cli import CLI
from mininet.log import setLogLevel, info
import time
import os
import re
import collections
import sys
from datetime import datetime

# ==============================================================================
#  SCALING CONFIGURATION
# ==============================================================================

# 1. Number of Consumers (1 to 5)
#    - 1: Only con0 is active.
#    - 2: con0 and con1 are active.
#    - 5: con0, con1, con2, con3, and con4 are active.
NUM_ACTIVE_CONSUMERS = 5

# 2. Number of Producers per Consumer (1 to 5)
#    - This determines how many producers each active consumer requests data from.
#    - The topology groups producers in blocks of 5
#      (pro0-4, pro5-9, pro10-14, pro15-19, pro20-24).
#    - Example: If set to 1, con0 talks to pro0.
#    - Example: If set to 5, con0 talks to pro0, pro1, pro2, pro3, pro4.
NUM_PRODUCERS_PER_CONSUMER = 5

# 3. Network Loss
#    - Packet loss % on links between Consumers and Cores.
CON_TO_CORE_LOSS = 0

# 3b. Deployment Mode
#    - "normal": deploy Mars multipath on con*/pro*, best-route on core*/edge*.
#    - "minimal": deploy Mars multipath only on pro*, best-route elsewhere.
DEPLOYMENT_MODE = "normal"

# 3c. Failure Mode
#    - "normal": no injected Mars face failure.
#    - "face-disable": on con* Mars forwarders, disable one DT-valid face during
#      the configured failure window after all active prefixes finish PD.
# FAILURE_MODE = "normal"
FAILURE_MODE = "face-disable"
FAILURE_START_SEC = 1.0
FAILURE_END_SEC = 3.0

# 4. Start Delays
#    - Staggering start times to prevent initial ARP/Routing storms.
CONSUMER_START_DELAYS = {
    'con0': 0,   # Starts immediately
    'con1': 1,   # Starts 1 seconds later
    'con2': 2,   # Starts 2 seconds later
    'con3': 1,   # Starts 3 seconds later
    'con4': 2,   # Starts 4 seconds later
}

# 5. Post-Run Behavior
#    - "cli": keep Mininet interactive shell after apps start (default behavior).
#    - "auto_exit_on_flow_summary": exit Mininet automatically after all expected
#      consumer "Flow Summary" lines are observed.
POST_RUN_MODE = "auto_exit_on_flow_summary"
FLOW_SUMMARY_POLL_INTERVAL_SEC = 2
FLOW_SUMMARY_TIMEOUT_SEC = 1200

# ==============================================================================
#  CONFIG GENERATION PARAMETERS
# ==============================================================================
DV_NETWORK_NAME = "/hkustgz"
DV_KEYCHAIN = "insecure"

# One editable parameter block per node type.
DV_TYPE_PARAMS = {
    'con': {'advertise_interval': 1000, 'router_dead_interval': 300000},
    'core': {'advertise_interval': 1000, 'router_dead_interval': 300000},
    'edge': {'advertise_interval': 1000, 'router_dead_interval': 300000},
    'pro': {'advertise_interval': 1000, 'router_dead_interval': 300000},
}

# One editable parameter block per node type.
FW_TYPE_PARAMS = {
    'con': {
        'core_log_level': "DEBUG",
        'udp_enabled_unicast': True,
        'udp_enabled_multicast': False,
        'udp_port_unicast': 6363,
        'tcp_enabled': True,
        'tcp_port_unicast': 6363,
        'websocket_enabled': False,
        'threads': 2,
    },
    'core': {
        'core_log_level': "INFO",
        'udp_enabled_unicast': True,
        'udp_enabled_multicast': False,
        'udp_port_unicast': 6363,
        'tcp_enabled': True,
        'tcp_port_unicast': 6363,
        'websocket_enabled': False,
        'threads': 2,
    },
    'edge': {
        'core_log_level': "INFO",
        'udp_enabled_unicast': True,
        'udp_enabled_multicast': False,
        'udp_port_unicast': 6363,
        'tcp_enabled': True,
        'tcp_port_unicast': 6363,
        'websocket_enabled': False,
        'threads': 2,
    },
    'pro': {
        'core_log_level': "DEBUG",
        'udp_enabled_unicast': True,
        'udp_enabled_multicast': False,
        'udp_port_unicast': 6363,
        'tcp_enabled': True,
        'tcp_port_unicast': 6363,
        'websocket_enabled': False,
        'threads': 2,
    },
}

# ==============================================================================
#  TOPOLOGY DEFINITION (Fixed Physical Layout)
# ==============================================================================
TOTAL_TOPO_CONSUMERS = 5
TOTAL_TOPO_CORES = 5
PRODUCERS_PER_BLOCK = 5
TOTAL_TOPO_PRODUCERS = TOTAL_TOPO_CONSUMERS * PRODUCERS_PER_BLOCK
TOTAL_TOPO_EDGES = TOTAL_TOPO_PRODUCERS


def build_fixed_nodes():
    nodes = []
    nodes.extend(f'pro{i}' for i in range(TOTAL_TOPO_PRODUCERS))
    nodes.extend(f'con{i}' for i in range(TOTAL_TOPO_CONSUMERS))
    nodes.extend(f'edge{i}' for i in range(TOTAL_TOPO_EDGES))
    nodes.extend(f'core{i}' for i in range(TOTAL_TOPO_CORES))
    return nodes


def build_fixed_links():
    links = []

    # Producers to Edges (Lossless)
    for producer_idx in range(TOTAL_TOPO_PRODUCERS):
        links.append((f'pro{producer_idx}', f'edge{producer_idx}', 40, '5ms', 1000, 0))

    # Each block of 5 edges uplinks to its corresponding core.
    for core_idx in range(TOTAL_TOPO_CORES):
        start_edge = core_idx * PRODUCERS_PER_BLOCK
        end_edge = start_edge + PRODUCERS_PER_BLOCK
        for edge_idx in range(start_edge, end_edge):
            links.append((f'edge{edge_idx}', f'core{core_idx}', 40, '5ms', 1000, 0))

    # Consumers connect to every core.
    for consumer_idx in range(TOTAL_TOPO_CONSUMERS):
        for core_idx in range(TOTAL_TOPO_CORES):
            links.append((f'con{consumer_idx}', f'core{core_idx}', 40, '1ms', 1000, CON_TO_CORE_LOSS))

    # Full core mesh (Lossless).
    for src_core in range(TOTAL_TOPO_CORES):
        for dst_core in range(src_core + 1, TOTAL_TOPO_CORES):
            links.append((f'core{src_core}', f'core{dst_core}', 40, '1ms', 1000, 0))

    return links


NODES = build_fixed_nodes()
LINKS = build_fixed_links()

# ==============================================================================
#  HELPER CLASSES
# ==============================================================================

def check_scaling_limits():
    """Ensures the scaling configuration fits within the physical topology."""
    MAX_TOPO_CONSUMERS = TOTAL_TOPO_CONSUMERS
    MAX_TOPO_PRODUCERS_PER_BLOCK = PRODUCERS_PER_BLOCK

    print("\n🔍 Checking Scaling Configuration...")
    
    if NUM_ACTIVE_CONSUMERS < 1 or NUM_ACTIVE_CONSUMERS > MAX_TOPO_CONSUMERS:
        print(f"❌ Error: NUM_ACTIVE_CONSUMERS ({NUM_ACTIVE_CONSUMERS}) must be between 1 and {MAX_TOPO_CONSUMERS}.")
        sys.exit(1)
    
    if NUM_PRODUCERS_PER_CONSUMER < 1 or NUM_PRODUCERS_PER_CONSUMER > MAX_TOPO_PRODUCERS_PER_BLOCK:
        print(f"❌ Error: NUM_PRODUCERS_PER_CONSUMER ({NUM_PRODUCERS_PER_CONSUMER}) must be between 1 and {MAX_TOPO_PRODUCERS_PER_BLOCK}.")
        sys.exit(1)

    print(f"✅ Configuration Valid: {NUM_ACTIVE_CONSUMERS} Consumer(s), {NUM_PRODUCERS_PER_CONSUMER} Producer(s) per Consumer.")
    print("📋 Planned Mapping:")
    for i in range(NUM_ACTIVE_CONSUMERS):
        start_idx = i * PRODUCERS_PER_BLOCK
        end_idx = start_idx + NUM_PRODUCERS_PER_CONSUMER - 1
        print(f"   - con{i} will request from: pro{start_idx} ... pro{end_idx} (Count: {NUM_PRODUCERS_PER_CONSUMER})")
    print("")

    if DEPLOYMENT_MODE not in {"normal", "minimal"}:
        print(f"❌ Error: DEPLOYMENT_MODE ({DEPLOYMENT_MODE}) must be either 'normal' or 'minimal'.")
        sys.exit(1)

    print(f"✅ Deployment Mode: {DEPLOYMENT_MODE}")

    if FAILURE_MODE not in {"normal", "face-disable"}:
        print(f"❌ Error: FAILURE_MODE ({FAILURE_MODE}) must be either 'normal' or 'face-disable'.")
        sys.exit(1)

    if FAILURE_MODE != "normal" and DEPLOYMENT_MODE != "normal":
        print(
            f"❌ Error: FAILURE_MODE ({FAILURE_MODE}) requires DEPLOYMENT_MODE='normal' "
            f"(current: {DEPLOYMENT_MODE})."
        )
        sys.exit(1)

    if FAILURE_START_SEC < 0 or FAILURE_END_SEC <= FAILURE_START_SEC:
        print(
            f"❌ Error: failure window must satisfy 0 <= start < end "
            f"(current: start={FAILURE_START_SEC}, end={FAILURE_END_SEC})."
        )
        sys.exit(1)

    print(
        f"✅ Failure Mode: {FAILURE_MODE} "
        f"(window={FAILURE_START_SEC:.1f}s-{FAILURE_END_SEC:.1f}s)"
    )

class IPAllocator:
    def __init__(self, links):
        self.links = links
        self.link_subnets = {} 
        self._assign_subnets()

    def _assign_subnets(self):
        subnet_counter = 0
        for link in self.links:
            u, v = link[0], link[1]
            # Assign /30 Subnet (172.16.X.Y)
            base_ip = subnet_counter * 4
            octet3 = base_ip // 256
            octet4 = base_ip % 256
            ip_u = f"172.16.{octet3}.{octet4 + 1}"
            ip_v = f"172.16.{octet3}.{octet4 + 2}"
            self.link_subnets[(u, v)] = {'u_ip': ip_u, 'v_ip': ip_v}
            self.link_subnets[(v, u)] = {'u_ip': ip_v, 'v_ip': ip_u}
            subnet_counter += 1

    def get_link_ip(self, node, neighbor):
        if (node, neighbor) in self.link_subnets:
            return self.link_subnets[(node, neighbor)]['u_ip']
        return None

class LinuxRouter(Node):
    """A Node with IP forwarding enabled."""
    def config(self, **params):
        super(LinuxRouter, self).config(**params)
        self.cmd('sysctl -w net.ipv4.ip_forward=1')

def get_node_type(node_name):
    if node_name.startswith('con'):
        return 'con'
    if node_name.startswith('core'):
        return 'core'
    if node_name.startswith('edge'):
        return 'edge'
    if node_name.startswith('pro'):
        return 'pro'
    raise ValueError(f"Unknown node type for node '{node_name}'")

def bool_yaml(value):
    return "true" if value else "false"

def build_neighbors_by_node(allocator):
    neighbors = {node_name: [] for node_name in NODES}
    for src_name, dst_name, *_ in LINKS:
        src_neighbor_ip = allocator.get_link_ip(dst_name, src_name)
        dst_neighbor_ip = allocator.get_link_ip(src_name, dst_name)

        if src_neighbor_ip is None or dst_neighbor_ip is None:
            raise RuntimeError(f"Missing IP allocation for link {src_name}<->{dst_name}")

        neighbors[src_name].append((dst_name, src_neighbor_ip))
        neighbors[dst_name].append((src_name, dst_neighbor_ip))
    return neighbors

def render_node_config(node_name, node_neighbors):
    node_type = get_node_type(node_name)
    dv_params = DV_TYPE_PARAMS[node_type]
    fw_params = FW_TYPE_PARAMS[node_type]

    lines = [
        "dv:",
        f"  network: {DV_NETWORK_NAME}",
        f"  router: {DV_NETWORK_NAME}/{node_name}",
        f"  keychain: \"{DV_KEYCHAIN}\"",
        "  neighbors:",
    ]

    for neighbor_name, neighbor_ip in node_neighbors:
        lines.append(f"    - uri: \"udp://{neighbor_ip}:6363\" # Neighbor: {neighbor_name}")

    lines.extend([
        f"  advertise_interval: {dv_params['advertise_interval']}",
        f"  router_dead_interval: {dv_params['router_dead_interval']}",
        "",
        "fw:",
        "  core:",
    ])

    lines.append(f"    log_level: {fw_params['core_log_level']}")

    lines.extend([
        "  faces:",
        "    udp:",
        f"      enabled_unicast: {bool_yaml(fw_params['udp_enabled_unicast'])}",
        f"      enabled_multicast: {bool_yaml(fw_params['udp_enabled_multicast'])}",
        f"      port_unicast: {fw_params['udp_port_unicast']}",
        "      # default_mtu: 1420",
        "    tcp:",
        f"      enabled: {bool_yaml(fw_params['tcp_enabled'])}",
        f"      port_unicast: {fw_params['tcp_port_unicast']}",
        "    websocket:",
        f"      enabled: {bool_yaml(fw_params['websocket_enabled'])}",
        "  fw:",
        f"    threads: {fw_params['threads']}",
        f"    strategy_node_name: {node_name}",
        "",
    ])

    return "\n".join(lines)

def regenerate_all_node_configs(allocator):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    configs_dir = os.path.join(script_dir, '..', 'configs')
    os.makedirs(configs_dir, exist_ok=True)

    neighbors_by_node = build_neighbors_by_node(allocator)
    for node_name in NODES:
        cfg_path = os.path.join(configs_dir, f"{node_name}.yml")
        cfg_content = render_node_config(node_name, neighbors_by_node[node_name])
        with open(cfg_path, "w", encoding="ascii", newline="\n") as cfg_file:
            cfg_file.write(cfg_content)

    print(f"Regenerated {len(NODES)} node config files in {configs_dir}")


def clear_logs_dir():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    logs_dir = os.path.join(script_dir, '..', 'logs')
    os.makedirs(logs_dir, exist_ok=True)

    removed_count = 0
    for entry in os.scandir(logs_dir):
        if not entry.is_file():
            continue
        os.unlink(entry.path)
        removed_count += 1

    print(f"Cleared {removed_count} existing log file(s) from {logs_dir}")
    return logs_dir


def loss_label(loss_value):
    return f"loss_{str(loss_value).replace('.', 'p')}pct"


def prepare_run_logs_dir():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    logs_root = os.path.join(script_dir, '..', 'logs')
    if DEPLOYMENT_MODE == "minimal":
        logs_root = os.path.join(logs_root, 'minimal')
    if FAILURE_MODE != "normal":
        logs_root = os.path.join(logs_root, 'failure', FAILURE_MODE.replace('-', '_'))
    loss_dir = os.path.join(logs_root, loss_label(CON_TO_CORE_LOSS))
    timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    run_dir = os.path.join(loss_dir, timestamp)
    os.makedirs(run_dir, exist_ok=True)
    print(f"Using run logs directory: {run_dir}")
    return run_dir

# ==============================================================================
#  SETUP & EXECUTION
# ==============================================================================

def create_topology_and_net():
    print("🚀 Starting NDN Topology")
    allocator = IPAllocator(LINKS)
    net = Mininet(topo=None, build=False, link=TCLink)

    print("*** Adding Nodes")
    nodes_obj = {}
    for n in NODES:
        if n.startswith('pro') or n.startswith('con'):
            nodes_obj[n] = net.addHost(n, privateDirs=['/run'], ip=None)
        else:
            nodes_obj[n] = net.addHost(n, cls=LinuxRouter, privateDirs=['/run'], ip=None)

    print(f"*** Adding Links (Loss: {CON_TO_CORE_LOSS}%)")
    for i, (src_name, dst_name, bw, delay, queue, loss_rate) in enumerate(LINKS):
        src = nodes_obj[src_name]
        dst = nodes_obj[dst_name]
        net.addLink(src, dst, bw=bw, delay=delay, max_queue_size=queue, loss=loss_rate,
                    intfName1=f"{src_name}-{dst_name}",
                    intfName2=f"{dst_name}-{src_name}")
        src_ip = allocator.get_link_ip(src_name, dst_name)
        dst_ip = allocator.get_link_ip(dst_name, src_name)
        src.setIP(src_ip, prefixLen=30, intf=f"{src_name}-{dst_name}")
        dst.setIP(dst_ip, prefixLen=30, intf=f"{dst_name}-{src_name}")

    print("*** Building network")
    net.build()
    print("*** Starting network")
    net.start()
    configure_linux_networking(net)
    return net, allocator

def configure_linux_networking(net):
    print("*** Configuring Linux Kernel Networking...")
    for host in net.hosts:
        host.cmd('sysctl -w net.ipv4.conf.all.rp_filter=0')
        host.cmd('sysctl -w net.ipv4.conf.default.rp_filter=0')
        host.cmd('for i in $(ls /sys/class/net/); do sysctl -w net.ipv4.conf.$i.rp_filter=0; done')

def log_ip_configuration(net):
    print("\n📊 Verifying IP Configuration (Sample):")
    for node in net.hosts:
        if node.name in ['con0', 'pro0', 'core0']:
            print(f"  Node: {node.name}")
            for intf_name in sorted(node.intfNames()):
                if intf_name == 'lo': continue
                print(f"    - {intf_name}: {node.IP(intf_name)}")

def warmup_network(net, allocator):
    print("\n🔥 Warming up network (Neighbor Pings)...")
    time.sleep(2) 
    failed_links = 0
    for src_name, dst_name, _, _, _, _ in LINKS:
        src_node = net.get(src_name)
        target_ip = allocator.get_link_ip(dst_name, src_name)
        result = src_node.cmd(f"ping -c 1 -W 0.5 {target_ip}")
        if "1 received" not in result and "1 packets received" not in result:
             print(f"⚠️  Warning: Link {src_name} -> {dst_name} ({target_ip}) failed.")
             failed_links += 1
    if failed_links == 0:
        print("✅ Network warmup complete.")
    else:
        print(f"❌ Warmup finished with {failed_links} failures.")

def start_ndnd_daemons(net, allocator, logs_dir):
    print(f"\nStarting ndnd daemons on {len(NODES)} nodes...")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    configs_dir = os.path.join(script_dir, '..', 'configs')
    failure_start_ms = int(FAILURE_START_SEC * 1000)
    failure_end_ms = int(FAILURE_END_SEC * 1000)

    # Always regenerate/override all configs from the current topology before daemon start.
    regenerate_all_node_configs(allocator)
    
    for node_name in NODES:
        node = net.get(node_name)
        config_path = os.path.join(configs_dir, f'{node_name}.yml')
        log_path = os.path.join(logs_dir, f'{node_name}.log')
        node.cmd('mkdir -p /run/nfd')
        node.cmd('rm -f /run/nfd/nfd.sock') 
        node.cmd(
            f'env MARS_LOG_DIR={logs_dir} '
            f'MARS_DEPLOYMENT_MODE={DEPLOYMENT_MODE} '
            f'MARS_FAILURE_MODE={FAILURE_MODE} '
            f'MARS_FAILURE_START_MS={failure_start_ms} '
            f'MARS_FAILURE_END_MS={failure_end_ms} '
            f'MARS_FAILURE_PREFIX_COUNT={NUM_PRODUCERS_PER_CONSUMER} '
            f'NDN_LOG=trace ndnd daemon {config_path} > {log_path} 2>&1 &'
        )
        time.sleep(0.05) 
    print("All ndnd daemons started.")

def stop_ndnd_daemons(net):
    print("\nStopping applications and ndnd daemons...")
    for node_name in NODES:
        node = net.get(node_name)
        node.cmd('killall producer')
        node.cmd('killall consumer')
        node.cmd('killall ndnd')
        node.cmd('rm -f /run/nfd/nfd.sock')
    print("Cleanup complete.")

def get_consumer_core_face_ids(con_node, con_name, allocator):
    core_neighbor_ips = {
        allocator.get_link_ip(f'core{i}', con_name)
        for i in range(TOTAL_TOPO_CORES)
    }
    core_neighbor_ips.discard(None)

    face_output = con_node.cmd('ndnd fw face-list')
    core_faces = []
    for line in face_output.split('\n'):
        if 'remote=udp4://' not in line:
            continue
        face_match = re.search(r'faceid=(\d+)', line)
        remote_match = re.search(r'remote=udp4://([0-9.]+):\d+', line)
        if not face_match or not remote_match:
            continue
        remote_ip = remote_match.group(1)
        if remote_ip in core_neighbor_ips:
            core_faces.append(int(face_match.group(1)))

    core_faces = sorted(set(core_faces))
    if len(core_faces) != TOTAL_TOPO_CORES:
        print(
            f"⚠️  {con_name}: discovered {len(core_faces)} direct core face(s) "
            f"(expected {TOTAL_TOPO_CORES}). faceids={core_faces}"
        )
    return core_faces


def install_strategies_for_mode(net):
    if DEPLOYMENT_MODE == "normal":
        strategy_groups = collections.OrderedDict([
            ("Consumers", ([f'con{i}' for i in range(TOTAL_TOPO_CONSUMERS)], "/localhost/nfd/strategy/multipath/v=1")),
            ("Cores", ([f'core{i}' for i in range(TOTAL_TOPO_CORES)], "/localhost/nfd/strategy/best-route/v=1")),
            ("Edges", ([f'edge{i}' for i in range(TOTAL_TOPO_EDGES)], "/localhost/nfd/strategy/best-route/v=1")),
            ("Producers", ([f'pro{i}' for i in range(TOTAL_TOPO_PRODUCERS)], "/localhost/nfd/strategy/multipath/v=1")),
        ])
    elif DEPLOYMENT_MODE == "minimal":
        strategy_groups = collections.OrderedDict([
            ("Consumers", ([f'con{i}' for i in range(TOTAL_TOPO_CONSUMERS)], "/localhost/nfd/strategy/best-route/v=1")),
            ("Cores", ([f'core{i}' for i in range(TOTAL_TOPO_CORES)], "/localhost/nfd/strategy/best-route/v=1")),
            ("Edges", ([f'edge{i}' for i in range(TOTAL_TOPO_EDGES)], "/localhost/nfd/strategy/best-route/v=1")),
            ("Producers", ([f'pro{i}' for i in range(TOTAL_TOPO_PRODUCERS)], "/localhost/nfd/strategy/multipath/v=1")),
        ])
    else:
        raise RuntimeError(f"Unsupported DEPLOYMENT_MODE: {DEPLOYMENT_MODE}")

    print(f"\n=== Configuring Strategy by Layer ({DEPLOYMENT_MODE}) ===")
    for group_name, (node_names, strategy_name) in strategy_groups.items():
        for node_name in node_names:
            net.get(node_name).cmd(f'ndnd fw strategy-set prefix=/ strategy={strategy_name}')
        print(f"  -> {group_name}: Strategy set to {strategy_name} on {len(node_names)} node(s).")


def configure_dynamic_routing(net, allocator):
    install_strategies_for_mode(net)

    if DEPLOYMENT_MODE == "minimal":
        print("\n=== Skipping consumer-side route injection (minimal deployment) ===")
        return

    print(f"\n=== Configuring Dynamic Routes for {NUM_ACTIVE_CONSUMERS} Consumers ===")
    consumers = [f'con{i}' for i in range(NUM_ACTIVE_CONSUMERS)]
    
    for con_name in consumers:
        con_node = net.get(con_name)
        # 1. Get the 5 direct consumer->core UDP faces from the physical topology.
        core_faces = get_consumer_core_face_ids(con_node, con_name, allocator)
        
        # 2. Get Discovered Routes (via NLSR/DV)
        route_output = con_node.cmd('ndnd fw route-list')
        prefix_data = {}
        for line in route_output.split('\n'):
            if 'prefix=/pro' in line:
                p_match = re.search(r'prefix=([^ ]+)', line)
                nh_match = re.search(r'nexthop=(\d+)', line)
                c_match = re.search(r'cost=(\d+)', line)
                if p_match and nh_match and c_match:
                    prefix = p_match.group(1)
                    nexthop = int(nh_match.group(1))
                    cost = int(c_match.group(1))
                    if prefix not in prefix_data:
                        prefix_data[prefix] = {'faces': set(), 'max_cost': 0}
                    prefix_data[prefix]['faces'].add(nexthop)
                    if cost > prefix_data[prefix]['max_cost']:
                        prefix_data[prefix]['max_cost'] = cost

        # 3. Inject Multipath Routes
        added_count = 0
        for prefix, data in prefix_data.items():
            target_cost = data['max_cost']
            for face_id in core_faces:
                if face_id not in data['faces']:
                    con_node.cmd(f"ndnd fw route-add prefix={prefix} face={face_id} cost={target_cost}")
                    added_count += 1
        
        print(f"  -> {con_name}: Added {added_count} consumer-core backup routes across {len(core_faces)} core face(s).")

def verify_dynamic_routing_shape_or_die(net):
    if DEPLOYMENT_MODE != "normal":
        print(f"Skipping dynamic routing verification in {DEPLOYMENT_MODE} deployment.")
        return

    if NUM_ACTIVE_CONSUMERS <= 0:
        return

    con_name = "con0"
    con_node = net.get(con_name)
    route_output = con_node.cmd('ndnd fw route-list')

    prefix_costs = {}
    for line in route_output.split('\n'):
        if 'prefix=/pro' not in line:
            continue
        p_match = re.search(r'prefix=([^ ]+)', line)
        c_match = re.search(r'cost=(\d+)', line)
        if p_match and c_match:
            prefix = p_match.group(1)
            cost = int(c_match.group(1))
            if prefix not in prefix_costs:
                prefix_costs[prefix] = []
            prefix_costs[prefix].append(cost)

    if not prefix_costs:
        print(f"❌ Dynamic routing verification failed on {con_name}: no /pro* routes found.")
        sys.exit(1)

    for prefix, costs in sorted(prefix_costs.items()):
        min_cost = min(costs)
        min_count = sum(1 for c in costs if c == min_cost)
        higher_costs = [c for c in costs if c > min_cost]

        if min_count != 1:
            print(
                f"❌ Dynamic routing verification failed on {con_name}: "
                f"{prefix} has {min_count} least-cost nexthops (expected 1). "
                f"Costs={costs}"
            )
            sys.exit(1)

        if not higher_costs:
            print(
                f"❌ Dynamic routing verification failed on {con_name}: "
                f"{prefix} has no higher-cost backup nexthops. Costs={costs}"
            )
            sys.exit(1)

        unique_higher = sorted(set(higher_costs))
        if len(unique_higher) != 1:
            print(
                f"❌ Dynamic routing verification failed on {con_name}: "
                f"{prefix} higher costs are not equal. Costs={costs}"
            )
            sys.exit(1)

    print(f"✅ Dynamic routing verification passed on {con_name} (all /pro* prefixes).")

def run_applications(net, allocator, logs_dir):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    apps_dir = os.path.join(script_dir, '..', 'apps')
    
    print("\nWaiting 45 seconds for routing convergence...")
    time.sleep(45)
    
    # 1. Identify Needed Producers based on Scaling Config
    producers_needed = []
    for i in range(NUM_ACTIVE_CONSUMERS):
        start_idx = i * PRODUCERS_PER_BLOCK
        # range is exclusive in Python, so we go up to start + N
        end_idx = start_idx + NUM_PRODUCERS_PER_CONSUMER
        producers_needed.extend(range(start_idx, end_idx))
    
    producers_needed = sorted(list(set(producers_needed)))
    
    print(f"=== Starting {len(producers_needed)} Producer Applications ===")
    p_bin = os.path.join(apps_dir, 'producer', 'producer')
    
    for i in producers_needed:
        node_name = f'pro{i}'
        p_node = net.get(node_name)
        p_log = os.path.join(logs_dir, f'{node_name}_app.log')
        app_name = f"{node_name}app"
        prefix = f"/pro{i}"
        p_node.cmd(f'env MARS_LOG_DIR={logs_dir} MARS_DEPLOYMENT_MODE={DEPLOYMENT_MODE} {p_bin} {app_name} {prefix} > {p_log} 2>&1 &')
        time.sleep(0.1)

    print("Waiting 5s for producers to register...")
    time.sleep(5)

    configure_dynamic_routing(net, allocator)
    verify_dynamic_routing_shape_or_die(net)

    print(f"\n=== Starting {NUM_ACTIVE_CONSUMERS} Consumer Applications ===")
    c_bin = os.path.join(apps_dir, 'consumer', 'consumer')
    start_time_ref = time.time()

    for i in range(NUM_ACTIVE_CONSUMERS):
        c_name = f'con{i}'
        c_node = net.get(c_name)
        c_log = os.path.join(logs_dir, f'{c_name}_app.log')
        
        # Calculate Range Argument for Go App
        # Go App expects "start-end" (inclusive)
        start_idx = i * PRODUCERS_PER_BLOCK
        end_idx = (i * PRODUCERS_PER_BLOCK) + NUM_PRODUCERS_PER_CONSUMER - 1
        range_arg = f"{start_idx}-{end_idx}"
        
        delay = CONSUMER_START_DELAYS.get(c_name, 0)
        elapsed = time.time() - start_time_ref
        time_to_wait = delay - elapsed
        if time_to_wait > 0:
            print(f"Waiting {time_to_wait:.2f}s before starting {c_name}...")
            time.sleep(time_to_wait)
            
        print(f"Starting {c_name} -> Producers [{range_arg}]")
        # Arguments: <node_name> <producer_range>
        # TODO: debug pd mode
        # c_node.cmd(f'{c_bin} {c_name}app {range_arg} ModeDT > {c_log} 2>&1 &')
        c_node.cmd(f'env MARS_LOG_DIR={logs_dir} MARS_DEPLOYMENT_MODE={DEPLOYMENT_MODE} {c_bin} {c_name}app {range_arg} ModePD > {c_log} 2>&1 &')
    
    print("All applications running.")

def _count_flow_summary_lines(log_path):
    if not os.path.exists(log_path):
        return 0

    count = 0
    with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            if "Flow Summary" in line:
                count += 1
    return count

def wait_for_all_flow_summaries(logs_dir):
    active_consumers = [f'con{i}' for i in range(NUM_ACTIVE_CONSUMERS)]
    expected_per_consumer = NUM_PRODUCERS_PER_CONSUMER
    expected_total = expected_per_consumer * len(active_consumers)

    print(
        f"\nWaiting for Flow Summary completion "
        f"(expected total={expected_total}, per-consumer={expected_per_consumer})..."
    )

    start = time.time()
    last_reported_total = -1
    while time.time() - start <= FLOW_SUMMARY_TIMEOUT_SEC:
        per_consumer_counts = {}
        total_count = 0
        done = True

        for con_name in active_consumers:
            log_path = os.path.join(logs_dir, f"{con_name}_app.log")
            flow_summary_count = _count_flow_summary_lines(log_path)
            per_consumer_counts[con_name] = flow_summary_count
            total_count += flow_summary_count
            if flow_summary_count < expected_per_consumer:
                done = False

        if done:
            print(f"✅ Flow Summary completion detected: {per_consumer_counts}")
            return True

        if total_count != last_reported_total:
            print(f"⏳ Flow Summary progress: {per_consumer_counts} (total={total_count}/{expected_total})")
            last_reported_total = total_count

        time.sleep(FLOW_SUMMARY_POLL_INTERVAL_SEC)

    print(
        f"❌ Timeout waiting for Flow Summary completion after {FLOW_SUMMARY_TIMEOUT_SEC}s. "
        f"Mode={POST_RUN_MODE}"
    )
    return False

if __name__ == '__main__':
    check_scaling_limits()
    setLogLevel('info')
    script_dir = os.path.dirname(os.path.abspath(__file__))
    original_dir = os.getcwd()
    
    os.system('mn -c > /dev/null 2>&1')
    
    net, allocator = create_topology_and_net()
    logs_dir = prepare_run_logs_dir()
    
    try:
        log_ip_configuration(net)
        warmup_network(net, allocator)
        start_ndnd_daemons(net, allocator, logs_dir)
        run_applications(net, allocator, logs_dir)
        if POST_RUN_MODE == "auto_exit_on_flow_summary":
            completed = wait_for_all_flow_summaries(logs_dir)
            if not completed:
                print("Falling back to interactive CLI due to incomplete Flow Summary.")
                CLI(net)
        elif POST_RUN_MODE == "cli":
            CLI(net)
        else:
            print(f"Unknown POST_RUN_MODE '{POST_RUN_MODE}', falling back to CLI.")
            CLI(net)
    finally:
        stop_ndnd_daemons(net)
        net.stop()
        os.chdir(original_dir)
        os.system('mn -c > /dev/null 2>&1')
