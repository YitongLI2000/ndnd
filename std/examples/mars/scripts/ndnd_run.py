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

# ==============================================================================
#  SCALING CONFIGURATION
# ==============================================================================

# 1. Number of Consumers (1 to 3)
#    - 1: Only con0 is active.
#    - 2: con0 and con1 are active.
#    - 3: con0, con1, and con2 are active.
NUM_ACTIVE_CONSUMERS = 3

# 2. Number of Producers per Consumer (1 to 5)
#    - This determines how many producers each active consumer requests data from.
#    - The topology groups producers in blocks of 5 (pro0-4, pro5-9, pro10-14).
#    - Example: If set to 1, con0 talks to pro0.
#    - Example: If set to 5, con0 talks to pro0, pro1, pro2, pro3, pro4.
NUM_PRODUCERS_PER_CONSUMER = 5

# 3. Network Loss
#    - Packet loss % on links between Consumers and Cores.
CON_TO_CORE_LOSS = 0

# 4. Start Delays
#    - Staggering start times to prevent initial ARP/Routing storms.
CONSUMER_START_DELAYS = {
    'con0': 0,   # Starts immediately
    'con1': 1,   # Starts 1 seconds later
    'con2': 2,  # Starts 2 seconds later
}

# ==============================================================================
#  TOPOLOGY DEFINITION (Fixed Physical Layout)
# ==============================================================================
NODES = [
    # 15 Producers
    'pro0', 'pro1', 'pro2', 'pro3', 'pro4', 'pro5', 'pro6', 'pro7', 'pro8', 'pro9', 
    'pro10', 'pro11', 'pro12', 'pro13', 'pro14',
    # 3 Consumers
    'con0', 'con1', 'con2', 
    # 15 Edges
    'edge0', 'edge1', 'edge2', 'edge3', 'edge4', 'edge5', 'edge6', 'edge7', 'edge8', 'edge9', 
    'edge10', 'edge11', 'edge12', 'edge13', 'edge14', 
    # 5 Cores
    'core0', 'core1', 'core2', 'core3', 'core4'
]

LINKS = [
    # Producers to Edges (Lossless)
    ('pro0', 'edge0', 40, '1ms', 500, 0), ('pro1', 'edge1', 40, '1ms', 500, 0),
    ('pro2', 'edge2', 40, '1ms', 500, 0), ('pro3', 'edge3', 40, '1ms', 500, 0),
    ('pro4', 'edge4', 40, '1ms', 500, 0), ('pro5', 'edge5', 40, '1ms', 500, 0),
    ('pro6', 'edge6', 40, '1ms', 500, 0), ('pro7', 'edge7', 40, '1ms', 500, 0),
    ('pro8', 'edge8', 40, '1ms', 500, 0), ('pro9', 'edge9', 40, '1ms', 500, 0),
    ('pro10', 'edge10', 40, '1ms', 500, 0), ('pro11', 'edge11', 40, '1ms', 500, 0),
    ('pro12', 'edge12', 40, '1ms', 500, 0), ('pro13', 'edge13', 40, '1ms', 500, 0),
    ('pro14', 'edge14', 40, '1ms', 500, 0), 
    
    # Edges to Core0 (Edges 0-4)
    ('edge0', 'core0', 40, '1ms', 500, 0), ('edge1', 'core0', 40, '1ms', 500, 0),
    ('edge2', 'core0', 40, '1ms', 500, 0), ('edge3', 'core0', 40, '1ms', 500, 0),
    ('edge4', 'core0', 40, '1ms', 500, 0),
    # Edges to Core1 (Edges 5-9)
    ('edge5', 'core1', 40, '1ms', 500, 0), ('edge6', 'core1', 40, '1ms', 500, 0),
    ('edge7', 'core1', 40, '1ms', 500, 0), ('edge8', 'core1', 40, '1ms', 500, 0),
    ('edge9', 'core1', 40, '1ms', 500, 0),
    # Edges to Core2 (Edges 10-14)
    ('edge10', 'core2', 40, '1ms', 500, 0), ('edge11', 'core2', 40, '1ms', 500, 0),
    ('edge12', 'core2', 40, '1ms', 500, 0), ('edge13', 'core2', 40, '1ms', 500, 0),
    ('edge14', 'core2', 40, '1ms', 500, 0),

    # Consumer to Cores (With Configurable Loss)
    # con0 -> Mesh
    ('con0', 'core0', 40, '1ms', 500, CON_TO_CORE_LOSS), ('con0', 'core1', 40, '1ms', 500, CON_TO_CORE_LOSS),
    ('con0', 'core2', 40, '1ms', 500, CON_TO_CORE_LOSS), ('con0', 'core3', 40, '1ms', 500, CON_TO_CORE_LOSS),
    ('con0', 'core4', 40, '1ms', 500, CON_TO_CORE_LOSS),
    # con1 -> Mesh
    ('con1', 'core0', 40, '1ms', 500, CON_TO_CORE_LOSS), ('con1', 'core1', 40, '1ms', 500, CON_TO_CORE_LOSS),
    ('con1', 'core2', 40, '1ms', 500, CON_TO_CORE_LOSS), ('con1', 'core3', 40, '1ms', 500, CON_TO_CORE_LOSS),
    ('con1', 'core4', 40, '1ms', 500, CON_TO_CORE_LOSS),
    # con2 -> Mesh
    ('con2', 'core0', 40, '1ms', 500, CON_TO_CORE_LOSS), ('con2', 'core1', 40, '1ms', 500, CON_TO_CORE_LOSS),
    ('con2', 'core2', 40, '1ms', 500, CON_TO_CORE_LOSS), ('con2', 'core3', 40, '1ms', 500, CON_TO_CORE_LOSS),
    ('con2', 'core4', 40, '1ms', 500, CON_TO_CORE_LOSS),

    # Core Mesh (Lossless)
    ('core0', 'core1', 40, '1ms', 500, 0), ('core0', 'core2', 40, '1ms', 500, 0),
    ('core0', 'core3', 40, '1ms', 500, 0), ('core0', 'core4', 40, '1ms', 500, 0),
    ('core1', 'core2', 40, '1ms', 500, 0), ('core1', 'core3', 40, '1ms', 500, 0),
    ('core1', 'core4', 40, '1ms', 500, 0), ('core2', 'core3', 40, '1ms', 500, 0),
    ('core2', 'core4', 40, '1ms', 500, 0), ('core3', 'core4', 40, '1ms', 500, 0)
]

# ==============================================================================
#  HELPER CLASSES
# ==============================================================================

def check_scaling_limits():
    """Ensures the scaling configuration fits within the physical topology."""
    MAX_TOPO_CONSUMERS = 3
    MAX_TOPO_PRODUCERS_PER_BLOCK = 5 

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
        start_idx = i * 5
        end_idx = start_idx + NUM_PRODUCERS_PER_CONSUMER - 1
        print(f"   - con{i} will request from: pro{start_idx} ... pro{end_idx} (Count: {NUM_PRODUCERS_PER_CONSUMER})")
    print("")

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

def start_ndnd_daemons(net):
    print(f"\nStarting ndnd daemons on {len(NODES)} nodes...")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    configs_dir = os.path.join(script_dir, '..', 'configs')
    logs_dir = os.path.join(script_dir, '..', 'logs')
    os.makedirs(logs_dir, exist_ok=True)
    
    for node_name in NODES:
        node = net.get(node_name)
        config_path = os.path.join(configs_dir, f'{node_name}.yml')
        log_path = os.path.join(logs_dir, f'{node_name}.log')
        node.cmd('mkdir -p /run/nfd')
        node.cmd('rm -f /run/nfd/nfd.sock') 
        node.cmd(f'env NDN_LOG=trace ndnd daemon {config_path} > {log_path} 2>&1 &')
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

def configure_dynamic_routing(net):
    print(f"\n=== Configuring Dynamic Routes for {NUM_ACTIVE_CONSUMERS} Consumers ===")
    consumers = [f'con{i}' for i in range(NUM_ACTIVE_CONSUMERS)]
    
    for con_name in consumers:
        con_node = net.get(con_name)
        # 1. Get UDP Faces
        face_output = con_node.cmd('ndnd fw face-list')
        udp_faces = []
        for line in face_output.split('\n'):
            if 'remote=udp4://' in line:
                match = re.search(r'faceid=(\d+)', line)
                if match: udp_faces.append(int(match.group(1)))
        
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
            for face_id in udp_faces:
                if face_id not in data['faces']:
                    con_node.cmd(f"ndnd fw route-add prefix={prefix} face={face_id} cost={target_cost}")
                    added_count += 1
        
        print(f"  -> {con_name}: Added {added_count} multipath routes.")
        con_node.cmd('ndnd fw strategy-set prefix=/ strategy=/localhost/nfd/strategy/multipath/v=1')

    print(f"\n=== Configuring Strategy for Producers ===")
    producers = [n for n in NODES if n.startswith('pro')]
    for pro_name in producers:
        pro_node = net.get(pro_name)
        pro_node.cmd('ndnd fw strategy-set prefix=/ strategy=/localhost/nfd/strategy/multipath/v=1')
        print(f"  -> {pro_name}: Strategy set to multipath.")

def run_applications(net):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    apps_dir = os.path.join(script_dir, '..', 'apps')
    logs_dir = os.path.join(script_dir, '..', 'logs')
    
    print("\nWaiting 45 seconds for routing convergence...")
    time.sleep(45)
    
    # 1. Identify Needed Producers based on Scaling Config
    producers_needed = []
    for i in range(NUM_ACTIVE_CONSUMERS):
        start_idx = i * 5
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
        p_node.cmd(f'{p_bin} {app_name} {prefix} > {p_log} 2>&1 &')
        time.sleep(0.1)

    print("Waiting 5s for producers to register...")
    time.sleep(5)

    configure_dynamic_routing(net)

    print(f"\n=== Starting {NUM_ACTIVE_CONSUMERS} Consumer Applications ===")
    c_bin = os.path.join(apps_dir, 'consumer', 'consumer')
    start_time_ref = time.time()

    for i in range(NUM_ACTIVE_CONSUMERS):
        c_name = f'con{i}'
        c_node = net.get(c_name)
        c_log = os.path.join(logs_dir, f'{c_name}_app.log')
        
        # Calculate Range Argument for Go App
        # Go App expects "start-end" (inclusive)
        start_idx = i * 5
        end_idx = (i * 5) + NUM_PRODUCERS_PER_CONSUMER - 1
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
        c_node.cmd(f'{c_bin} {c_name}app {range_arg} ModePD > {c_log} 2>&1 &')
    
    print("All applications running.")

if __name__ == '__main__':
    check_scaling_limits()
    setLogLevel('info')
    script_dir = os.path.dirname(os.path.abspath(__file__))
    original_dir = os.getcwd()
    
    os.system('mn -c > /dev/null 2>&1')
    
    net, allocator = create_topology_and_net()
    
    try:
        log_ip_configuration(net)
        warmup_network(net, allocator)
        start_ndnd_daemons(net)
        run_applications(net)
        CLI(net)
    finally:
        stop_ndnd_daemons(net)
        net.stop()
        os.chdir(original_dir)
        os.system('mn -c > /dev/null 2>&1')

