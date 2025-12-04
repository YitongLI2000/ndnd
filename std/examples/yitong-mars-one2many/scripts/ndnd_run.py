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

# --- Configuration ---
# Loss rate in percentage (0 to 100). 
# e.g., 0.001 means 0.001% packet loss. 1 means 1% packet loss.
CON_TO_CORE_LOSS = 1

# --- Topology Definition ---
NODES = [
    'pro0', 'pro1', 'pro2', 'pro3', 'pro4',
    'con0',
    'edge0', 'edge1', 'edge2', 'edge3', 'edge4',
    'core0', 'core1', 'core2', 'core3', 'core4'
]

# Format: (Source, Destination, Bandwidth, Delay, QueueSize, LossRate)
LINKS = [
    # Producers to Edges (Lossless)
    ('pro0', 'edge0', 100, '1ms', 500, 0),
    ('pro1', 'edge1', 100, '1ms', 500, 0),
    ('pro2', 'edge2', 100, '1ms', 500, 0),
    ('pro3', 'edge3', 100, '1ms', 500, 0),
    ('pro4', 'edge4', 100, '1ms', 500, 0),
    
    # Edges to Core0 (Lossless)
    ('edge0', 'core0', 100, '1ms', 500, 0),
    ('edge1', 'core0', 100, '1ms', 500, 0),
    ('edge2', 'core0', 100, '1ms', 500, 0),
    ('edge3', 'core0', 100, '1ms', 500, 0),
    ('edge4', 'core0', 100, '1ms', 500, 0),
    
    # Consumer to Cores (With Configurable Loss)
    ('con0', 'core0', 100, '1ms', 500, CON_TO_CORE_LOSS),
    ('con0', 'core1', 100, '1ms', 500, CON_TO_CORE_LOSS),
    ('con0', 'core2', 100, '1ms', 500, CON_TO_CORE_LOSS),
    ('con0', 'core3', 100, '1ms', 500, CON_TO_CORE_LOSS),
    ('con0', 'core4', 100, '1ms', 500, CON_TO_CORE_LOSS),
    
    # Core Mesh (Lossless)
    ('core0', 'core1', 100, '1ms', 500, 0),
    ('core0', 'core2', 100, '1ms', 500, 0),
    ('core0', 'core3', 100, '1ms', 500, 0),
    ('core0', 'core4', 100, '1ms', 500, 0),
    ('core1', 'core2', 100, '1ms', 500, 0),
    ('core1', 'core3', 100, '1ms', 500, 0),
    ('core1', 'core4', 100, '1ms', 500, 0),
    ('core2', 'core3', 100, '1ms', 500, 0),
    ('core2', 'core4', 100, '1ms', 500, 0),
    ('core3', 'core4', 100, '1ms', 500, 0)
]

# --- 1. Helper Class for IP Allocation ---
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

# --- 2. Special Node Class ---
class LinuxRouter(Node):
    """A Node with IP forwarding enabled."""
    def config(self, **params):
        super(LinuxRouter, self).config(**params)
        self.cmd('sysctl -w net.ipv4.ip_forward=1')

def create_topology_and_net():
    print("🚀 Starting NDN Topology")
    allocator = IPAllocator(LINKS)
    
    net = Mininet(topo=None, build=False, link=TCLink)

    print("*** Adding Nodes")
    nodes_obj = {}
    for n in NODES:
        if n.startswith('pro') or n == 'con0':
            nodes_obj[n] = net.addHost(n, privateDirs=['/run'], ip=None)
        else:
            nodes_obj[n] = net.addHost(n, cls=LinuxRouter, privateDirs=['/run'], ip=None)

    print(f"*** Adding Links & Configuring Interfaces (Con-Core Loss: {CON_TO_CORE_LOSS}%)")
    # Unpacking the 6th element 'loss_rate'
    for i, (src_name, dst_name, bw, delay, queue, loss_rate) in enumerate(LINKS):
        src = nodes_obj[src_name]
        dst = nodes_obj[dst_name]
        
        # Pass the loss parameter to Mininet
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
    print("*** Configuring Linux Kernel Networking (Disabling rp_filter)...")
    for host in net.hosts:
        host.cmd('sysctl -w net.ipv4.conf.all.rp_filter=0')
        host.cmd('sysctl -w net.ipv4.conf.default.rp_filter=0')
        host.cmd('for i in $(ls /sys/class/net/); do sysctl -w net.ipv4.conf.$i.rp_filter=0; done')

def log_ip_configuration(net):
    print("\n📊 Verifying IP Configuration:")
    for node in net.hosts:
        print(f"  Node: {node.name}")
        intf_names = sorted(node.intfNames())
        for intf_name in intf_names:
            if intf_name == 'lo': continue
            ip = node.IP(intf_name)
            if ip:
                print(f"    - {intf_name}: {ip}")
            else:
                print(f"    - {intf_name}: [Unassigned]")

def warmup_network(net, allocator):
    print("\n🔥 Warming up network (Checking Neighbor Connectivity)...")
    time.sleep(2) 

    failed_links = 0
    # Adjusted to unpack 6 items
    for src_name, dst_name, _, _, _, _ in LINKS:
        src_node = net.get(src_name)
        target_ip = allocator.get_link_ip(dst_name, src_name)
        
        result = src_node.cmd(f"ping -c 1 -W 1 {target_ip}")
        
        if "1 received" not in result and "1 packets received" not in result:
             print(f"⚠️  Warning: Link {src_name} -> {dst_name} ({target_ip}) failed.")
             failed_links += 1
    
    if failed_links == 0:
        print("✅ Network warmup complete. All neighbors reachable.")
    else:
        print(f"❌ Warmup finished with {failed_links} failures.")

def start_ndnd_daemons(net):
    # Step 1: Start all ndnd daemon for all nodes
    print(f"\nStarting ndnd daemons on all {len(NODES)} nodes...")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    configs_dir = os.path.join(script_dir, '..', 'configs')
    configs_dir = os.path.abspath(configs_dir)
    logs_dir = os.path.join(script_dir, '..', 'logs')
    os.makedirs(logs_dir, exist_ok=True)
    
    for node_name in NODES:
        node = net.get(node_name)
        config_path = os.path.join(configs_dir, f'{node_name}.yml')
        log_path = os.path.join(logs_dir, f'{node_name}.log')
        
        node.cmd('mkdir -p /run/nfd')
        node.cmd('rm -f /run/nfd/nfd.sock') 
        
        cmd = f'env NDN_LOG=trace ndnd daemon {config_path} > {log_path} 2>&1 &'
        node.cmd(cmd)
        time.sleep(0.1) 
    
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

# --- Step 3 Logic: Dynamic Route Configuration ---
def configure_con0_routing(net):
    print("\n=== Step 3: Configuring Dynamic Routes for con0 ===")
    con0 = net.get('con0')
    
    # 3.1 Read Face List
    print("Reading face-list...")
    face_output = con0.cmd('ndnd fw face-list')
    udp_faces = []
    
    # Parse face-list for UDP faces
    # Example: faceid=4 remote=udp4://172.16.0.42:6363 ...
    for line in face_output.split('\n'):
        if 'remote=udp4://' in line:
            # Extract faceid
            match = re.search(r'faceid=(\d+)', line)
            if match:
                fid = int(match.group(1))
                udp_faces.append(fid)
    
    print(f"Found UDP Face IDs: {udp_faces}")

    # 3.2 Read Route List
    print("Reading route-list...")
    route_output = con0.cmd('ndnd fw route-list')
    
    # Data structure: prefix -> {'faces': set(), 'max_cost': int}
    prefix_data = {}
    
    # Parse route-list
    # Example: prefix=/pro4app nexthop=4 origin=nlsr cost=3 ...
    for line in route_output.split('\n'):
        if 'prefix=/pro' in line:
            # Extract Prefix
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

    # 3.3 Inject Missing Routes
    print("Injecting missing routes...")
    added_count = 0
    
    for prefix, data in prefix_data.items():
        target_cost = data['max_cost'] # Use the largest existing cost
        existing_faces = data['faces']
        
        for face_id in udp_faces:
            if face_id not in existing_faces:
                # Add route
                print(f"  + Adding route: prefix={prefix} face={face_id} cost={target_cost}")
                con0.cmd(f"ndnd fw route-add prefix={prefix} face={face_id} cost={target_cost}")
                added_count += 1
            else:
                # Route exists, skip
                pass

    if added_count == 0:
        print("  No new routes needed (mesh is full).")
    else:
        print(f"  Successfully added {added_count} routes.")

    # 3.4 Verify with FIB List
    print("\n=== Verifying FIB (fib-list) ===")
    fib_output = con0.cmd('ndnd fw fib-list')
    # Just print the lines relevant to /pro to keep log clean
    for line in fib_output.split('\n'):
        if '/pro' in line:
            print(line)

def run_applications(net):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    apps_dir = os.path.join(script_dir, '..', 'apps')
    logs_dir = os.path.join(script_dir, '..', 'logs')
    
    # Step 1 (Cont.): Wait for convergence
    print("\nWaiting 60 seconds for routing convergence (NLSR/DV)...")
    time.sleep(60)
    
    # Step 2: Start Producers
    print("=== Starting Producer Applications ===")
    p_bin = os.path.join(apps_dir, 'producer', 'producer')
    for i in range(5):
        node_name = f'pro{i}'
        p_node = net.get(node_name)
        p_log = os.path.join(logs_dir, f'{node_name}_app.log')
        app_name = f"{node_name}app"
        prefix = f"/pro{i}"
        print(f"Starting Producer on {node_name} ({prefix})")
        p_node.cmd(f'{p_bin} {app_name} {prefix} > {p_log} 2>&1 &')
        time.sleep(0.2)

    # Wait for producers to register prefixes locally so they propagate
    print("Waiting 10s for producers to register prefixes...")
    time.sleep(2)

    # Step 3: Dynamic Route Configuration
    configure_con0_routing(net)

    # Step 3.5: Set Forwarding Strategy
    print("\n=== Setting Multipath Strategy for con0 ===")
    net.get('con0').cmd('ndnd fw strategy-set prefix=/ strategy=/localhost/nfd/strategy/multipath/v=1')

    # Step 4: Start Consumer
    time.sleep(2)
    c0 = net.get('con0')
    c0_bin = os.path.join(apps_dir, 'consumer', 'consumer')
    c0_log = os.path.join(logs_dir, 'con0_app.log')
    print(f"\n=== Starting Consumer on con0 ===")
    c0.cmd(f'{c0_bin} con0app 0-4 > {c0_log} 2>&1 &')
    
    print("Applications running.")

if __name__ == '__main__':
    setLogLevel('info')
    script_dir = os.path.dirname(os.path.abspath(__file__))
    original_dir = os.getcwd()
    
    # Clean up previous runs
    os.system('mn -c > /dev/null 2>&1')
    
    net, allocator = create_topology_and_net()
    
    try:
        # Log the IPs before warming up
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



# #!/usr/bin/env python3

# from mininet.net import Mininet
# from mininet.node import Node, Host
# from mininet.link import TCLink
# from mininet.cli import CLI
# from mininet.log import setLogLevel, info
# import time
# import os
# import re
# import collections

# # --- Topology Definition ---
# NODES = [
#     'pro0', 'pro1', 'pro2', 'pro3', 'pro4',
#     'con0',
#     'edge0', 'edge1', 'edge2', 'edge3', 'edge4',
#     'core0', 'core1', 'core2', 'core3', 'core4'
# ]

# LINKS = [
#     ('pro0', 'edge0', 100, '1ms', 500),
#     ('pro1', 'edge1', 100, '1ms', 500),
#     ('pro2', 'edge2', 100, '1ms', 500),
#     ('pro3', 'edge3', 100, '1ms', 500),
#     ('pro4', 'edge4', 100, '1ms', 500),
#     ('edge0', 'core0', 100, '1ms', 500),
#     ('edge1', 'core0', 100, '1ms', 500),
#     ('edge2', 'core0', 100, '1ms', 500),
#     ('edge3', 'core0', 100, '1ms', 500),
#     ('edge4', 'core0', 100, '1ms', 500),
#     ('con0', 'core0', 100, '1ms', 500),
#     ('con0', 'core1', 100, '1ms', 500),
#     ('con0', 'core2', 100, '1ms', 500),
#     ('con0', 'core3', 100, '1ms', 500),
#     ('con0', 'core4', 100, '1ms', 500),
#     ('core0', 'core1', 100, '1ms', 500),
#     ('core0', 'core2', 100, '1ms', 500),
#     ('core0', 'core3', 100, '1ms', 500),
#     ('core0', 'core4', 100, '1ms', 500),
#     ('core1', 'core2', 100, '1ms', 500),
#     ('core1', 'core3', 100, '1ms', 500),
#     ('core1', 'core4', 100, '1ms', 500),
#     ('core2', 'core3', 100, '1ms', 500),
#     ('core2', 'core4', 100, '1ms', 500),
#     ('core3', 'core4', 100, '1ms', 500)
# ]

# # --- 1. Helper Class for IP Allocation ---
# class IPAllocator:
#     def __init__(self, links):
#         self.links = links
#         self.link_subnets = {} 
#         self._assign_subnets()

#     def _assign_subnets(self):
#         subnet_counter = 0
#         for link in self.links:
#             u, v = link[0], link[1]
            
#             # Assign /30 Subnet (172.16.X.Y)
#             base_ip = subnet_counter * 4
#             octet3 = base_ip // 256
#             octet4 = base_ip % 256
            
#             ip_u = f"172.16.{octet3}.{octet4 + 1}"
#             ip_v = f"172.16.{octet3}.{octet4 + 2}"
            
#             self.link_subnets[(u, v)] = {'u_ip': ip_u, 'v_ip': ip_v}
#             self.link_subnets[(v, u)] = {'u_ip': ip_v, 'v_ip': ip_u}
#             subnet_counter += 1

#     def get_link_ip(self, node, neighbor):
#         if (node, neighbor) in self.link_subnets:
#             return self.link_subnets[(node, neighbor)]['u_ip']
#         return None

# # --- 2. Special Node Class ---
# class LinuxRouter(Node):
#     """A Node with IP forwarding enabled."""
#     def config(self, **params):
#         super(LinuxRouter, self).config(**params)
#         self.cmd('sysctl -w net.ipv4.ip_forward=1')

# def create_topology_and_net():
#     print("🚀 Starting NDN Topology")
#     allocator = IPAllocator(LINKS)
    
#     net = Mininet(topo=None, build=False, link=TCLink)

#     print("*** Adding Nodes")
#     nodes_obj = {}
#     for n in NODES:
#         if n.startswith('pro') or n == 'con0':
#             nodes_obj[n] = net.addHost(n, privateDirs=['/run'], ip=None)
#         else:
#             nodes_obj[n] = net.addHost(n, cls=LinuxRouter, privateDirs=['/run'], ip=None)

#     print("*** Adding Links & Configuring Interfaces")
#     for i, (src_name, dst_name, bw, delay, queue) in enumerate(LINKS):
#         src = nodes_obj[src_name]
#         dst = nodes_obj[dst_name]
        
#         net.addLink(src, dst, bw=bw, delay=delay, max_queue_size=queue,
#                     intfName1=f"{src_name}-{dst_name}",
#                     intfName2=f"{dst_name}-{src_name}")
        
#         src_ip = allocator.get_link_ip(src_name, dst_name)
#         dst_ip = allocator.get_link_ip(dst_name, src_name)
        
#         src.setIP(src_ip, prefixLen=30, intf=f"{src_name}-{dst_name}")
#         dst.setIP(dst_ip, prefixLen=30, intf=f"{dst_name}-{src_name}")

#     print("*** Building network")
#     net.build()
#     print("*** Starting network")
#     net.start()
    
#     configure_linux_networking(net)
    
#     return net, allocator

# def configure_linux_networking(net):
#     print("*** Configuring Linux Kernel Networking (Disabling rp_filter)...")
#     for host in net.hosts:
#         host.cmd('sysctl -w net.ipv4.conf.all.rp_filter=0')
#         host.cmd('sysctl -w net.ipv4.conf.default.rp_filter=0')
#         host.cmd('for i in $(ls /sys/class/net/); do sysctl -w net.ipv4.conf.$i.rp_filter=0; done')

# def log_ip_configuration(net):
#     print("\n📊 Verifying IP Configuration:")
#     for node in net.hosts:
#         print(f"  Node: {node.name}")
#         intf_names = sorted(node.intfNames())
#         for intf_name in intf_names:
#             if intf_name == 'lo': continue
#             ip = node.IP(intf_name)
#             if ip:
#                 print(f"    - {intf_name}: {ip}")
#             else:
#                 print(f"    - {intf_name}: [Unassigned]")

# def warmup_network(net, allocator):
#     print("\n🔥 Warming up network (Checking Neighbor Connectivity)...")
#     time.sleep(2) 

#     failed_links = 0
#     for src_name, dst_name, _, _, _ in LINKS:
#         src_node = net.get(src_name)
#         target_ip = allocator.get_link_ip(dst_name, src_name)
        
#         result = src_node.cmd(f"ping -c 1 -W 1 {target_ip}")
        
#         if "1 received" not in result and "1 packets received" not in result:
#              print(f"⚠️  Warning: Link {src_name} -> {dst_name} ({target_ip}) failed.")
#              failed_links += 1
    
#     if failed_links == 0:
#         print("✅ Network warmup complete. All neighbors reachable.")
#     else:
#         print(f"❌ Warmup finished with {failed_links} failures.")

# def start_ndnd_daemons(net):
#     # Step 1: Start all ndnd daemon for all nodes
#     print(f"\nStarting ndnd daemons on all {len(NODES)} nodes...")
#     script_dir = os.path.dirname(os.path.abspath(__file__))
#     configs_dir = os.path.join(script_dir, '..', 'configs')
#     configs_dir = os.path.abspath(configs_dir)
#     logs_dir = os.path.join(script_dir, '..', 'logs')
#     os.makedirs(logs_dir, exist_ok=True)
    
#     for node_name in NODES:
#         node = net.get(node_name)
#         config_path = os.path.join(configs_dir, f'{node_name}.yml')
#         log_path = os.path.join(logs_dir, f'{node_name}.log')
        
#         node.cmd('mkdir -p /run/nfd')
#         node.cmd('rm -f /run/nfd/nfd.sock') 
        
#         cmd = f'env NDN_LOG=trace ndnd daemon {config_path} > {log_path} 2>&1 &'
#         node.cmd(cmd)
#         time.sleep(0.1) 
    
#     print("All ndnd daemons started.")

# def stop_ndnd_daemons(net):
#     print("\nStopping applications and ndnd daemons...")
#     for node_name in NODES:
#         node = net.get(node_name)
#         node.cmd('killall producer')
#         node.cmd('killall consumer')
#         node.cmd('killall ndnd')
#         node.cmd('rm -f /run/nfd/nfd.sock')
#     print("Cleanup complete.")

# # --- Step 3 Logic: Dynamic Route Configuration ---
# def configure_con0_routing(net):
#     print("\n=== Step 3: Configuring Dynamic Routes for con0 ===")
#     con0 = net.get('con0')
    
#     # 3.1 Read Face List
#     print("Reading face-list...")
#     face_output = con0.cmd('ndnd fw face-list')
#     udp_faces = []
    
#     # Parse face-list for UDP faces
#     # Example: faceid=4 remote=udp4://172.16.0.42:6363 ...
#     for line in face_output.split('\n'):
#         if 'remote=udp4://' in line:
#             # Extract faceid
#             match = re.search(r'faceid=(\d+)', line)
#             if match:
#                 fid = int(match.group(1))
#                 udp_faces.append(fid)
    
#     print(f"Found UDP Face IDs: {udp_faces}")

#     # 3.2 Read Route List
#     print("Reading route-list...")
#     route_output = con0.cmd('ndnd fw route-list')
    
#     # Data structure: prefix -> {'faces': set(), 'max_cost': int}
#     prefix_data = {}
    
#     # Parse route-list
#     # Example: prefix=/pro4app nexthop=4 origin=nlsr cost=3 ...
#     for line in route_output.split('\n'):
#         if 'prefix=/pro' in line:
#             # Extract Prefix
#             p_match = re.search(r'prefix=([^ ]+)', line)
#             nh_match = re.search(r'nexthop=(\d+)', line)
#             c_match = re.search(r'cost=(\d+)', line)
            
#             if p_match and nh_match and c_match:
#                 prefix = p_match.group(1)
#                 nexthop = int(nh_match.group(1))
#                 cost = int(c_match.group(1))
                
#                 if prefix not in prefix_data:
#                     prefix_data[prefix] = {'faces': set(), 'max_cost': 0}
                
#                 prefix_data[prefix]['faces'].add(nexthop)
#                 if cost > prefix_data[prefix]['max_cost']:
#                     prefix_data[prefix]['max_cost'] = cost

#     # 3.3 Inject Missing Routes
#     print("Injecting missing routes...")
#     added_count = 0
    
#     for prefix, data in prefix_data.items():
#         target_cost = data['max_cost'] # Use the largest existing cost
#         existing_faces = data['faces']
        
#         for face_id in udp_faces:
#             if face_id not in existing_faces:
#                 # Add route
#                 print(f"  + Adding route: prefix={prefix} face={face_id} cost={target_cost}")
#                 con0.cmd(f"ndnd fw route-add prefix={prefix} face={face_id} cost={target_cost}")
#                 added_count += 1
#             else:
#                 # Route exists, skip
#                 pass

#     if added_count == 0:
#         print("  No new routes needed (mesh is full).")
#     else:
#         print(f"  Successfully added {added_count} routes.")

#     # 3.4 Verify with FIB List
#     print("\n=== Verifying FIB (fib-list) ===")
#     fib_output = con0.cmd('ndnd fw fib-list')
#     # Just print the lines relevant to /pro to keep log clean
#     for line in fib_output.split('\n'):
#         if '/pro' in line:
#             print(line)

# def run_applications(net):
#     script_dir = os.path.dirname(os.path.abspath(__file__))
#     apps_dir = os.path.join(script_dir, '..', 'apps')
#     logs_dir = os.path.join(script_dir, '..', 'logs')
    
#     # Step 1 (Cont.): Wait for convergence
#     print("\nWaiting 60 seconds for routing convergence (NLSR/DV)...")
#     time.sleep(60)
    
#     # Step 2: Start Producers
#     print("=== Starting Producer Applications ===")
#     p_bin = os.path.join(apps_dir, 'producer', 'producer')
#     for i in range(5):
#         node_name = f'pro{i}'
#         p_node = net.get(node_name)
#         p_log = os.path.join(logs_dir, f'{node_name}_app.log')
#         app_name = f"{node_name}app"
#         prefix = f"/pro{i}"
#         print(f"Starting Producer on {node_name} ({prefix})")
#         p_node.cmd(f'{p_bin} {app_name} {prefix} > {p_log} 2>&1 &')
#         time.sleep(0.2)

#     # Wait for producers to register prefixes locally so they propagate
#     print("Waiting 10s for producers to register prefixes...")
#     time.sleep(2)

#     # Step 3: Dynamic Route Configuration
#     configure_con0_routing(net)

#     # Step 3.5: Set Forwarding Strategy
#     print("\n=== Setting Multipath Strategy for con0 ===")
#     net.get('con0').cmd('ndnd fw strategy-set prefix=/ strategy=/localhost/nfd/strategy/multipath/v=1')

#     # Step 4: Start Consumer
#     time.sleep(2)
#     c0 = net.get('con0')
#     c0_bin = os.path.join(apps_dir, 'consumer', 'consumer')
#     c0_log = os.path.join(logs_dir, 'con0_app.log')
#     print(f"\n=== Starting Consumer on con0 ===")
#     c0.cmd(f'{c0_bin} con0app 0-4 > {c0_log} 2>&1 &')
    
#     print("Applications running.")

# if __name__ == '__main__':
#     setLogLevel('info')
#     script_dir = os.path.dirname(os.path.abspath(__file__))
#     original_dir = os.getcwd()
    
#     # Clean up previous runs
#     os.system('mn -c > /dev/null 2>&1')
    
#     net, allocator = create_topology_and_net()
    
#     try:
#         # Log the IPs before warming up
#         log_ip_configuration(net)
        
#         warmup_network(net, allocator)
#         start_ndnd_daemons(net)
#         run_applications(net)
#         CLI(net)
        
#     finally:
#         stop_ndnd_daemons(net)
#         net.stop()
#         os.chdir(original_dir)
#         os.system('mn -c > /dev/null 2>&1')


