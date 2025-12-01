from mininet.net import Mininet
from mininet.topo import Topo
from mininet.node import Host
from mininet.link import TCLink
from mininet.cli import CLI
import time
import os

class SimpleNDNTopo(Topo):
    def build(self):
        c0 = self.addHost('c0')
        f0 = self.addHost('f0')
        p0 = self.addHost('p0')
        self.addLink(c0, f0, cls=TCLink, bw=1000, delay='0ms', max_queue_size=100)
        self.addLink(f0, p0, cls=TCLink, bw=1000, delay='0ms', max_queue_size=100)

def start_ndnd_daemons(net):
    """Start ndnd daemon on all nodes"""
    nodes = ['c0', 'f0', 'p0']
    
    print("Starting ndnd daemons on all nodes...")
    
    # Get the absolute path to the configs directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    configs_dir = os.path.join(script_dir, '..', 'configs')
    configs_dir = os.path.abspath(configs_dir)
    
    for node_name in nodes:
        node = net.get(node_name)
        config_path = os.path.join(configs_dir, f'{node_name}.yml')
        cmd = f'ndnd daemon {config_path} &'
        print(f"Executing on {node_name}: {cmd}")
        node.cmd(cmd)
    
    print("All ndnd daemons started.")
    print("Use 'exit' to stop the network when done.\n")

def run_applications(net):
    """Helper function to run consumer and producer applications"""
    
    # Get the absolute path to the apps directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    apps_dir = os.path.join(script_dir, '..', 'apps')
    apps_dir = os.path.abspath(apps_dir)
    
    print("\n=== Application Commands ===")
    print("To run the applications manually:")
    print(f"Consumer: c0 {os.path.join(apps_dir, 'consumer', 'consumer')}")
    print(f"Producer: p0 {os.path.join(apps_dir, 'producer', 'producer')}")
    print("=============================\n")

if __name__ == '__main__':
    # Change to the script directory to ensure relative paths work
    script_dir = os.path.dirname(os.path.abspath(__file__))
    original_dir = os.getcwd()
    
    print(f"Script directory: {script_dir}")
    print(f"Original directory: {original_dir}")
    
    topo = SimpleNDNTopo()
    net = Mininet(topo=topo, link=TCLink, controller=None)
    net.start()
    
    # Set IP addresses for interfaces
    net.get('c0').setIP('10.0.0.1/24', intf='c0-eth0')
    net.get('f0').setIP('10.0.0.2/24', intf='f0-eth0')
    net.get('f0').setIP('10.0.1.1/24', intf='f0-eth1')
    net.get('p0').setIP('10.0.1.2/24', intf='p0-eth0')
    
    # Start ndnd daemons on all nodes
    start_ndnd_daemons(net)
    
    # Show application run commands
    run_applications(net)
    
    # Give control to user via CLI
    CLI(net)
    
    # Clean up when user exits CLI
    net.stop()
    
    # Restore original directory
    os.chdir(original_dir)



# from mininet.net import Mininet
# from mininet.topo import Topo
# from mininet.node import Host
# from mininet.link import TCLink
# from mininet.cli import CLI
# import time

# class SimpleNDNTopo(Topo):
#     def build(self):
#         c0 = self.addHost('c0')
#         f0 = self.addHost('f0')
#         p0 = self.addHost('p0')
#         self.addLink(c0, f0, cls=TCLink, bw=1000, delay='0ms', max_queue_size=100)
#         self.addLink(f0, p0, cls=TCLink, bw=1000, delay='0ms', max_queue_size=100)

# def start_ndnd_daemons(net):
#     """Start ndnd daemon on all nodes"""
#     nodes = ['c0', 'f0', 'p0']
    
#     print("Starting ndnd daemons on all nodes...")
#     for node_name in nodes:
#         node = net.get(node_name)
#         cmd = f'ndnd daemon {node_name}.yml &'
#         print(f"Executing on {node_name}: {cmd}")
#         node.cmd(cmd)
    
#     print("All ndnd daemons started.")
#     print("Use 'exit' to stop the network when done.\n")

# if __name__ == '__main__':
#     topo = SimpleNDNTopo()
#     net = Mininet(topo=topo, link=TCLink, controller=None)
#     net.start()
    
#     # Set IP addresses for interfaces
#     net.get('c0').setIP('10.0.0.1/24', intf='c0-eth0')
#     net.get('f0').setIP('10.0.0.2/24', intf='f0-eth0')
#     net.get('f0').setIP('10.0.1.1/24', intf='f0-eth1')
#     net.get('p0').setIP('10.0.1.2/24', intf='p0-eth0')
    
#     # Start ndnd daemons on all nodes
#     start_ndnd_daemons(net)
    
#     # Give control to user via CLI
#     CLI(net)
    
#     # Clean up when user exits CLI
#     net.stop()
