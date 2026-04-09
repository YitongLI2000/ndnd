from mininet.net import Mininet
from mininet.topo import Topo
from mininet.node import Host
from mininet.link import TCLink
from mininet.cli import CLI
import time
import os

class SimpleNDNTopo(Topo):
    def build(self):
        # Add privateDirs=['/run'] to isolate the socket files for each host
        c0 = self.addHost('c0', privateDirs=['/run'])
        f0 = self.addHost('f0', privateDirs=['/run'])
        p0 = self.addHost('p0', privateDirs=['/run'])
        
        self.addLink(c0, f0, cls=TCLink, bw=1000, delay='0ms', max_queue_size=100)
        self.addLink(f0, p0, cls=TCLink, bw=1000, delay='0ms', max_queue_size=100)

def start_ndnd_daemons(net):
    """Start ndnd daemon on all nodes sequentially with full logging"""
    nodes = ['p0', 'f0', 'c0'] 
    
    print("Starting ndnd daemons sequentially (p0 -> f0 -> c0)...")
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    configs_dir = os.path.join(script_dir, '..', 'configs')
    configs_dir = os.path.abspath(configs_dir)

    logs_dir = os.path.join(script_dir, '..', 'logs')
    os.makedirs(logs_dir, exist_ok=True)
    print(f"Logs will be saved to: {logs_dir}")
    
    for node_name in nodes:
        node = net.get(node_name)
        config_path = os.path.join(configs_dir, f'{node_name}.yml')
        log_path = os.path.join(logs_dir, f'{node_name}.log')
        
        # Ensure the directory exists in the private /run namespace
        node.cmd('mkdir -p /run/nfd')
        
        # Clean up previous socket
        node.cmd('rm -f /run/nfd/nfd.sock') 
        
        # Start daemon with trace logging
        cmd = f'env NDN_LOG=trace ndnd daemon {config_path} > {log_path} 2>&1 &'
        
        print(f"Starting {node_name} -> Log: {node_name}.log")
        node.cmd(cmd)
        
        time.sleep(1)
    
    print("All ndnd daemons started.")

def stop_ndnd_daemons(net):
    """Kill ndnd daemons and applications on all nodes"""
    nodes = ['c0', 'f0', 'p0']
    print("\nStopping applications and ndnd daemons...")
    for node_name in nodes:
        node = net.get(node_name)
        # Kill applications first
        node.cmd('killall producer')
        node.cmd('killall consumer')
        # Kill the ndnd process
        node.cmd('killall ndnd')
        node.cmd('rm -f /run/nfd/nfd.sock')
    print("Cleanup complete.")

def run_applications(net):
    """Automatically run consumer and producer applications after delay"""
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    apps_dir = os.path.join(script_dir, '..', 'apps')
    apps_dir = os.path.abspath(apps_dir)
    
    logs_dir = os.path.join(script_dir, '..', 'logs')
    
    # 1. Wait for routing convergence
    print("\nWaiting 10 seconds for routing convergence and DV stability...")
    time.sleep(10)
    
    print("=== Starting Applications ===")

    # 2. Start Producer on p0
    p0 = net.get('p0')
    p0_bin = os.path.join(apps_dir, 'producer', 'producer')
    # Log file named p0_app.log to distinguish from p0.log (forwarder)
    p0_log = os.path.join(logs_dir, 'p0_app.log')
    
    print(f"Starting Producer on p0 -> Log: {os.path.basename(p0_log)}")
    # Run in background (&)
    p0.cmd(f'{p0_bin} > {p0_log} 2>&1 &')

    time.sleep(1)  # Small delay to ensure producer starts before consumer

    # 3. Start Consumer on c0
    c0 = net.get('c0')
    c0_bin = os.path.join(apps_dir, 'consumer', 'consumer')
    # Log file named c0_app.log
    c0_log = os.path.join(logs_dir, 'c0_app.log')
    
    print(f"Starting Consumer on c0 -> Log: {os.path.basename(c0_log)}")
    # Run in background (&)
    c0.cmd(f'{c0_bin} > {c0_log} 2>&1 &')
    
    print("Applications running in background.")
    print("Use 'exit' to stop the network when done.\n")

if __name__ == '__main__':
    script_dir = os.path.dirname(os.path.abspath(__file__))
    original_dir = os.getcwd()
    
    print(f"Script directory: {script_dir}")
    print(f"Original directory: {original_dir}")
    
    # Ensure a clean slate before starting
    os.system('mn -c > /dev/null 2>&1')
    
    topo = SimpleNDNTopo()
    net = Mininet(topo=topo, link=TCLink, controller=None)
    net.start()
    
    try:
        # Set IP addresses for interfaces
        net.get('c0').setIP('10.0.0.1/24', intf='c0-eth0')
        net.get('f0').setIP('10.0.0.2/24', intf='f0-eth0')
        
        net.get('f0').setIP('10.0.1.1/24', intf='f0-eth1')
        net.get('p0').setIP('10.0.1.2/24', intf='p0-eth0')
        
        # Start ndnd daemons
        start_ndnd_daemons(net)
        
        # Automate application startup
        run_applications(net)
        
        CLI(net)
        
    finally:
        stop_ndnd_daemons(net)
        net.stop()
        os.chdir(original_dir)
        os.system('mn -c > /dev/null 2>&1')



# from mininet.net import Mininet
# from mininet.topo import Topo
# from mininet.node import Host
# from mininet.link import TCLink
# from mininet.cli import CLI
# import time
# import os

# class SimpleNDNTopo(Topo):
#     def build(self):
#         # CHANGE 1: Add privateDirs=['/run'] to isolate the socket files for each host
#         c0 = self.addHost('c0', privateDirs=['/run'])
#         f0 = self.addHost('f0', privateDirs=['/run'])
#         p0 = self.addHost('p0', privateDirs=['/run'])
        
#         self.addLink(c0, f0, cls=TCLink, bw=1000, delay='0ms', max_queue_size=100)
#         self.addLink(f0, p0, cls=TCLink, bw=1000, delay='0ms', max_queue_size=100)

# def start_ndnd_daemons(net):
#     """Start ndnd daemon on all nodes sequentially with full logging"""
#     nodes = ['p0', 'f0', 'c0'] 
    
#     print("Starting ndnd daemons sequentially (p0 -> f0 -> c0)...")
    
#     script_dir = os.path.dirname(os.path.abspath(__file__))
#     configs_dir = os.path.join(script_dir, '..', 'configs')
#     configs_dir = os.path.abspath(configs_dir)

#     logs_dir = os.path.join(script_dir, '..', 'logs')
#     os.makedirs(logs_dir, exist_ok=True)
#     print(f"Logs will be saved to: {logs_dir}")
    
#     for node_name in nodes:
#         node = net.get(node_name)
#         config_path = os.path.join(configs_dir, f'{node_name}.yml')
#         log_path = os.path.join(logs_dir, f'{node_name}.log')
        
#         # CHANGE 2: Ensure the directory exists in the private /run namespace
#         # Since /run is now private and empty, we must create the nfd folder.
#         node.cmd('mkdir -p /run/nfd')
        
#         # Clean up previous socket (safe now, only affects this node)
#         node.cmd('rm -f /run/nfd/nfd.sock') 
        
#         # Start daemon with trace logging
#         cmd = f'env NDN_LOG=trace ndnd daemon {config_path} > {log_path} 2>&1 &'
        
#         print(f"Starting {node_name} -> Log: {node_name}.log")
#         node.cmd(cmd)
        
#         time.sleep(1)
    
#     print("All ndnd daemons started.")
#     print("Use 'exit' to stop the network when done.\n")

# def stop_ndnd_daemons(net):
#     """Kill ndnd daemons on all nodes"""
#     nodes = ['c0', 'f0', 'p0']
#     print("\nStopping ndnd daemons...")
#     for node_name in nodes:
#         node = net.get(node_name)
#         node.cmd('killall ndnd')
#         node.cmd('rm -f /run/nfd/nfd.sock')
#     print("Daemons stopped and sockets cleaned.")

# def run_applications(net):
#     """Helper function to run consumer and producer applications"""
    
#     script_dir = os.path.dirname(os.path.abspath(__file__))
#     apps_dir = os.path.join(script_dir, '..', 'apps')
#     apps_dir = os.path.abspath(apps_dir)
    
#     print("\n=== Application Commands ===")
#     print("To run the applications manually:")
#     print(f"Consumer: c0 {os.path.join(apps_dir, 'consumer', 'consumer')}")
#     print(f"Producer: p0 {os.path.join(apps_dir, 'producer', 'producer')}")
#     print("=============================\n")

# if __name__ == '__main__':
#     script_dir = os.path.dirname(os.path.abspath(__file__))
#     original_dir = os.getcwd()
    
#     print(f"Script directory: {script_dir}")
#     print(f"Original directory: {original_dir}")
    
#     # Ensure a clean slate before starting
#     os.system('mn -c > /dev/null 2>&1')
    
#     topo = SimpleNDNTopo()
#     net = Mininet(topo=topo, link=TCLink, controller=None)
#     net.start()
    
#     try:
#         # Set IP addresses for interfaces
#         net.get('c0').setIP('10.0.0.1/24', intf='c0-eth0')
#         net.get('f0').setIP('10.0.0.2/24', intf='f0-eth0')
        
#         net.get('f0').setIP('10.0.1.1/24', intf='f0-eth1')
#         net.get('p0').setIP('10.0.1.2/24', intf='p0-eth0')
        
#         # Start ndnd daemons
#         start_ndnd_daemons(net)
        
#         # Show application run commands
#         run_applications(net)
        
#         CLI(net)
        
#     finally:
#         stop_ndnd_daemons(net)
#         net.stop()
#         os.chdir(original_dir)
#         os.system('mn -c > /dev/null 2>&1')

