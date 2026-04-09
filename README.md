# MARS Prototype

This repository contains the source code and instructions for the MARS prototype. This project is built using NDNd, a Golang implementation of the Named Data Networking (NDN) protocol stack. Forwarder and application are both written using ndnd library.

**Source project:**  
For full context, details about underlying implementation, please refer to the official ndnd page:  
[https://github.com/named-data/ndnd.git](https://github.com/named-data/ndnd.git)

---

## Setup Guide: Setup Mininet/Go on Ubuntu 22.04

To reproduce the experiments described in the **MARS**, we need to install mininet/go1.23 first.

### Install Mininet
First, ensure Mininet is installed on your system.
```bash
sudo apt-get update
sudo apt-get install mininet
```

### Install go1.23

```
# 1. Download the Go 1.23.0 archive
wget https://go.dev/dl/go1.23.0.linux-amd64.tar.gz

# 2. Remove any previous Go installation and extract the new archive to /usr/local
sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go1.23.0.linux-amd64.tar.gz

# 3. Add Go to your PATH (if you haven't already)
echo "export PATH=\$PATH:/usr/local/go/bin" >> ~/.profile

# 4. Apply the changes to the current session
source ~/.profile

# 5. Verify the installation
go version
```

### Clone the project from github

```
git clone -b emulation-ndnd https://github.com/YitongLI2000/MARS-ndnsim.git mars-go

```

### Build the project

- Build the ndnd source project under root directory
    ```
    cd mars-go
    make
    sudo make install
    ```
- Build receiver/sender
    ```
    cd std/examples/mars
    go build consumer.go
    cd ../producer
    go build producer.go
    ```
- Start experiment via mininet
    ```
    cd ../..
    sudo python3 ./scripts/ndnd_run.py
    ```

