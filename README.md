# data-service-rs #

## Create data-service rust version develop environment

# Pre-requirement


* The shell of the ubuntu environment is installed in the program folder, 
* which can be executed directly and completed according to the command line prompt.

```bash requirement_install.sh```

1.  The program built environment is ubuntu 18.04.
    Install Ubuntu environment related(curl, git).

    If not installed:
    ```
    sudo apt update
    sudo apt install curl
    sudo apt install git
    ```
    
    Specified package version.
    
    gcc(version 7.3.0, release 3ubuntu2)
    ```
    sudo apt install gcc=4:7.4.0-1ubuntu2.3
    ```
    
    OpenSSL(version 1.1.1, release 1ubuntu2.1~18.04.4)
    ```
    sudo apt install libssl-dev=1.1.1-1ubuntu2.1~18.04.4
    ```
    
    g++-multilib(version 7.4.0, release 1ubuntu2.3)
    ```
    sudo apt install g++-multilib=4:7.4.0-1ubuntu2.3
    ```
    
2. Rust install
     
    install rust 
    ```
    curl https://sh.rustup.rs -sSf | sh
    ```
    Just need install default toolchain.
    add Rust to your system PATH manually .
    ```
    source $HOME/.cargo/env
    ```
    (Alternatively, you can add the following line to your ~/.bash_profile)
    ```
    export PATH="$HOME/.cargo/bin:$PATH"
    ```
    
    The version developed is rustc 1.35.0 (3c235d560 2019-05-20),
    if not, install and change rust to the specified version:
    ```
    rustup toolchain install 1.35.0
    rustup default 1.35.0
    ```
    
    Test whether cargo is successfully installed(check cargo version):
    ```
    cargo --version
    ```
3.  Cassandra driver install

    Download and install cassandra-cpp dependencies(libuv), have *-dbg, *-dev, driver.
    
    dependencies driver, 1.28 ver. 
    ```
    wget https://downloads.datastax.com/cpp-driver/ubuntu/18.04/dependencies/libuv/v1.28.0/libuv1_1.28.0-1_amd64.deb
    sudo dpkg -i libuv1_1.28.0-1_amd64.deb
    ```
    dependencies *-dbg
    ```
    wget https://downloads.datastax.com/cpp-driver/ubuntu/18.04/dependencies/libuv/v1.28.0/libuv1-dbg_1.28.0-1_amd64.deb
    sudo dpkg -i libuv1-dbg_1.28.0-1_amd64.deb
    ```
    dependencies *-dev
    ```
    wget https://downloads.datastax.com/cpp-driver/ubuntu/18.04/dependencies/libuv/v1.28.0/libuv1-dev_1.28.0-1_amd64.deb
    sudo dpkg -i libuv1-dev_1.28.0-1_amd64.deb
    ```
    
    Download and install cassandra-cpp driver, have *-dbg, *-dev, driver.
    
    cassandra driver 2.12 ver.
    ```
    wget https://downloads.datastax.com/cpp-driver/ubuntu/18.04/cassandra/v2.12.0/cassandra-cpp-driver_2.12.0-1_amd64.deb
    sudo dpkg -i cassandra-cpp-driver_2.12.0-1_amd64.deb
    ```
    cassnadra *-dbg 2.12 ver.
    ```
    wget https://downloads.datastax.com/cpp-driver/ubuntu/18.04/cassandra/v2.12.0/cassandra-cpp-driver-dev_2.12.0-1_amd64.deb
    sudo dpkg -i cassandra-cpp-driver-dev_2.12.0-1_amd64.deb
    ```
    cassandra *-dbg 2.12 ver.
    ```
    wget https://downloads.datastax.com/cpp-driver/ubuntu/18.04/cassandra/v2.12.0/cassandra-cpp-driver-dbg_2.12.0-1_amd64.deb
    sudo dpkg -i cassandra-cpp-driver-dbg_2.12.0-1_amd64.deb
    ```
    __Make sure that the driver (specifically libcassandra_static.a and libcassandra.so) are in your “/usr/local/lib64/” or “/usr/lib/x86_64-linux-gnu/” directory__
    
# Build rust code(data service)

1. clone rust code
2. Enter the folder of the rust project(code fold name is data-service-rs):
    ```
    cd data-service-rs
    ```
3. Confirm config argument, config location in ```/data-service-rs/config```, name is ```data-service-config.yaml```:
    ```
    tsc:
      ip: '127.0.0.1'
      port: 9042
    thread:
      num: 8
    web:
      port: '8080'
    log:
      path: '/home/ubuntu/data-service-log/'
    ```
    
4. Create a folder for the log file:
    ```
    mkdir /home/ubuntu/data-service-log/
    ```
    
5. Build rust code, use release version:
    ```
    cargo build --release
    ```

6. Build data-service for systemd,configuring data-service.service in __/etc/systemd/system/__:
    ```
    [Unit]
    Description=cassandra-rs-driver
    After=network.target
    
    [Service]
    ExecStart=/home/ubuntu/data-service-rs/target/release/data-service-rs /home/ubuntu/data-service-rs/data-service-config.yaml
    [Install]
    WantedBy=multi-user.target
    ```

