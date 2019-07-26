#!/bin/bash

#Ubuntu environment related
echo "----------------update----------------"
sudo apt-get update

echo "----------------curl installing...-------------"
sudo apt install curl

echo "----------------git installing...--------------"
sudo apt install git

echo "----------------gcc(version 7.3.0, release 3ubuntu2) installing...----------------"
sudo apt install gcc=4:7.4.0-1ubuntu2.3

echo "----------------OpenSSL(version 1.1.1, release 1ubuntu2.1~18.04.4) installing...--"
sudo apt install libssl-dev=1.1.1-1ubuntu2.1~18.04.4

echo "----------------g++-multilib(version 7.4.0, release 1ubuntu2.3) installing...-----"
sudo apt install g++-multilib=4:7.4.0-1ubuntu2.3


#Rust part
echo "----------------rust install----------------"
echo "----------------input '1'(default)----------"
curl https://sh.rustup.rs -sSf | sh

echo "----------------add Rust to your system PATH------------"
source $HOME/.cargo/env

echo "----------------install rust 1.35.0 version-------------"
rustup toolchain install 1.35.0

echo "----------------Specify the rust version----------------"
rustup default 1.35.0

#Cassandra part
## cassandrs-cpp dependencies
echo "----------------cassandra-cpp dependencies driver download------------"
wget https://downloads.datastax.com/cpp-driver/ubuntu/18.04/dependencies/libuv/v1.28.0/libuv1_1.28.0-1_amd64.deb

echo "----------------cassandra-cpp dependencies driver install-------------"
sudo dpkg -i libuv1_1.28.0-1_amd64.deb

echo "----------------cassandra-cpp dependencies dbg download---------------"
wget https://downloads.datastax.com/cpp-driver/ubuntu/18.04/dependencies/libuv/v1.28.0/libuv1-dbg_1.28.0-1_amd64.deb

echo "----------------cassandra-cpp dependencies dbg install----------------"
sudo dpkg -i libuv1-dbg_1.28.0-1_amd64.deb

echo "----------------cassandra-cpp dependencies dev download---------------"
wget https://downloads.datastax.com/cpp-driver/ubuntu/18.04/dependencies/libuv/v1.28.0/libuv1-dev_1.28.0-1_amd64.deb

echo "----------------cassandra-cpp dependencies dev install----------------"
sudo dpkg -i libuv1-dev_1.28.0-1_amd64.deb


## cassandrs-cpp driver
echo "----------------cassandrs-cpp driver download----------------"
wget https://downloads.datastax.com/cpp-driver/ubuntu/18.04/cassandra/v2.12.0/cassandra-cpp-driver_2.12.0-1_amd64.deb

echo "----------------cassandra-cpp  driver install----------------"
sudo dpkg -i cassandra-cpp-driver_2.12.0-1_amd64.deb

echo "----------------cassandra-cpp driver dbg download------------"
wget https://downloads.datastax.com/cpp-driver/ubuntu/18.04/cassandra/v2.12.0/cassandra-cpp-driver-dev_2.12.0-1_amd64.deb

echo "----------------cassandra-cpp driver dev install-------------"
sudo dpkg -i cassandra-cpp-driver-dev_2.12.0-1_amd64.deb


echo "----------------cassandra-cpp driver dev download------------"
wget https://downloads.datastax.com/cpp-driver/ubuntu/18.04/cassandra/v2.12.0/cassandra-cpp-driver-dbg_2.12.0-1_amd64.deb

echo "----------------cassandra-cpp driver dbg install-------------"
sudo dpkg -i cassandra-cpp-driver-dbg_2.12.0-1_amd64.deb


echo "=========Make sure that the driver (specifically libcassandra_static.a and libcassandra.so)  
are in your “/usr/local/lib64/” or “/usr/lib/x86_64-linux-gnu/” directory========"
sudo find / -name libcassandra_static.a
sudo find / -name libcassandra.so
