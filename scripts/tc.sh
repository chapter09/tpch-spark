#!/bin/bash

if [ $# -ne 2 ]; then
  echo "tc.sh <dst_ip> <bw>"
  exit 1
fi

dst_ip=$1
dev="ens3"
bw=$2

if [[ $(tc qdisc show dev ${dev} | grep 'htb') ]]; then
  sudo tc qdisc del dev ${dev} root handle 1: htb
fi

#handles=`tc filter list dev ${dev} |grep "flowid 1:"|awk '{print $10}'`

#for handle in $handles
#do
#  tc filter delete dev ${dev} parent 1: protocol ip prio 1 handle ${handle} u32
#done

tc qdisc add dev ens3 root handle 1: htb
tc class add dev ens3 parent 1: classid 1:1 htb rate ${bw}mbit ceil ${bw}mbit
tc filter add dev ens3 protocol ip parent 1: prio 1 u32 match ip dst $dst_ip flowid 1:1

