#!/bin/bash

if [ "$1" == "u" ]; 
then
  echo "change to class 1:4"
  tc filter del dev ens3 parent 1: handle 800::800 prio 1 protocol ip u32
  tc filter add dev ens3 protocol ip parent 1: prio 1 u32 match ip dst 10.6.3.19 flowid 1:4
else
  echo "change to class 1:1"
  tc filter del dev ens3 parent 1: handle 800::800 prio 1 protocol ip u32
  tc filter add dev ens3 protocol ip parent 1: prio 1 u32 match ip dst 10.6.3.19 flowid 1:1
fi
