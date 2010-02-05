#! /bin/bash

name="main.sh $*"
prg="jmanager.py"
${prg} "$(dirname $0)/proc_sub.sh" -n "$name" -P 2

