#! /bin/bash

name="sub.sh $*"
prg="./jmanager.py"
${prg} "ls /tmp | sort | head; sleep 5; echo 'test'" -n "$name A" -P 1 -R 6
${prg} "_ERROR_ls /tmp | sort | head && sleep 5; echo 'test'" -n "$name B" -P 1 -R 6
${prg} "ls /tmp | sort | tail" -n "$name C" -P 1 -R 4

