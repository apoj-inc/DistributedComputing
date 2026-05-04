#!/bin/bash

echo $(sudo dmidecode -t 4 | grep ID -m 1 | sed 's/.*ID://;s/ //g') $(ip addr | grep eth0: -A 1 | awk '{print $2}' | sed 's/://g' | grep -v 'eth') | sha256sum | awk '{print $1}'
