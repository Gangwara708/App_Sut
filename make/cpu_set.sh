#!/bin/bash
taskset -p -c 0  `pgrep -l ss7p | grep -w ss7p | awk '{ print $1 }'`
taskset -p -c 1  `pgrep -l app_map | grep -w app_map | awk '{ print $1 }'`
taskset -p -c 2  `pgrep -l MAP_Appl| grep -w MAP_Appl | awk '{ print $1 }'`
taskset -p -c 5  `pgrep -l app_map_em | grep -w app_map_em | awk '{ print $1 }'`
taskset -p -c 5  `pgrep -l em | grep -w em | awk '{ print $1 }'`
taskset -p -c 5  `pgrep -l app_map_cli | grep -w app_map_cli | awk '{ print $1 }'`
taskset -p -c 5  `pgrep -l cli | grep -w cli | awk '{ print $1 }'`
exit 0
