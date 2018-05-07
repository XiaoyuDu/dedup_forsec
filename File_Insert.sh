#!/usr/bin/env bash

if [ $1 = "/dev/zero" ]
then
    dd if=$1 of=$2 conv=notrunc seek=$3 count=1
else
    dd if=$1 of=$2 conv=notrunc skip=$3 seek=$4 count=$5 bs=$6
fi
