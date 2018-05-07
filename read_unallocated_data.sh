#!/usr/bin/env bash

dd if=$1 of=$2 conv=notrunc skip=$3 count=$4 bs=$5
