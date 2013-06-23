#!/bin/sh
CFLAGS='-O0 -g' ./configure --prefix=/tmp/pg --enable-depend --enable-debug --enable-cassert
make -sj6
make -s install

