#!/bin/bash
gcc -o pelt -lconfig -lpq -lmysqlclient -L/usr/local/mysql/lib -I/usr/local/mysql/include pelt.c