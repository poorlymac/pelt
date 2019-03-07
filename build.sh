#!/bin/bash
if [ "$1" == "static" ]
then
    # A static compile if you want to distribute
    # Note using homebrew libraries
    gcc -O3 -o pelt \
        pelt.c postgres.c extract.c \
        /usr/local/lib/libpq.a \
        /usr/local/lib/libcsv.a \
        /usr/local/lib/libconfig.a \
        /usr/local/mysql/lib/libmysqlclient.a \
        /usr/local/Cellar/openssl/1.0.2q/lib/libcrypto.a \
        /usr/local/Cellar/openssl/1.0.2q/lib/libssl.a \
        -lldap \
        -lkrb5 \
        -I/usr/local/mysql/include
else
    gcc -O3 -o pelt \
        pelt.c postgres.c extract.c \
        -lconfig \
        -lpq \
        -lcsv \
        -lmysqlclient -L/usr/local/mysql/lib -I/usr/local/mysql/include
fi