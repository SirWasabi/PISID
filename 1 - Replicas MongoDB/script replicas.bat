@echo off
start "Server S1" /MIN mongod --config c:\replica\server1\server1.conf
start "Server S2" /MIN mongod --config c:\replica\server2\server2.conf
start "Server S3" /MIN mongod --config c:\replica\server3\server3.conf