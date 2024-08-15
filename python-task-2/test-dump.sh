#!/bin/bash
python3 main.py backup --db sakila --db-type mysql --connection-string mysql://root:root@localhost:3306/ --type structure
