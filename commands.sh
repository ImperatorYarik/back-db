#!/bin/bash

## MSQL ##
python3 main.py backup --db sakila --db-type mysql --connection-string mysql://root:root@localhost:3306/ --save-multi
sleep 1
python3 main.py backup --db sakila --db-type mysql --connection-string mysql://root:root@localhost:3306/ --type structure
sleep 1
python3 main.py backup --db sakila --db-type mysql --connection-string mysql://root:root@localhost:3306/ --type data
sleep 1
python3 main.py backup --db sakila --db-type mysql --connection-string mysql://root:root@localhost:3306/
sleep 1

## POSTGRESQL ##

python3 main.py backup --db northwind --db-type postgresql --connection-string postgresql://dumper:dumper@localhost:5432/ --save-multi
sleep 1
python3 main.py backup --db northwind --db-type postgresql --connection-string postgresql://dumper:dumper@localhost:5432/ --type structure
sleep 1
python3 main.py backup --db northwind --db-type postgresql --connection-string postgresql://dumper:dumper@localhost:5432/ --type data
sleep 1
python3 main.py backup --db northwind --db-type postgresql --connection-string postgresql://dumper:dumper@localhost:5432/
sleep 1


## RESTORE ##

#python3 main.py restore --db sakila --db-type mysql --connection-string mysql://root:root@localhost:3307/ --backup-version 1725030638

#python3 main.py restore --db northwind --db-type postgresql --connection-string postgresql://dumper:dumper@localhost:5433/


