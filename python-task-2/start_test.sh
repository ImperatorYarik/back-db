#!/bin/bash
#python3 main.py backup --db sakila --db-type mysql --connection-string mysql://root:root@localhost:3306/

#python3 main.py backup --db sakila --db-type mysql --connection-string mysql://root:root@localhost:3306/ --save-multi

#python3 main.py restore --db sakila --db-type mysql --connection-string mysql://root:root@localhost:3306/ --type structure

#python3 main.py restore --db sakila --db-type mysql --connection-string mysql://root:root@localhost:3307/ --backup-version 1725030638

#python3 main.py -vvv restore --db sakila --db-type mysql --connection-string mysql://root:root@localhost:3307/

#python3 main.py backup --db sakila --db-type mysql --connection-string mysql://root:root@localhost:3306/ --save-multi

python3 main.py backup --db northwind --db-type postgresql --connection-string postgresql://dumper:dumper@localhost:5432/ --save-multi

