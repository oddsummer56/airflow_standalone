#!/bin/bash

CSV_PATH=$1
DEL_DT=$2

user="root"
password="oddsummer56"
database="history_db"

mysql --local-infile=1 -u"$user" -p"$password" "$database" <<EOF
-- LOAD DATA LOCAL INFILE '/var/lib/mysql-files/csv.csv'
DELETE FROM history_db.tmp_cmd_usage WHERE dt='${DEL_DT}';

LOAD DATA LOCAL INFILE '$CSV_PATH'
INTO TABLE history_db.tmp_cmd_usage
character set latin1
FIELDS TERMINATED BY ',' ENCLOSED BY '^' ESCAPED BY '\b'
LINES TERMINATED BY '\n';
EOF
