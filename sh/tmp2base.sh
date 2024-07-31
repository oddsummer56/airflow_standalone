#!/bin/bash

DT=$1

user="root"
password="oddsummer56"
database="history_db"

mysql --local-infile=1 -u"$user" -p"$password" "$database" <<EOF
DELETE FROM history_db.cmd_usage where dt='$DT';

INSERT into cmd_usage
SELECT
	CASE WHEN dt like '%-%-%'
		THEN str_to_date(dt, '%Y-%m-%d')
		ELSE str_to_date('1970-01-01', '%Y-%m-%d')
	END AS dt,
	command,
	CASE WHEN cnt REGEXP '[0-9]+$'
		THEN cast(cnt as unsigned)
		ELSE -1
	END AS cnt,
	${DT} 
FROM tmp_cmd_usage
WHERE dt='${DT}';
EOF
