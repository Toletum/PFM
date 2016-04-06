#!/bin/bash

/home/toletum/stop.sh
rm -r neo4j-community-3.0.0-M03/data/databases/graph.db
neo4j-import --into neo4j-community-3.0.0-M03/data/databases/graph.db \
--nodes "barrios.header,barrios.csv" \
--nodes "meses.header,meses.csv" \
--nodes "dias.header,dias.csv" \
--relationships "mesesdias.header,mesesdias.csv" \
--relationships "delitos.header,delitos.csv"
/home/toletum/start.sh
