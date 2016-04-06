hdfs dfs -cat /meses.csv* > meses.csv
hdfs dfs -cat /barrios.csv* > barrios.csv
hdfs dfs -cat /dias.csv* > dias.csv
hdfs dfs -cat /mesesdias.csv* > mesesdias.csv 
hdfs dfs -cat /delitos.csv* > delitos.csv 

scp dias.csv barrios.csv meses.csv mesesdias.csv delitos.csv database:

hdfs dfs -rm /meses.csv*
hdfs dfs -rm /barrios.csv*
hdfs dfs -rm /dias.csv*
hdfs dfs -rm /mesesdias.csv*
hdfs dfs -rm /delitos.csv*
