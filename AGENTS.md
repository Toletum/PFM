# PFM

Proyecto fin de máster en arquitectura de Big Data: procesamiento de datos de delitos con Kafka, Flink, Neo4j y Redis (Java, Maven).

## Visibilidad y rama

- Público (licencia en `LICENSE`).
- Rama por defecto: `master`.

## Stack

- Java con Maven (`pom.xml`, artifact `PFM` 0.0.1-SNAPSHOT).
- Dependencias clave: Apache Flink (1.0.0 y 0.10.2), `flink-connector-kafka`, `kafka-clients` 0.9.0.0, `neo4j-jdbc` 2.3.2, `jedis` 2.8.0, `flink-jdbc`, JUnit 3.8.1 (test).
- Algunas dependencias de Flink están comentadas en el `pom.xml`.
- Shell para importar/exportar datos (HDFS, neo4j-import).

## Estructura

- `pom.xml`: dependencias y plugins (assembly, compiler 2.3.2). El `mainClass` es un placeholder (`fully.qualified.MainClass`).
- `src/main/java/org/toletum/pfm/`: código principal.
  - `batch/`: jobs batch (`Batch.java`, formatos de salida y tuplas de delitos).
  - `streaming/`: jobs de streaming con Flink (`Streaming.java`, sinks y funciones de estadísticas).
  - `simulador/Simulador.java`: simulador de eventos.
  - `Config.java`, `Utils.java`: configuración y utilidades.
  - `OLD/`: código antiguo, no usado en el flujo actual.
- `src/main/java/Pruebas/`: ejemplos sueltos de Flink, Redis y Neo4j (fuera del paquete principal).
- `src/test/java/Toletum/PFM/AppTest.java`: test de ejemplo de JUnit.
- `sh/`: scripts `export.sh` (HDFS a CSV y envío por scp), `import.sh` (neo4j-import) y cabeceras `*.header` para la importación.
- `doc/`: diagrama de arquitectura y presentación en PDF.

## Comandos

No hay instrucciones de build en el README. Comandos deducidos del `pom.xml` (no ejecutados):

- Compilar: `mvn compile` (no verificado).
- Test: `mvn test` (no verificado; solo hay un test de ejemplo).
- Paquete: `mvn package` con el plugin assembly (no verificado; `mainClass` sin configurar).
- Scripts: `sh/export.sh` (requiere HDFS y acceso a `database`); `sh/import.sh` (requiere neo4j en `~/` y `stop.sh`/`start.sh` en `/home/toletum/`).

## Configuración

- `src/main/java/org/toletum/pfm/Config.java` concentra la configuración (no revisada en detalle; puede contener hosts o credenciales: revisar antes de citar valores).
- Sin variables de entorno declaradas.
- Los scripts de `sh/` usan rutas absolutas del entorno personal del autor (`/home/toletum/`).

## Convenciones para agentes

- Los paquetes de `OLD/` no deben tocarse salvo petición explícita.
- Las clases de `Pruebas/` son ejemplos; no deben usarse como referencia de la arquitectura.
- Las versiones de Flink, Kafka y Neo4j son antiguas (2015-2016); no actualizarlas sin revisar la API.
- Los scripts de importación modifican bases de datos (`rm -r ... graph.db`): no ejecutarlos sin confirmación.

## Estado

- Proyecto de máster antiguo; el README es breve.
- Atención: `mainClass` es `fully.qualified.MainClass`; el assembly no está configurado.
- Atención: dependencias de Flink mezcladas (0.10.2 y 1.0.0) y el artefacto `flink-connector-kafka` en 0.10.2; no verificado que compile.
- Atención: hay código en `OLD/` y `Pruebas/` que no forma parte del flujo principal.
- Atención: `import.sh` borra la base Neo4j antes de importar.
- Test: solo `AppTest` de ejemplo; no verificado si pasa.
