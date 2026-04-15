FROM flink:2.2.0-scala_2.12-java17

COPY target/flink-optimisations-*.jar /flink-optimisations.jar

ENTRYPOINT ["java", "-cp", "/opt/flink/lib/flink-cep-2.2.0.jar:/opt/flink/lib/flink-connector-files-2.2.0.jar:/opt/flink/lib/flink-csv-2.2.0.jar:/opt/flink/lib/flink-json-2.2.0.jar:/opt/flink/lib/flink-scala_2.12-2.2.0.jar:/opt/flink/lib/flink-table-api-java-uber-2.2.0.jar:/opt/flink/lib/flink-table-planner-loader-2.2.0.jar:/opt/flink/lib/flink-table-runtime-2.2.0.jar:/opt/flink/lib/log4j-1.2-api-2.24.3.jar:/opt/flink/lib/log4j-api-2.24.3.jar:/opt/flink/lib/log4j-core-2.24.3.jar:/opt/flink/lib/log4j-slf4j-impl-2.24.3.jar:/opt/flink/lib/flink-dist-2.2.0.jar:/flink-optimisations.jar", "com.xebia.flink.workshop.optimisations.reinterpret.ReinterpretBenchmarks"]