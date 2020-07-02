
dependencies {
    // logging
    implementation("org.slf4j:slf4j-api:1.7.30")
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:2.12.0")
    implementation("org.apache.logging.log4j:log4j-api:2.11.2")
    implementation("org.apache.logging.log4j:log4j-core:2.11.2")

    // JSON serialization
    implementation("com.fasterxml.jackson.core:jackson-databind:2.11.0")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jdk8:2.11.0")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.11.0")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.11.0")
    implementation("com.fasterxml.jackson.module:jackson-module-parameter-names:2.11.0")

    // Kafka
    implementation("org.apache.kafka:kafka-streams:2.5.0")
    implementation("io.confluent:kafka-streams-avro-serde:5.5.0")

    implementation("org.elasticsearch:elasticsearch:7.7.1")
    implementation("org.elasticsearch.plugin:percolator-client:7.7.1")
    implementation("org.elasticsearch.client:elasticsearch-rest-high-level-client:7.7.1")


    implementation("org.json:json:20200518")


    testImplementation("org.junit.jupiter:junit-jupiter-api:5.6.2")
    testImplementation("org.junit.jupiter:junit-jupiter-engine:5.6.2")

    testImplementation("org.testcontainers:junit-jupiter:1.14.3")
    testImplementation("org.testcontainers:elasticsearch:1.14.3")
    testImplementation("org.testcontainers:kafka:1.14.3")


    testImplementation("org.awaitility:awaitility-kotlin:4.0.3")

}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
}