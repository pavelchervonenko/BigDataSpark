plugins {
    id("application")
    id("com.github.johnrengelman.shadow") version "8.1.1"

}

repositories {
    mavenCentral()
}

dependencies {
    compileOnly("org.apache.spark:spark-core_2.13:3.5.0")
    compileOnly("org.apache.spark:spark-sql_2.13:3.5.0")

    implementation("org.postgresql:postgresql:42.7.10")

    implementation("com.clickhouse:clickhouse-jdbc:0.9.8:all")

    implementation("com.datastax.spark:spark-cassandra-connector_2.13:3.5.1")

    implementation("org.mongodb.spark:mongo-spark-connector_2.13:11.0.1")

    testImplementation("org.junit.jupiter:junit-jupiter-api:5.13.4")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

application {
    mainClass = "org.example.Main"
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}

tasks.shadowJar {
    archiveClassifier.set("")
    mergeServiceFiles()
}
