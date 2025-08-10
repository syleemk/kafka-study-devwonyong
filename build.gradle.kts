plugins {
    id("java")
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.kafka:kafka-clients:2.5.0")
    implementation("org.apache.kafka:kafka-streams:2.5.0")
    implementation("org.slf4j:slf4j-simple:1.7.30")
}