plugins {
    kotlin("jvm") version "1.3.72"
}


allprojects {
    group = "com.findinpath"
    version = "1.0-SNAPSHOT"

    apply(plugin="kotlin")

    tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
        kotlinOptions.jvmTarget = "1.8"
    }

    repositories {
        mavenCentral()
        maven(url ="https://packages.confluent.io/maven/")
    }


    dependencies {
        implementation(kotlin("stdlib-jdk8"))
    }
}