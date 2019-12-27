plugins {
    java
    application
    `java-library`
}

application {
    mainClassName = "io.toxa108.storage.BlitzarDatabase"
}

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

repositories {
    mavenCentral()
    jcenter()
}

sourceSets.create("jmh") {
    java.setSrcDirs(listOf("src/jmh/java"))
}

dependencies {
    implementation("org.projectlombok:lombok:1.18.10")

    "jmhImplementation"(project)
    "jmhImplementation"("org.openjdk.jmh:jmh-core:1.21")
    "jmhAnnotationProcessor"("org.openjdk.jmh:jmh-generator-annprocess:1.21")

    testCompile ("junit:junit:4.12")
}

tasks {
    register("jmh", type = JavaExec::class) {
        dependsOn("jmhClasses")
        group = "benchmark"
        main = "org.openjdk.jmh.Main"
        classpath = sourceSets["jmh"].runtimeClasspath
        // To pass parameters ("-h" gives a list of possible parameters)
        // args(listOf("-h"))
    }
}