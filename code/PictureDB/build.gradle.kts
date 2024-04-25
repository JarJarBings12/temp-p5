import org.gradle.api.tasks.testing.logging.TestLogEvent.*

plugins {
    application
}

// Project/Module information
description = "Lab05 PictureDB"
group = "ch.zhaw.it.prog2"
version = "2024"

// Dependency configuration
repositories {
    mavenCentral()
}

dependencies {
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.+")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

// Plugin configurations
java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
    tasks.compileJava {
        options.encoding = "UTF-8"
    }
}

application {
    mainClass = "ch.zhaw.prog2.io.picturedb.PictureImport"
}


// Task configuration
tasks.test {
    useJUnitPlatform()
    testLogging {
        events(FAILED, PASSED, SKIPPED)
    }
}


tasks.run<JavaExec> {
    // enable console input when running with gradle
    standardInput = System.`in`
    // set system property to load log configuration using class (takes precedence; if not set or fails, file is used)
    systemProperty("java.util.logging.config.class", "ch.zhaw.prog2.io.picturedb.LogConfiguration")
    // set system property to load log configuration from properties
    systemProperty("java.util.logging.config.file", "log.properties")
}

