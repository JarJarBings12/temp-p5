plugins {
    application
}

// Project/Module information
description = "Lab05 ByteCharStream"
group = "ch.zhaw.it.prog2"
version = "2024"

// Dependency configuration
repositories {
    mavenCentral()
}

dependencies {

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
    mainClass = "ch.zhaw.prog2.io.FileCopy"
}


// Task configuration
tasks.run<JavaExec> {
    // enable console input when running with gradle
    standardInput = System.`in`
}

