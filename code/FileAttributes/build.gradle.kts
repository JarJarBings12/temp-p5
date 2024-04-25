plugins {
    application
}

// Project/Module information
description = "Lab05 FileAttributes"
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
    mainClass = "ch.zhaw.prog2.io.DirList"
}


// Task configuration
tasks.run<JavaExec> {
    // enable console input when running with gradle
    standardInput = System.`in`
}

