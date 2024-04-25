/*
 *  Dynamic Multi-Module project structure
 *  automatically adds each exercise as a sub-project (module)
 */
rootProject.name = rootDir.name

// dynamically add sub-projects in code folder
File(rootDir, "code").listFiles()?.forEach { includeIfSubproject(it) }

// dynamically add sub-projects in solutions folder(s) adding the "-solution" postfix
File(rootDir, "solutions-exercises").listFiles()?.forEach { includeIfSubproject(it, "solution") }
File(rootDir, "solutions-assessment").listFiles()?.forEach { includeIfSubproject(it, "solution") }

/**
 * Include directory as a subproject with the given "-postfix", if it contains a build.gradle(.kts) file.
 * @param dir directory file object to include
 * @param postfix postfix to add to the project name (optional)
 */
fun includeIfSubproject (dir : File, postfix : String = "") {
    if (dir.isDirectory &&
        (File(dir, "build.gradle.kts").exists() || File(dir, "build.gradle").exists()) ) {
        val subProjectName = ":${dir.name}${if(postfix.isBlank()) "" else "-${postfix}"}"
        include(subProjectName)
        project(subProjectName).projectDir = dir
    }
}
