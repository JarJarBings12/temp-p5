package ch.zhaw.prog2.io;

import java.io.*;
import java.util.*;


public class FileCopy {
    private static final String DEFAULT_SOURCE_DIR = "./files";

	public static void main(String[] args) throws IOException {

		// get the filename from the arguments. By default, use 'files'-directory in current working directory.
        String sourceDirPath = args.length >= 1 ? args[0] : DEFAULT_SOURCE_DIR;
        File sourceDir = new File(sourceDirPath);

        // Part a – Verify the directory structure
        // Implement the method 'verifySourceDir()'
        System.out.format("Verifying Source Directory: %s%n", sourceDirPath);
        try {
            verifySourceDir(sourceDir);
        } catch (FileNotFoundException error) {
            System.err.format("Directory %s does not comply with predefined structure: %s%n", sourceDir.getPath(), error.getMessage());
            System.err.println("Terminating programm!");
            System.exit(1);
        }
        System.out.println("Source Directory verified successfully.");


        // Part b – Copy the files byte resp. char wise.
        // Implement the method 'copyFiles()'
        System.out.println("Initiating file copies.");
        try {
            copyFiles(sourceDir);
        } catch (IOException error) {
            System.err.format("Error creating file copies:  %s%n", error.getMessage());
            System.err.println("Terminating programm!");
            System.exit(2);
        }
        System.out.println("Files copied successfully.");
    }

    /**
     * Part a – directory structure.
     * <p>
     * Verify the directory structure for correctness.
     * Correct means, that the directory exists and beside the two files rmz450.jpg and rmz450-spec.txt does not contain
     * any other files or directories.
     *
     * @param sourceDir File source directory to verify it contains the correct structure
     * @throws FileNotFoundException if the source directory or required file are missing
     * @throws InvalidObjectException if the source directory contains invalid files or directories.
     */
    private static void verifySourceDir(File sourceDir) throws FileNotFoundException, InvalidObjectException {
        if (sourceDir.isDirectory()) {
            System.out.format("Directory %s exists.", sourceDir.getPath());
        } else {
            throw new FileNotFoundException("Directory %s does not exist.".formatted(sourceDir.getPath()));
        }

        // mutable list of required files
        List<String> requiredFiles = new ArrayList<>(List.of("rmz450.jpg", "rmz450-spec.txt" ));

        // check all files in the directory
        for (File file : sourceDir.listFiles()) {
            // if found, remove file from required list, otherwise throw an error (invalid file)
            if (!requiredFiles.remove(file.getName())) {
                throw new InvalidObjectException("Directory %s contains invalid element %s".formatted(sourceDir.getPath(), file.getName()));
            }
            // check for valid file type
            if (file.isDirectory()) {
                throw new InvalidObjectException("File %s is a directory. Must be a standard file.".formatted(file.getName()));
            }
        }
        // verify all required files are available
        if (!requiredFiles.isEmpty()) {
            throw new FileNotFoundException("Required file(s) not found:" + String.join(", ", requiredFiles));
        }
    }


    /**
     * Part b – Copy files.
     * <p>
     * Copies each file of the source directory twice, once character-oriented and once byte-oriented.
     * Source and target files should be opened and copied byte by byte respectively char by char.
     * The target files should be named, so the type of copy can be identified.
     *
     * @param sourceDir File representing the source directory containing the files to copy
     * @throws IOException if an error is happening while copying the files
     */
    private static void copyFiles(File sourceDir) throws IOException {
        Objects.requireNonNull(sourceDir, "Source directory must not be null");
        for (File file : sourceDir.listFiles()) {
            try (FileInputStream inputStream = new FileInputStream(file);
                 FileOutputStream outputStream = new FileOutputStream("copy-bin-" + file.getName());
                 FileReader reader = new FileReader(file);
                 FileWriter writer = new FileWriter("copy-char-" + file.getName()))
            {
                int byteValue;
                System.out.println("Binary copy.");
                while ((byteValue = inputStream.read()) >= 0) {
                    outputStream.write(byteValue);
                }
                // or use: inputStream.transferTo(outputStream);
                int charValue;
                System.out.println("Character-oriented copy.");
                while ((charValue = reader.read()) >= 0) {
                    writer.write(charValue);
                }
                // or use: reader.transferTo(writer);
            }
        }
    }
}
