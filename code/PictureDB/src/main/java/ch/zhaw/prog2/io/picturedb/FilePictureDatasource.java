package ch.zhaw.prog2.io.picturedb;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.BiFunction;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implements the PictureDatasource Interface storing the data in
 * Character Separated Values (CSV) format, where each line consists of a record
 * whose fields are separated by the DELIMITER value ";".<br>
 * See example file: db/picture-data.csv
 */
public class FilePictureDatasource implements PictureDatasource {
    private static final Logger LOGGER = Logger.getLogger(FilePictureDatasource.class.getName());
    private static final List<String> HEADER_COLUMNS = List.of("id", "date", "longitude", "latitude", "title", "url");


    // Charset to use for file encoding.
    protected static final Charset CHARSET = StandardCharsets.UTF_8;
    // Delimiter to separate record fields on a line
    protected static final String DELIMITER = ";";
    // Date format to use for date specific record fields
    protected static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private final DateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
    private final File databaseFile;

    /**
     * Creates the FilePictureDatasource object with the given file path as datafile.
     * Creates the file if it does not exist.
     * Also creates an empty temp file for write operations.
     *
     * @param filepath of the file to use as database file.
     * @throws IOException if accessing or creating the file fails
     */
    public FilePictureDatasource(String filepath) throws IOException {
        this.databaseFile = new File(filepath);
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void insert(Picture picture) {
        Objects.requireNonNull(picture, "picture must not be null");

        try {
            final File parent = new File(databaseFile.getParent());
            final File tempFile = Files.createTempFile(parent.toPath(), "db-", ".tmp").toFile();

            LOGGER.finer("Opening db file at '%s'".formatted(databaseFile));
            LOGGER.finer("Opening temp file at '%s'".formatted(tempFile));
            try (FileOutputStream oldFileReader = new FileOutputStream(tempFile);
                 BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(tempFile, true))) {
                Files.copy(databaseFile.toPath(), oldFileReader);

                final RawPictureProjection projection = RawPictureProjection.create(dateFormat, HEADER_COLUMNS);
                projection.setRow(new String[HEADER_COLUMNS.size()]);

                picture.setId(getHighestId() + 1);
                projection.updateRowFromPicture(picture);

                bufferedWriter.write(String.join(DELIMITER, projection.getRow()));
                bufferedWriter.write(System.lineSeparator());
            } finally {
                LOGGER.finer("Closing db file...");
                LOGGER.finer("Closing temp file...");
            }
            replaceFile(databaseFile, tempFile);
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, "An error occurred while inserting entry.", ex);
            throw new DatasourceException("Error while inserting record", ex);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void update(Picture picture) throws RecordNotFoundException {
        Objects.requireNonNull(picture, "picture must not be null");

        try {
            final File parent = new File(databaseFile.getParent());
            final File tempFile = Files.createTempFile(parent.toPath(), "db-", ".tmp").toFile();

            boolean didPredicateMatch;
            try (BufferedReader reader = new BufferedReader(new FileReader(databaseFile));
                 BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile))) {

                final RawPictureProjection projection = RawPictureProjection.create(dateFormat, HEADER_COLUMNS);
                // predicateDidNotMatch is the best name I could come up with.
                // The Method copyWhile returns the result of the last predicate invocation.
                // This means if predicateDidNotMatch is true, we didn't find the record to update and if it's false
                // we found the record.
                LOGGER.fine("Copy while looking for id '%d'".formatted(picture.getId()));
                didPredicateMatch = !copyWhile(reader, writer, projection, picture.getId(), (p, id) -> p.selectId() != id);
                if (didPredicateMatch) {
                    LOGGER.fine("Found id '%d'; Updating entry and writing it back into the data file.");
                    projection.updateRowFromPicture(picture);
                    writer.write(String.join(DELIMITER, projection.getRow()));
                    writer.write(System.lineSeparator());
                    LOGGER.fine("Transferring left over data.");
                    reader.transferTo(writer);
                }
            } finally {
                LOGGER.finer("Closing db file...");
                LOGGER.finer("Closing temp file...");
            }

            if (didPredicateMatch) {
                replaceFile(databaseFile, tempFile);
            } else {
                if (!tempFile.delete())
                    LOGGER.warning("Couldn't delete temp file: " + tempFile);
                throw new RecordNotFoundException("Record not found: " + picture.getId());
            }
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, "An exception occurred while updating record.");
            throw new DatasourceException("Error while updating record", ex);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(Picture picture) throws RecordNotFoundException {
        Objects.requireNonNull(picture, "picture must not be null");

        try {
            final File parent = new File(databaseFile.getParent());
            final File tempFile = Files.createTempFile(parent.toPath(), "db-", ".tmp").toFile();

            boolean didPredicateMatch;
            LOGGER.finer("Opening db file at '%s'".formatted(databaseFile));
            LOGGER.finer("Opening temp file at '%s'".formatted(tempFile));
            try (BufferedReader reader = new BufferedReader(new FileReader(databaseFile));
                 BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile))) {
                final RawPictureProjection projection = RawPictureProjection.create(dateFormat, HEADER_COLUMNS);

                LOGGER.fine("Copy while looking for id '%d'.".formatted(picture.getId()));
                didPredicateMatch = !copyWhile(reader, writer, projection, picture.getId(), (p, id) -> p.selectId() != id);
                if (didPredicateMatch) {
                    LOGGER.fine("Found id '%s'. Transferring left over data.".formatted(picture.getId()));
                    reader.transferTo(writer);
                }
            } finally {
                LOGGER.finer("Closing db file...");
                LOGGER.finer("Closing temp file...");
            }

            if (didPredicateMatch) {
                LOGGER.info("Renaming '%s' to '%s'".formatted(tempFile, databaseFile));
                replaceFile(databaseFile, tempFile);
            } else {
                LOGGER.info("Couldn't find id '%d'. Deleting temp file...".formatted(picture.getId()));
                if (!tempFile.delete())
                    LOGGER.warning("Couldn't delete temp file: " + tempFile);
                throw new RecordNotFoundException("Record not found: " + picture.getId());
            }
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, "An exception occurred while trying to delete record", ex);
            throw new DatasourceException("Error while deleting record", ex);
        }

    }

    /**
     * Copies the content of the reader to the writer until the predicate is true.
     * The line matched by the predicate is kept in the {@code projection} so it may be used after the method returns.
     *
     * @param reader     to read from
     * @param writer     to write to
     * @param projection to use for reading and writing
     * @param state      to compare against for the predicate
     * @param predicate  to determine when to stop copying
     * @param <T>        type of the state
     * @return true if the {@see predicate} was read to the end of the file, false otherwise
     */
    private <T> boolean copyWhile(BufferedReader reader, BufferedWriter writer, RawPictureProjection projection, T state, BiFunction<RawPictureProjection, T, Boolean> predicate) throws IOException {
        boolean foundEntry = true;
        String line = reader.readLine();
        while (line != null && foundEntry) {
            final String[] rawRow = line.split(DELIMITER);

            projection.setRow(rawRow);
            foundEntry = predicate.apply(projection, state);
            if (foundEntry) {
                writer.write(line);
                writer.write(System.lineSeparator());
                line = reader.readLine();
            }
        }
        return foundEntry;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long count() {
        long count = 0;
        LOGGER.finer("Opening db file at '%s'".formatted(databaseFile));
        try (BufferedReader reader = new BufferedReader(new FileReader(databaseFile))) {
            while (reader.readLine() != null) {
                count++;
            }
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, "Failed to process db file", ex);
            throw new DatasourceException("Error while counting records", ex);
        } finally {
            LOGGER.finer("Closing db file...");
        }
        return count;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<Picture> findById(long id) {
        LOGGER.finer("Opening db file at '%s'".formatted(databaseFile));
        try (BufferedReader reader = new BufferedReader(new FileReader(databaseFile))) {
            final RawPictureProjection projection = RawPictureProjection.create(dateFormat, HEADER_COLUMNS);

            Picture picture = null;
            String line = reader.readLine();
            while (line != null && picture == null) {
                projection.setRow(line.split(DELIMITER));
                if (projection.selectId() == id) {
                    picture = projection.convertToPicture().orElseThrow();
                }
                line = reader.readLine();
            }
            return Optional.ofNullable(picture);
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, "Failed to process db file", ex);
            throw new DatasourceException("Error while reading records", ex);
        } finally {
            LOGGER.finer("Closing db file...");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<Picture> findAll() {
        try (BufferedReader reader = new BufferedReader(new FileReader(databaseFile))) {
            final RawPictureProjection projector = RawPictureProjection.create(dateFormat, HEADER_COLUMNS);

            ArrayList<Picture> pictures = new ArrayList<>();
            String line = reader.readLine();
            while (line != null) {
                projector.setRow(line.split(DELIMITER));

                Optional<Picture> picture = projector.convertToPicture();
                picture.ifPresent(pictures::add);
                line = reader.readLine();
            }
            return pictures;
        } catch (IOException e) {
            throw new DatasourceException("Error while reading records", e);
        }
    }

    private void replaceFile(File original, File newFile) {
        LOGGER.fine("Deleting original file.");
        if (!original.delete()) {
            LOGGER.severe("Couldn't delete original file: " + original);
            throw new IllegalStateException("Couldn't delete file: " + original);
        }
        LOGGER.fine("Renaming '%s' to '%s'".formatted(newFile, original));
        if (!newFile.renameTo(original)) {
            LOGGER.severe("Couldn't rename file: %s to %s!%n".formatted(newFile, original));
            throw new IllegalStateException("Couldn't rename file: '%s' to '%s'".formatted(newFile, original));
        }
    }

    private long getHighestId() {
        try (BufferedReader reader = new BufferedReader(new FileReader(databaseFile))) {
            final RawPictureProjection projection = RawPictureProjection.create(dateFormat, HEADER_COLUMNS);

            String line = reader.readLine();
            long highestId = -1;
            while (line != null) {
                projection.setRow(line.split(DELIMITER));
                if (projection.selectId() > highestId) {
                    highestId = projection.selectId();
                }
                line = reader.readLine();
            }
            return highestId;
        } catch (IOException e) {
            throw new DatasourceException("Error while reading records", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<Picture> findByPosition(float longitude, float latitude, float deviation) {
        try (BufferedReader reader = new BufferedReader(new FileReader(databaseFile))) {
            List<Picture> results = new ArrayList<>();

            final RawPictureProjection projection = RawPictureProjection.create(dateFormat, HEADER_COLUMNS);

            String line = reader.readLine();
            while (line != null) {
                projection.setRow(line.split(DELIMITER));
                if (testCoordinates(longitude, latitude, deviation, projection.selectLongitude(), projection.selectLatitude())) {
                    results.add(projection.convertToPicture().orElseThrow());
                }
                line = reader.readLine();
            }
            return results;
        } catch (IOException e) {
            throw new DatasourceException("Error while reading records", e);
        }
    }

    /**
     * Retrieves all images close to a certain position.
     * All images with a deviation from the exact coordinates are returned.
     * This includes all objects in a square range
     * from [longitude - deviation / latitude - deviation]
     * to   [longitude + deviation / latitude + deviation]
     */
    private boolean testCoordinates(float longitude, float latitude, float deviation, float testLongitude, float testLatitude) {
        return testLongitude >= longitude - deviation && testLongitude <= longitude + deviation
                && testLatitude >= latitude - deviation && testLatitude <= latitude + deviation;
    }
}
