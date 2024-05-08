package ch.zhaw.prog2.io.picturedb;

import javax.swing.plaf.basic.BasicDesktopIconUI;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.BiFunction;

/**
 * Implements the PictureDatasource Interface storing the data in
 * Character Separated Values (CSV) format, where each line consists of a record
 * whose fields are separated by the DELIMITER value ";".<br>
 * See example file: db/picture-data.csv
 */
public class FilePictureDatasource implements PictureDatasource {
    private static final List<String> HEADER_COLUMNS = List.of("id", "date", "longitude", "latitude", "title", "url");


    // Charset to use for file encoding.
    protected static final Charset CHARSET = StandardCharsets.UTF_8;
    // Delimiter to separate record fields on a line
    protected static final String DELIMITER = ";";
    // Date format to use for date specific record fields
    protected static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private final DateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
    private final File filePath;

    /**
     * Creates the FilePictureDatasource object with the given file path as datafile.
     * Creates the file if it does not exist.
     * Also creates an empty temp file for write operations.
     *
     * @param filepath of the file to use as database file.
     * @throws IOException if accessing or creating the file fails
     */
    public FilePictureDatasource(String filepath) throws IOException {
        this.filePath   = new File(filepath);
    }


    /**
     * {@inheritDoc}
     *
     */
    @Override
    public void insert(Picture picture) {
        Objects.requireNonNull(picture, "picture must not be null");

        try {
            final File tempFile = Files.createTempFile(filePath.toPath(), "db-", ".tmp").toFile();

            Files.copy(filePath.toPath(), tempFile.toPath());
            try (BufferedReader reader = new BufferedReader(new FileReader(tempFile));
                 BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile, true))) {

                List<String> header = readHeader(reader);
                final RawPictureProjection projection = RawPictureProjection.create(dateFormat, header);
                projection.setRow(new String[header.size()]);

                picture.setId(count() + 1);
                projection.updateRowFromPicture(picture);

                writer.write(String.join(DELIMITER, projection.getRow()));
            }
            replaceFile(filePath, tempFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     *
     */
    @Override
    public void update(Picture picture) throws RecordNotFoundException {
        Objects.requireNonNull(picture, "picture must not be null");

        try {
            final File tempFile = Files.createTempFile(filePath.toPath(), "db-", ".tmp").toFile();

            try (BufferedReader reader = new BufferedReader(new FileReader(filePath));
                 BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile))) {
                List<String> header = readHeader(reader);

                final RawPictureProjection projection = RawPictureProjection.create(dateFormat, header);
                copyWhile(reader, writer, projection, picture.getId(), (p, id) -> p.selectId() != id);
                projection.updateRowFromPicture(picture);

                writer.write(String.join(DELIMITER, projection.getRow()));
                reader.transferTo(writer);
            }
            replaceFile(filePath, tempFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     *
     */
    @Override
    public void delete(Picture picture) throws RecordNotFoundException {
        Objects.requireNonNull(picture, "picture must not be null");

        try {
            final File tempFile = Files.createTempFile(filePath.toPath(), "db-", ".tmp").toFile();

            try (BufferedReader reader = new BufferedReader(new FileReader(filePath));
                 BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile))) {
                List<String> header = readHeader(reader);
                final RawPictureProjection projection = RawPictureProjection.create(dateFormat, header);

                copyWhile(reader, writer, projection, picture.getId(), (p, id) -> p.selectId() != id);
                reader.transferTo(writer);
            }
            replaceFile(filePath, tempFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Copies the content of the reader to the writer until the predicate is true.
     * The line matched by the predicate is kept in the {@code projection} so it may be used after the method returns.
     *
     * @param reader to read from
     * @param writer to write to
     * @param projection to use for reading and writing
     * @param state to compare against for the predicate
     * @param predicate to determine when to stop copying
     * @param <T> type of the state
     */
    private <T> void copyWhile(BufferedReader reader, BufferedWriter writer, RawPictureProjection projection, T state, BiFunction<RawPictureProjection, T, Boolean> predicate) {
        try {
            String line = reader.readLine();
            while (line != null) {
                final String[] rawRow = line.split(DELIMITER);

                projection.setRow(rawRow);
                if (predicate.apply(projection, state)) {
                    break;
                } else {
                    writer.write(line);
                }
                line = reader.readLine();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     *
     */
    @Override
    public long count() {
        // ToDo: Correct Implementation
        return 0;
    }

    /**
     * {@inheritDoc}
     *
     */
    @Override
    public Optional<Picture> findById(long id) {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            List<String> header = readHeader(reader);
            final RawPictureProjection projection = RawPictureProjection.create(dateFormat, header);

            Picture picture = null;
            String line = reader.readLine();
            while(line != null && picture == null) {
                projection.setRow(line.split(DELIMITER));
                if (projection.selectId() == id) {
                    picture = projection.convertToPicture().orElseThrow();
                }
                line = reader.readLine();
            }
            return Optional.ofNullable(picture);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     *
     */
    @Override
    public Collection<Picture> findAll() {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            List<String> header = readHeader(reader);
            final RawPictureProjection projector = RawPictureProjection.create(dateFormat, header);

            ArrayList<Picture> pictures = new ArrayList<>();
            String line = reader.readLine();
            while(line != null) {
                projector.setRow(line.split(DELIMITER));

                Optional<Picture> picture = projector.convertToPicture();
                picture.ifPresent(pictures::add);
                line = reader.readLine();
            }
            return pictures;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void replaceFile(File original, File newFile) {
        if (!original.delete())
            throw new IllegalStateException("Couldn't delete file: " + original);
        if (!newFile.renameTo(original))
            throw new IllegalStateException("Couldn't rename file: " + newFile + " to " + original);
    }

    /**
     * {@inheritDoc}
     *
     */
    @Override
    public Collection<Picture> findByPosition(float longitude, float latitude, float deviation) {
        // ToDo: Correct Implementation
        return Collections.emptyList();
    }

    private List<String> readHeader(BufferedReader reader) throws IOException {
        return Collections.unmodifiableList(HEADER_COLUMNS);
        //String line = readNextNoneEmptyLine(reader);
        //final List<String> splits = Collections.unmodifiableList(Arrays.asList(line.split(DELIMITER)));
//
        //if (!splits.containsAll(HEADER_COLUMNS)) {
        //    throw new IllegalArgumentException("header is missing required columns");
        //}
        //return splits;
    }

    private String readNextNoneEmptyLine(BufferedReader reader) throws IOException {
        String line;
        do {
            line = reader.readLine();
        } while (line != null && line.trim().isEmpty());

        if (line == null)
            throw new IOException("Couldn't find non empty line");
        return line;
    }
}
