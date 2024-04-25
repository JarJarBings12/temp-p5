package ch.zhaw.prog2.io.picturedb;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Implements the PictureDatasource Interface storing the data in
 * Character Separated Values (CSV) format, where each line consists of a record
 * whose fields are separated by the DELIMITER value ";".<br>
 * See example file: db/picture-data.csv
 */
public class FilePictureDatasource implements PictureDatasource {
    // Charset to use for file encoding.
    protected static final Charset CHARSET = StandardCharsets.UTF_8;
    // Delimiter to separate record fields on a line
    protected static final String DELIMITER = ";";
    // Date format to use for date specific record fields
    protected static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private final DateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);

    /**
     * Creates the FilePictureDatasource object with the given file path as datafile.
     * Creates the file if it does not exist.
     * Also creates an empty temp file for write operations.
     *
     * @param filepath of the file to use as database file.
     * @throws IOException if accessing or creating the file fails
     */
    public FilePictureDatasource(String filepath) throws IOException {
        // ToDo: Implement
    }


    /**
     * {@inheritDoc}
     *
     */
    @Override
    public void insert(Picture picture) {
        // ToDo: Implement
    }

    /**
     * {@inheritDoc}
     *
     */
    @Override
    public void update(Picture picture) throws RecordNotFoundException {
        // ToDo: Implement
    }

    /**
     * {@inheritDoc}
     *
     */
    @Override
    public void delete(Picture picture) throws RecordNotFoundException {
        // ToDo: Implement
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
        // ToDo: Correct Implementation
        return Optional.empty();
    }

    /**
     * {@inheritDoc}
     *
     */
    @Override
    public Collection<Picture> findAll() {
        // ToDo: Correct Implementation
        return Collections.emptyList();
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

}
