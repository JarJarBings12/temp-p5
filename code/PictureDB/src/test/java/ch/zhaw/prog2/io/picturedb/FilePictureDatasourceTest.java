package ch.zhaw.prog2.io.picturedb;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.stream.Stream;

import static ch.zhaw.prog2.io.picturedb.FilePictureDatasource.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class FilePictureDatasourceTest {
    public static final long NO_ID = -1L;
    public static final long HIGHEST_ID = 14L;
    private static final long EXISTING_ID = 13L;
    private static final long INEXISTENT_ID = 0L;
    public static final String TEST_PICTURE_URL = "http://test.url/hallo.img";
    public static final String TEST_PICTURE_TITLE = "Test picture";


    private final DateFormat df = new SimpleDateFormat(DATE_FORMAT);
    private final Random random = new Random(new Date().getTime());

    /*
     * Static class configuration.
     * Only executed once when class is loaded.
     */
    static {
        // logger configuration
        try {
            // show log messages in english
            Locale.setDefault(Locale.ROOT);
            // load default minimal log configuration
            LogManager.getLogManager().readConfiguration(ClassLoader.getSystemResourceAsStream("log.properties"));
            // set level for class to test to WARNING (minimize log messages)
            Logger.getLogger(FilePictureDatasource.class.getCanonicalName()).setLevel(Level.WARNING);
        } catch (IOException e) {
            System.err.println("Failed to read log configuration: " + e.getMessage());
        }
    }


    Path dbTemplatePath;    // path of template database
    Path dbPath;            // path of temporary test database

    PictureDatasource datasource = null; // datasource instance to test

    FilePictureDatasourceTest() {
        URL dbTemplateUrl = FilePictureDatasourceTest.class.getClassLoader().getResource("db");
        Objects.requireNonNull(dbTemplateUrl, "Test database directory not found");
        String dbDir = new File(dbTemplateUrl.getPath()).getAbsolutePath(); // for Windows to remove leading '/'
        String dbDirRaw = URLDecoder.decode(dbDir, CHARSET);  // replace urlencoded characters, e.g. %20 -> " "
        dbTemplatePath = Path.of(dbDirRaw, "test-data-template.csv");
        dbPath = Path.of(dbDirRaw, "test-data.csv");
    }


    @BeforeEach
    void setUp() throws IOException {
        // initialize test database file
        Files.copy(dbTemplatePath, dbPath);
        // setup datasource
        datasource = new FilePictureDatasource(dbPath.toString());
    }

    @AfterEach
    void tearDown() throws IOException {
        // cleanup test database file
        Files.deleteIfExists(dbPath);
    }


    @Test
    void insert() {
        Picture testPicture = createPicture(TEST_PICTURE_URL, TEST_PICTURE_TITLE);
        assertEquals(NO_ID, testPicture.getId(), "New picture must have Id " + NO_ID);
        datasource.insert(testPicture);
        assertNotEquals(NO_ID, testPicture.getId(), "Insert must set new id to picture");
        assertTrue(testPicture.getId() > HIGHEST_ID, "Id must be larger than last existing");
        assertEquals(HIGHEST_ID + 1, testPicture.getId(), "Id must be 1 larger than current highest");
        try {
            String insertedLine = readLineNo(4);
            assertNotNull(insertedLine, "Inserted line does not exist");
            assertEquals(pictureToCsvLine(testPicture), insertedLine);
        } catch (IOException e) {
            fail("Failed reading inserted picture record");
        }
    }

    @Test
    void insertNull() {
        assertThrows(NullPointerException.class, () -> datasource.insert(null));
    }

    @Test
    void delete() {
        long  pictureIdToDelete = EXISTING_ID;
        Optional<Picture> pictureToDelete = datasource.findById(pictureIdToDelete);
        assumeTrue(pictureToDelete.isPresent(), "Picture to delete not found");

        try {
            datasource.delete(pictureToDelete.get());
        } catch (RecordNotFoundException e) {
            fail("Failed to delete picture", e);
        }

        try {
            String deletedRecord = readLineWithId(pictureIdToDelete);
            assertNull(deletedRecord, "Datarecord still found after delete");
        } catch (IOException e) {
            fail("Failed to read deleted record");
        }
    }

    @Test
    void deleteNull() {
        assertThrows(NullPointerException.class, () -> datasource.delete(null));
    }

    @Test
    void deleteInexistent() {
        Picture testPicture = createPicture(TEST_PICTURE_URL, TEST_PICTURE_TITLE);
        testPicture.setId(INEXISTENT_ID);
        assertThrows(RecordNotFoundException.class, () -> datasource.delete(testPicture));
    }

    @Test
    void update() {
        Optional<Picture> originalOptional = datasource.findById(EXISTING_ID);
        assumeTrue(originalOptional.isPresent(), "Picture to update not found");
        Picture originalPicture = originalOptional.get();
        assumeTrue(originalPicture.id == EXISTING_ID);

        Date updatedDate = new Date(originalPicture.getDate().getTime() + 60_000);
        URL updatedURL = originalPicture.getUrl();
        try {
            updatedURL = new URI(originalPicture.getUrl().toExternalForm() + "/updated").toURL();
        } catch (URISyntaxException | MalformedURLException e) {
            fail("Invalid URL format", e);
        }
        Picture updatedPicture = new Picture(
            originalPicture.id,
            updatedURL,
            updatedDate,
            originalPicture.getTitle() + " (updated)",
            originalPicture.getLongitude() + 10,
            originalPicture.getLatitude() + 10);

        try {
            datasource.update(updatedPicture);
        } catch (RecordNotFoundException e) {
            fail("Test record not found for update", e);
        }

        Optional<Picture> readUpdatedOptional = datasource.findById(originalPicture.getId());
        assertTrue(readUpdatedOptional.isPresent(), "updated picture not found");
        Picture readUpdatedPicture = readUpdatedOptional.get();
        assertEquals(updatedPicture.id, readUpdatedPicture.id);
        assertEquals(updatedPicture.getUrl(), readUpdatedPicture.getUrl());
        assertEquals(updatedPicture.getDate(), readUpdatedPicture.getDate());
        assertEquals(updatedPicture.getTitle(), readUpdatedPicture.getTitle());
        assertEquals(updatedPicture.getLatitude(), readUpdatedPicture.getLatitude());
        assertEquals(updatedPicture.getLongitude(), readUpdatedPicture.getLongitude());
    }

    @Test
    void updateNull() {
        assertThrows(NullPointerException.class, () -> datasource.update(null));
    }

    @Test
    void updateInexistent() {
        Picture testPicture = createPicture(TEST_PICTURE_URL, TEST_PICTURE_TITLE);
        testPicture.setId(INEXISTENT_ID);
        assertThrows(RecordNotFoundException.class, () -> datasource.update(testPicture));
    }

    @Test
    void count() {
        assertEquals(3, datasource.count(), "Count for initial datasource not correct");
        datasource.insert(createPicture(TEST_PICTURE_URL, TEST_PICTURE_TITLE));
        assertEquals(4, datasource.count(), "Count for updated datasource not correct");
    }

    @Test
    void findById() {
        Optional<Picture> foundOptional = datasource.findById(EXISTING_ID);
        assertTrue(foundOptional.isPresent(), "Picture not found");
        Picture foundPicture = foundOptional.get();
        assertEquals("2013-05-07 13:45:13", df.format(foundPicture.getDate()));
        assertEquals("http://blog.stackoverflow.com/wp-content/uploads/code_monkey_colour.jpg",
            foundPicture.getUrl().toExternalForm());
        assertEquals("Need a coder", foundPicture.getTitle());
        assertEquals(-71.098270, foundPicture.getLongitude(), 0.00001);
        assertEquals(42.302583, foundPicture.getLatitude(), 0.00001);
    }

    @Test
    void findByIdInexistent() {
        Optional<Picture> foundPicture = datasource.findById(INEXISTENT_ID);
        assertFalse(foundPicture.isPresent(), "Inexistent Id found: " + INEXISTENT_ID);
    }

    @Test
    void findAll() {
        Collection<Picture> pictures = datasource.findAll();
        assertNotNull(pictures, "Collection of pictures must not be null");
        assertEquals(countLines(), pictures.size(), "Number of records does not match number of found pictures");
        for (Picture picture : pictures) {
            assertNotNull(picture, "Found <null> picture in collection");
        }
    }

    @Test
    void findByPosition() {
        Collection<Picture> pictures = datasource.findByPosition(-75, 41, 4);
        assertNotNull(pictures);
        assertEquals(2, pictures.size(), "Not correct amount of items found at position");

        pictures = datasource.findByPosition(55, 23, 1);
        assertEquals(0, pictures.size(), "Found items not to be found");
    }


    /*
     * Helper methods
     */
    private Picture createPicture(String url, String title) {
        URL testURL = null;
        try {
            testURL = new URI(url).toURL();
        } catch (URISyntaxException | MalformedURLException e) {
            fail("Invalid URL format", e);
        }
        float longitude = -180 + random.nextFloat() * 360; // range [-180..+180[
        float latitude =   -90 + random.nextFloat() * 180; // range [-90..+90[
        return new Picture(testURL, new Date(), title, longitude, latitude);
    }

    private String readLineNo(int lineNo) throws IOException {
        try (Stream<String> lineStream = Files.lines(dbPath, CHARSET)) {
            return lineStream.skip(lineNo - 1).findFirst().orElse(null);
        }
    }

    private String readLineWithId(long id) throws IOException {
        try (Stream<String> lineStream = Files.lines(dbPath, CHARSET)) {
            return lineStream.filter(line -> line.strip().startsWith(id + DELIMITER)).findFirst().orElse(null);
        }
    }

    private long countLines() {
        try (Stream<String> lineStream = Files.lines(dbPath, CHARSET)) {
            return lineStream.filter(Predicate.not(String::isBlank)).count();
        } catch (IOException e) {
            fail("Failed to count lines in db file", e);
        }
        return 0;
    }

    private String pictureToCsvLine(Picture picture) {
        assertNotNull(picture, "Picture must not be null");
        return String.join(DELIMITER,
            String.valueOf(picture.getId()),
            df.format(picture.getDate()),
            String.valueOf(picture.getLongitude()),
            String.valueOf(picture.getLatitude()),
            picture.getTitle(),
            picture.getUrl().toExternalForm()
        );
    }
}
