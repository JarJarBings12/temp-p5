package ch.zhaw.prog2.io.picturedb;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;

/**
 * This demo-application reads some picture data from terminal,
 * saves it to the datasource, read it from the DB and prints the result.
 */

public class PictureImport {
    private static final String PICTURE_DB = "db/picture-data.csv";
    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private final DateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
    private final PrintWriter out = new PrintWriter(System.out, true);


    /**
     * Constructor of the demo class.
     * The logger is initialized by setting the properties "java.util.logging.config.class" -> {@link LogConfiguration}
     * and "java.util.logging.config.file" -> {@code log.properties}.<br>
     * It additionally sets the log level of the {@link FilePictureDatasource}.
     */
    PictureImport() {
        // initialize logger level
        LogConfiguration.setLogLevel(FilePictureDatasource.class, Level.FINE);
    }

    /**
     * Main method to run the demo application.
     * Accepts an optional locale as argument (e.g., "en", "de"). Uses the default locale if no argument is provided.
     * @param args optional locale as argument
     * @throws IOException if the data source cannot be accessed.
     */
    public static void main(String[] args) throws IOException {
        Locale locale = (args.length > 0) ? Locale.forLanguageTag(args[0]) : Locale.getDefault();
        System.out.printf("Locale: %s (%s)%n", locale.getDisplayName(), locale.toLanguageTag());
        new PictureImport().runDemo(PICTURE_DB, locale);
    }

    private void runDemo(String pictureDB, Locale locale) throws IOException {
        // create datasource
        PictureDatasource dataSource = new FilePictureDatasource(pictureDB);
        // read picture data from the terminal
        Picture picture = createPicture();
        // save the picture to the data source
        dataSource.insert(picture);
        // read the picture back from file
        Optional<Picture> readPicture = dataSource.findById(picture.getId());
        readPicture.ifPresentOrElse(
                pict -> out.printf("The following pictures has been saved: %s%n", pict),
                () -> out.printf("Picture with id = %d not found!%n", picture.getId())
        );
        if (readPicture.isPresent()) {
            out.printf("The following pictures has been saved: %s%n", readPicture.get());
        } else {
            out.printf("Picture with id = %d not found!%n", picture.getId());
        }

        // read all pictures and list them on the console
        Collection<Picture> pictures = dataSource.findAll();
        out.println("Pictures:");
        for (Picture pict : pictures) {
            out.println(pict.toString());
        }
    }

    /**
     * Reads the picture data from the terminal.
     * Using default values, if the user enters invalid data.
     * @return the picture object
     */
    private Picture createPicture() {
        out.println("** Create a new picture **");
        URL url = null;
        do {
            String urlString = prompt("Picture URL: ");
            try {
                url = new URI(urlString).toURL();
            } catch (URISyntaxException | MalformedURLException e) {
                out.printf("Malformed URL: %s%n", e.getMessage());
            }
        } while (url == null);

        String title = prompt("Picture title: ");

        Date date = new Date(); // now
        try {
            date = dateFormat.parse(prompt("Picture time (" + DATE_FORMAT + ") Default = now: "));
        } catch (ParseException e) {
            out.printf("Unknown date format. Using default value %s%n", dateFormat.format(date));
        }

        float longitude = 0.0f;
        try {
            longitude = Float.parseFloat(prompt("Picture position longitude: "));
        } catch (NumberFormatException e) {
            out.printf("Unknown number format. Using default value %s%n", longitude);
        }

        float latitude = 0.0f;
        try {
            latitude = Float.parseFloat(prompt("Picture position latitude: "));
        } catch (NumberFormatException e) {
            out.printf("Unknown number format. Using default value %s%n", latitude);
        }

        return new Picture(url, date, title, longitude, latitude);
    }

    /**
     * Reads a string from the console.
     * @param prompt the message to be prompted before the cursor
     * @return the string entered by the user
     */
    static String prompt(String prompt) {
        try {
            Scanner scanner = new Scanner(System.in);
            System.out.print(prompt);
            System.out.flush();
            return scanner.nextLine().strip();
        } catch (NoSuchElementException e) {
            return "";
        }
    }

}

