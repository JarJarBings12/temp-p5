package ch.zhaw.prog2.io.picturedb;

import java.net.URL;
import java.util.Date;
import java.util.Objects;

/**
 * A data record for a picture found on the internet.
 * The picture is referenced by a URL, and has a title, a date, and a location (longitude and latitude).
 * It extends the abstract class {@link Datarecord}, which provides an id to identify the record in the data source.
 */
public class Picture extends Datarecord {

    private final URL url;
    private final Date date;
    private final String title;
    private final float longitude;
    private final float latitude;


    /**
     * Create a new picture record with the given data.
     * @param url URL of the picture
     * @param date  date when the picture was discovered
     * @param title title of the picture
     * @param longitude geographic longitude of the location where the picture was taken
     * @param latitude geographic latitude of the location where the picture was taken
     */
    public Picture(URL url, Date date, String title, float longitude, float latitude) {
        this.url = url;
        this.date = date;
        this.title = title;
        this.longitude = longitude;
        this.latitude = latitude;
    }

    /**
     * Simplified constructor for a new picture record with the given data.
     * The date is set to now and location is to 0,0.
     * @param url URL of the picture
     * @param title title of the picture
     */
    public Picture(URL url, String title) {
        this(url, new Date(), title, 0, 0);
    }

    /**
     * Extended constructor to create a new picture record with the given data including the data source id.
     * This constructor is used by the data source when reading records from the data source.
     * @param id unique identifier of the record
     * @param url URL of the picture
     * @param date date when the picture was discovered
     * @param title title of the picture
     * @param longitude geographic longitude of the location where the picture was taken
     * @param latitude geographic latitude of the location where the picture was taken
     */
    protected Picture(long id, URL url, Date date, String title, float longitude, float latitude) {
        this(url, date, title, longitude, latitude);
        this.id = id;
    }

    /**
     * Get the URL of the picture.
     * @return URL of the picture
     */
    public URL getUrl() {
        return url;
    }

    /**
     * Get the date when the picture was discovered.
     * @return date of the picture
     */
    public Date getDate() {
        return date;
    }

    /**
     * Get the title of the picture.
     * @return title of the picture
     */
    public String getTitle() {
        return title;
    }

    /**
     * Get the geographic longitude of the location where the picture was taken.
     * @return longitude of the location
     */
    public float getLongitude() {
        return longitude;
    }

    /**
     * Get the geographic latitude of the location where the picture was taken.
     * @return latitude of the location
     */
    public float getLatitude() {
        return latitude;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Picture picture = (Picture) o;
        return Float.compare(picture.longitude, longitude) == 0 &&
            Float.compare(picture.latitude, latitude) == 0 &&
            url.equals(picture.url) &&
            date.equals(picture.date) &&
            title.equals(picture.title);
    }

    @Override
    public int hashCode() {
        return Objects.hash(url, date, title, longitude, latitude);
    }

    @Override
    public String toString() {
        return "Picture{id=%d, url=%s, date=%s, title='%s', longitude=%s, latitude=%s}"
            .formatted(id, url, date, title, longitude, latitude);
    }

}
