package ch.zhaw.prog2.io.picturedb;


import java.net.MalformedURLException;
import java.net.URL;
import java.text.DateFormat;
import java.util.Date;
import java.util.List;
import java.util.Optional;

public class RawPictureProjection {
    private final DateFormat dateFormat;

    private final int pictureIdIdx;
    private final int pictureTitleIdx;
    private final int pictureLongitudeIdx;
    private final int pictureLatitudeIdx;
    private final int pictureDateIdx;
    private final int pictureUrlIdx;

    private String[] rawPicture;


    private RawPictureProjection(DateFormat dateFormat, int pictureIdIdx, int pictureDescriptionIdx, int pictureTitleIdx, int pictureFilenameIdx, int pictureLongitudeIdx, int pictureLatitudeIdx, int pictureDateIdx) {
        this.dateFormat             = dateFormat;
        this.pictureIdIdx           = pictureIdIdx;
        this.pictureUrlIdx          = pictureDescriptionIdx;
        this.pictureTitleIdx        = pictureTitleIdx;
        this.pictureLongitudeIdx    = pictureLongitudeIdx;
        this.pictureLatitudeIdx     = pictureLatitudeIdx;
        this.pictureDateIdx         = pictureDateIdx;
    }


    public long selectId() {
        return Long.parseLong(rawPicture[pictureIdIdx]);
    }

    public String getTitle() {
        return rawPicture[pictureTitleIdx];
    }

    public URL getUrl() throws MalformedURLException {
        return new URL(rawPicture[pictureUrlIdx]);
    }

    public float getLongitude() {
        return Float.parseFloat(rawPicture[pictureLongitudeIdx]);
    }

    public float getLatitude() {
        return Float.parseFloat(rawPicture[pictureLatitudeIdx]);
    }

    public Date getDate() {
        try {
            return dateFormat.parse(rawPicture[pictureDateIdx]);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public Optional<Picture> convertToPicture() {
        try {
            Picture picture = new Picture(selectId(), getUrl(), getDate(), getTitle(), getLongitude(), getLatitude());
            return Optional.of(picture);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    public String convertToCsvRow(Picture picture) {

    }

    public void setRow(String[] split) {

    }


    public static RawPictureProjection create(final DateFormat dateFormat, final List<String> header) {
        int readPictureIdIdx = header.indexOf("id");
        int readPictureDescriptionIdx = header.indexOf("description");
        int readPictureTitleIdx = header.indexOf("title");
        int readPictureFilenameIdx = header.indexOf("filename");
        int readPictureLongitudeIdx = header.indexOf("longitude");
        int readPictureLatitudeIdx = header.indexOf("latitude");
        int readPictureDateIdx = header.indexOf("date");

        return new RawPictureProjection(dateFormat, readPictureIdIdx, readPictureDescriptionIdx, readPictureTitleIdx, readPictureFilenameIdx, readPictureLongitudeIdx, readPictureLatitudeIdx, readPictureDateIdx);
    }
}
