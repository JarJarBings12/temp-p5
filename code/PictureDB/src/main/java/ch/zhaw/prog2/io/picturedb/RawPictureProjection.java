package ch.zhaw.prog2.io.picturedb;


import java.net.MalformedURLException;
import java.net.URL;
import java.text.DateFormat;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class RawPictureProjection {
    private final DateFormat dateFormat;

    private final int pictureIdIdx;
    private final int pictureTitleIdx;
    private final int pictureLongitudeIdx;
    private final int pictureLatitudeIdx;
    private final int pictureDateIdx;
    private final int pictureUrlIdx;

    private String[] rawPicture;


    private RawPictureProjection(DateFormat dateFormat, int pictureIdIdx, int pictureUrlIdx, int pictureTitleIdx, int pictureLongitudeIdx, int pictureLatitudeIdx, int pictureDateIdx) {
        this.dateFormat             = dateFormat;

        this.pictureIdIdx           = pictureIdIdx;
        this.pictureUrlIdx          = pictureUrlIdx;
        this.pictureTitleIdx        = pictureTitleIdx;
        this.pictureLongitudeIdx    = pictureLongitudeIdx;
        this.pictureLatitudeIdx     = pictureLatitudeIdx;
        this.pictureDateIdx         = pictureDateIdx;
    }


    public void setRow(String[] split) {
        Objects.requireNonNull(split);
        this.rawPicture = split;
    }

    public String[] getRow() {
        return rawPicture;
    }


    public long selectId() {
        return Long.parseLong(checkedRawDataAccess(pictureIdIdx));
    }

    public String selectTitle() {
        return checkedRawDataAccess(pictureTitleIdx);
    }

    public URL selectUrl() throws MalformedURLException {
        return new URL(checkedRawDataAccess(pictureUrlIdx));
    }

    public float selectLongitude() {
        return Float.parseFloat(checkedRawDataAccess(pictureLongitudeIdx));
    }

    public float selectLatitude() {
        return Float.parseFloat(checkedRawDataAccess(pictureLatitudeIdx));
    }

    public Date selectDate() {
        try {
            return dateFormat.parse(checkedRawDataAccess(pictureDateIdx));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Optional<Picture> convertToPicture() {
        try {
            Picture picture = new Picture(selectId(), selectUrl(), selectDate(), selectTitle(), selectLongitude(), selectLatitude());
            return Optional.of(picture);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    public void updateRowFromPicture(Picture picture) {
        rawPicture[pictureUrlIdx]       = picture.getUrl().toString();
        rawPicture[pictureTitleIdx]     = picture.getTitle();
        rawPicture[pictureLongitudeIdx] = String.valueOf(picture.getLongitude());
        rawPicture[pictureLatitudeIdx]  = String.valueOf(picture.getLatitude());
        rawPicture[pictureDateIdx]      = dateFormat.format(picture.getDate());
    }

    private String checkedRawDataAccess(int idx) {
        if (rawPicture == null) {
            throw new IllegalStateException("Raw data not set");
        }
        return rawPicture[idx];
    }

    public static RawPictureProjection create(final DateFormat dateFormat, final List<String> header) {
        int readPictureIdIdx = header.indexOf("id");
        int readPictureUrlIdx = header.indexOf("url");
        int readPictureTitleIdx = header.indexOf("title");
        int readPictureLongitudeIdx = header.indexOf("longitude");
        int readPictureLatitudeIdx = header.indexOf("latitude");
        int readPictureDateIdx = header.indexOf("date");

        return new RawPictureProjection(dateFormat, readPictureIdIdx, readPictureUrlIdx, readPictureTitleIdx, readPictureLongitudeIdx, readPictureLatitudeIdx, readPictureDateIdx);
    }
}
