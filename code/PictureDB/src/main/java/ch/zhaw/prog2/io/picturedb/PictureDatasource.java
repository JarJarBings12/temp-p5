package ch.zhaw.prog2.io.picturedb;

import java.util.Collection;

/**
 * Declaration of a Datasource to store Picture records.
 * Beside the default Datasource methods, it declares an additional Picture specific query method.
 */
public interface PictureDatasource extends Datasource<Picture> {
    /**
     * Retrieves all images close to a certain position.
     * All images with a deviation from the exact coordinates are returned.
     * This includes all objects in a square range
     * from [longitude - deviation / latitude - deviation]
     * to   [longitude + deviation / latitude + deviation]
     *
     * @param longitude longitude coordinate of the center of the square area
     * @param latitude  latitude coordinate of the center of the square area
     * @param deviation deviation from the center of the area in longitude and latitude direction
     * @return Collection of all Picture records in the area
     * @throws DatasourceException if an error occurs accessing the data source
     */
    Collection<Picture> findByPosition(float longitude, float latitude, float deviation);
}
