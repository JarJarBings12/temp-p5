package ch.zhaw.prog2.io.picturedb;

import java.util.Collection;
import java.util.Optional;

/**
 * Generic data source interface to persist items of type T extending {@link Datarecord}<br>
 * What kind of persistence media is used, is defined by the concrete implementation.
 * e.g. InMemory, Files, Database, ...<br>
 *
 * @param <T> type of the data record to be persisted. Must extend {@link Datarecord}
 */
public interface Datasource<T extends Datarecord> {

    /**
     * Insert a new record to the data source.
     * The id field of the record is ignored, and a new unique id has to be generated, which will be set in the record.
     * This id is used to identify the record in the data source by the other methods (i.e. find, update or delete)
     *
     * @param record of type T to insert into the data source.
     * @throws DatasourceException if an error occurs accessing the data source
     */
    void insert(T record);

    /**
     * Update the content of an existing record in the data source, which is identified by the unique identifier,
     * with the new values from the given record object.
     * If the identifier can not be found in the data dource, an {@link RecordNotFoundException} is thrown.
     *
     * @param record to be updated in the datasource
     * @throws RecordNotFoundException if the record is not existing
     * @throws DatasourceException if an error occurs accessing the data source
     */
    void update(T record) throws RecordNotFoundException;

    /**
     * Deletes the record, identified by the id of the given record from the data source.
     * All other fields of the record are ignored.
     * If the identifier can not be found in the data source, an {@link RecordNotFoundException} is thrown.
     *
     * @param record to be deleted form the data source
     * @throws RecordNotFoundException if the record is not existing
     * @throws DatasourceException if an error occurs accessing the data source
     */
    void delete(T record) throws RecordNotFoundException;

    /**
     * Returns the number of records in the data source.
     * @return number of records
     * @throws DatasourceException if an error occurs accessing the data source
     */
    long count();

    /**
     * Retrieves an instance of the record identified by the given id.
     * If the record can not be found, an {@link Optional#empty()} is returned.
     * (better than returning {@code null}. {@link java.util.Optional} is covered in part Functional Programming)
     * An empty result is not an error. Therefore, we do not throw an exception.
     *
     * @param id of the record to be retrieved
     * @return Optional containing data record of type T or is empty if not found
     * @throws DatasourceException if an error occurs accessing the data source
     */
    Optional<T> findById(long id);

    /**
     * Retrieves all records of the data source.
     * If the data source is empty an empty collection is returned.
     *
     * @return collection of all records of the data source
     * @throws DatasourceException if an error occurs accessing the data source
     */
    Collection<T> findAll();
}
