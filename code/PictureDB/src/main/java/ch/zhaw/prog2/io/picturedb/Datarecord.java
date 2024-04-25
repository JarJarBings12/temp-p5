package ch.zhaw.prog2.io.picturedb;

/**
 * Abstract class to be used as base class for data records, which can stored in a {@link Datasource}
 * When a record is created it gets the default id of -1, indicating that it is a new record, which is not stored
 * in a DataSource.
 */
public abstract class Datarecord {
    /**
     * Default id for new records, indicating, that it is not yet stored in a datasource.
     */
    private static final long DEFAULT_ID = -1L;
    /**
     * Identifier of the record.
     */
    protected long id = DEFAULT_ID;

    /**
     * Set the id for this record. This method is used by the specific data set handler when adding the record to a
     * {@link Datasource}. It should not be used directly by the user.
     * @param id new id of the record, when the record is added to the data source
     */
    protected void setId(long id) {
        this.id = id;
    }

    /**
     * Returns the identifier of the record.
     * @return identifier of the record
     */
    public long getId() {
        return id;
    }

    /**
     * Returns the status of the record.
     * @return true, if the record is not stored to the data source, false otherwise
     */
    public boolean isNew() {
        return id == DEFAULT_ID;
    }

}
