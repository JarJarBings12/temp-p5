package ch.zhaw.prog2.io.picturedb;

/**
 * Exception indicating that a record could not be found in a datasource.
 * The reason why is given as a text message.
 */
public class RecordNotFoundException extends Exception {
    /**
     * Constructs a new exception with the specified detail message.
     * @param message the detail message
     */
    public RecordNotFoundException(String message) {
        super(message);
    }

    /**
     * Constructs a new exception with the specified detail message and cause.
     * @param message the detail message
     * @param cause the cause
     */
    public RecordNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a new exception with the specified cause and detail message of (cause==null ? null : cause.toString())
     * (which typically contains the class and detail message of cause).
     * @param cause the cause for this exception
     */
    public RecordNotFoundException(Throwable cause) {
        super(cause);
    }

}
