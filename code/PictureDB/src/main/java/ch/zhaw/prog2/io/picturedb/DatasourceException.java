package ch.zhaw.prog2.io.picturedb;

/**
 * Exception indicating that an error occurred accessing the datasource.
 * This exception could be subclassed for more specific error conditions or for specific datasource implementations.
 * The reason why is given as a text message.
 */
public class DatasourceException extends RuntimeException {
    /**
     * Constructs a new exception with the specified detail message.
     * @param message the detail message
     */
    public DatasourceException(String message) {
        super(message);
    }

    /**
     * Constructs a new exception with the specified detail message and cause.
     * @param message the detail message
     * @param cause the cause
     */
    public DatasourceException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a new exception with the specified cause and detail message of (cause==null ? null : cause.toString())
     * (which typically contains the class and detail message of cause).
     * @param cause the cause for this exception
     */
    public DatasourceException(Throwable cause) {
        super(cause);
    }

}
