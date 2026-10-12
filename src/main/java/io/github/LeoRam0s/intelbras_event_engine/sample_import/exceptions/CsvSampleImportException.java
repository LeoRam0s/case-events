package io.github.LeoRam0s.intelbras_event_engine.sample_import.exceptions;

public class CsvSampleImportException extends RuntimeException {
    public CsvSampleImportException(String message) {
        super(message);
    }

    public CsvSampleImportException(String message, Throwable cause) {
        super(message, cause);
    }
}
