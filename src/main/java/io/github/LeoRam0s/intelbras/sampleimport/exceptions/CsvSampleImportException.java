package io.github.LeoRam0s.intelbras.sampleimport.exceptions;

public class CsvSampleImportException extends RuntimeException {
    public CsvSampleImportException(String message) {
        super(message);
    }

    public CsvSampleImportException(String message, Throwable cause) {
        super(message, cause);
    }
}
