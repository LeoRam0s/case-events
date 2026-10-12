package io.github.LeoRam0s.intelbras_event_engine.sample_import;

import io.github.LeoRam0s.intelbras_event_engine.sample_import.exceptions.CsvSampleImportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api")
public class SampleCsvEventController {
    private static final Logger log = LoggerFactory.getLogger(SampleCsvEventController.class);

    private final SampleCsvEventImportService sampleCsvEventImportService;

    public SampleCsvEventController(SampleCsvEventImportService sampleCsvEventImportService) {
        this.sampleCsvEventImportService = sampleCsvEventImportService;
    }

    @PostMapping("/read-sample-csv")
    public void sampleCsv() throws CsvSampleImportException {
        sampleCsvEventImportService.readSample();
    }

    @ExceptionHandler(CsvSampleImportException.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public Map<String, String> handleCsvSampleImport(Exception exception) {
        log.error("Failed to process the sample CSV", exception);
        return Map.of("error", "Não foi possível processar o SampleCSV de eventos");
    }
}
