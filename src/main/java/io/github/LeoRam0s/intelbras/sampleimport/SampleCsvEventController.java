package io.github.LeoRam0s.intelbras.sampleimport;

import io.github.LeoRam0s.intelbras.sampleimport.exceptions.CsvSampleImportException;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api")
@Tag(name = "Sample CSV", description = "Importação de eventos a partir do CSV de amostra")
public class SampleCsvEventController {
    private static final Logger log = LoggerFactory.getLogger(SampleCsvEventController.class);

    private final SampleCsvEventImportService sampleCsvEventImportService;

    public SampleCsvEventController(SampleCsvEventImportService sampleCsvEventImportService) {
        this.sampleCsvEventImportService = sampleCsvEventImportService;
    }

    @PostMapping("/read-sample-csv")
    @Operation(
            summary = "Processa o CSV de amostra",
            description = "Lê e processa o arquivo CSV de amostra e publica os eventos extraídos no Kafka."
    )
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "CSV processado com sucesso; sem corpo de resposta."),
            @ApiResponse(responseCode = "500", description = "Falha ao ler ou processar o CSV de amostra.")
    })
    public void sampleCsv() throws CsvSampleImportException {
        sampleCsvEventImportService.readSample();
    }

    @ExceptionHandler(CsvSampleImportException.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public Map<String, String> handleCsvSampleImport(Exception exception) {
        log.atError()
                .addKeyValue("outcome", "failure")
                .addKeyValue("errorType", exception.getClass().getSimpleName())
                .setCause(exception)
                .log("Failed to process the sample CSV");
        return Map.of("error", "Não foi possível processar o SampleCSV de eventos");
    }
}
