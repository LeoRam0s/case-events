package io.github.LeoRam0s.intelbras_event_engine;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@OpenAPIDefinition(info = @Info(
		title = "Intelbras Event Engine API",
		version = "1.0.0",
		description = "API responsável pela simulação de entrada e processamento assíncrono de eventos de dispositivos através do Apache Kafka."
))
public class IntelbrasEventEngineApplication {

	public static void main(String[] args) {
		SpringApplication.run(IntelbrasEventEngineApplication.class, args);
	}

}
