package DataTransformer.DataTransformer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
public class DataTransformerApplication {

	public static void main(String[] args) {
		SpringApplication.run(DataTransformerApplication.class, args);
	}

}
