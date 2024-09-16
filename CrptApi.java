package com.github.zigcat.kafka_test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class CrptApi {
    private final TimeUnit timeUnit;
    private final int requestLimit;
    private final ObjectMapper objectMapper;
    private final HttpClient http;
    private final Semaphore semaphore;
    private final ScheduledExecutorService scheduledExecutor;

    public CrptApi(TimeUnit timeUnit, int requestLimit) {
        this.timeUnit = timeUnit;
        this.requestLimit = requestLimit;
        this.http = HttpClient.newHttpClient();
        this.objectMapper = new ObjectMapper();
        this.semaphore = new Semaphore(requestLimit);
        this.scheduledExecutor = Executors.newScheduledThreadPool(1);

        scheduledExecutor.scheduleAtFixedRate(this::resetLimit,
                0,
                1,
                timeUnit);
    }

    public void createDocument(Document document, String token) throws InterruptedException, IOException {
        semaphore.acquire();
        try {
            String body = objectMapper.writeValueAsString(document);
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("https://ismp.crpt.ru/api/v3/lk/documents/create"))
                    .header("Content-Type", "application/json")
                    .header("Authorization", "Bearer " + token)
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            if(response.statusCode() != 200){
                throw new IOException("Error while sending request, status: "+response.statusCode());
            }
            System.out.println("Document created");
        } catch (JsonProcessingException e) {
            System.out.println("Error while serializing object");
        } finally {
            semaphore.release();
        }
    }

    public void resetLimit(){
        semaphore.release(requestLimit - semaphore.availablePermits());
    }

    public void shutdown(){
        scheduledExecutor.shutdown();
    }

    record Description(
            String participantInn
    ){}

    record Product(
            String certificate_document,
            String certificate_document_date,
            String certificate_document_number,
            String owner_inn,
            String producer_inn,
            String production_date,
            String tnved_code,
            String uit_code,
            String uitu_code
    ){}

    record Document(
            Description description,
            String doc_id,
            String doc_status,
            String doc_type,
            boolean importRequest,
            String owner_inn,
            String participant_inn,
            String producer_inn,
            String production_date,
            String production_type,
            Product[] products,
            String reg_date,
            String reg_number
    ){}
}
