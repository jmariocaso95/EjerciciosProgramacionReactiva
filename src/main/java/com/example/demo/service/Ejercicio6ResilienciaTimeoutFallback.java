package com.example.demo.service;

import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

/**
 * Ejercicio 6: Resiliencia - Timeout y Fallback (timeout y onErrorReturn)
 *
 * Proteger nuestro sistema de dependencias lentas estableciendo un tiempo máximo
 * de espera y un valor seguro por defecto.
 *
 * - Si el servicio no responde en 800ms, lanza TimeoutException
 * - Si hay TimeoutException, no fallamos. Devolvemos un score "seguro" de 50
 */
@Service
public class Ejercicio6ResilienciaTimeoutFallback {

    private final WebClient webClient;

    public Ejercicio6ResilienciaTimeoutFallback(WebClient externalWebClient) {
        this.webClient = externalWebClient;
    }

    /**
     * Obtiene el score de riesgo de un cliente desde servicio de antifraude
     * Si el servicio es lento (>800ms), devuelve un score seguro
     *
     * @param clienteId ID del cliente a evaluar
     * @return Score de riesgo (0-100), o 50 si timeout
     */
    public Mono<Integer> obtenerScoreRiesgo(String clienteId) {
        return webClient.post()
                .uri("/api/antifraude")
                .bodyValue(clienteId)
                .retrieve()
                .bodyToMono(Integer.class)
                // Si el servicio no responde en 800ms, lanza TimeoutException
                .timeout(Duration.ofMillis(800))
                // Si hay TimeoutException, no fallamos. Devolvemos un score "seguro" de 50
                .onErrorReturn(TimeoutException.class, 50);
    }
}
