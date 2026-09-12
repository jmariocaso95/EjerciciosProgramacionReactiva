package com.example.demo.service;

import com.example.demo.comon.TransientException;
import com.example.demo.dto.CotizacionPrecio;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;

/**
 * Ejercicio 5: Resiliencia - Reintento con Backoff (retryWhen)
 *
 * Reintentar un error de red transitorio esperando tiempos cada vez más largos.
 * - 1er intento inmediato
 * - Si falla: espera 200ms, 2do intento
 * - Si falla: espera 400ms, 3er intento
 * - Si falla: espera 800ms, 4to intento
 * Solo reintenta si es un error transitorio (ErrorDeRed), no otros tipos de excepción (e.j. 404)
 */
@Service
public class Ejercicio5ResilienciaRetryBackoff {

    private final WebClient webClient;

    public Ejercicio5ResilienciaRetryBackoff(WebClient externalWebClient) {
        this.webClient = externalWebClient;
    }

    /**
     * Obtiene el precio de un producto con reintento exponencial en caso de error de red
     *
     * @param productoId ID del producto a cotizar
     * @return Mono con la cotización del precio
     */
    public Mono<CotizacionPrecio> obtenerPrecio(Long productoId) {
        return webClient.get()
                .uri("/api/precios/" + productoId)
                .retrieve()
                .bodyToMono(CotizacionPrecio.class)
                // Reintenta hasta 3 veces con backoff exponencial
                // Inicio: 200ms, luego 400ms, luego 800ms
                .retryWhen(Retry.backoff(3, Duration.ofMillis(200))
                        // Solo reintentamos si el error fue un problema de conexión transitorio
                        // No reintentamos si es un 404 u otro error de aplicación
                        .filter(ex -> ex instanceof TransientException));
    }
}
