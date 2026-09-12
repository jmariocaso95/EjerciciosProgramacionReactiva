package com.example.demo.service;

import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * Ejercicio 7: Propagación Implícita de Contexto (contextWrite y deferContextual)
 *
 * Pasar variables a través de toda la cadena reactiva (ej. Correlation ID) sin
 * agregarlas a los parámetros de los métodos.
 *
 * El contexto se propaga automáticamente a través de toda la cadena Mono/Flux,
 * permitiendo que métodos profundos accedan a valores sin pasarlos explícitamente.
 */
@Service
public class Ejercicio7ContextoPropagacion {

    /**
     * Método profundo que necesita el ID de correlación
     * Lee el contexto de forma reactiva usando deferContextual
     *
     * @return Mono con resultado de la operación
     */
    public Mono<String> logOperacion() {
        // Leemos el contexto de forma reactiva
        // Si la clave no existe, usamos un valor por defecto
        return Mono.deferContextual(ctx -> {
            String correlationId = ctx.getOrDefault("X-Corr-Id", "default-id");
            System.out.println("[ " + correlationId + " ] Operación procesada");
            return Mono.just("OK");
        });
    }

    /**
     * Método en la capa alta (ej. Controlador/Filtro HTTP) que inicia la cadena
     * Escribe el Correlation ID en el contexto usando contextWrite
     * El contexto viaja "hacia abajo" cubriendo todo el flujo reactivo
     *
     * @return Mono con el resultado de la cadena completa
     */
    public Mono<String> endpointPeticion() {
        return logOperacion()
                // Escribimos en el contexto
                // Este contexto se propaga a todos los operadores downstream (abajo)
                .contextWrite(ctx -> ctx.put("X-Corr-Id", "123e4567-e89b-12d3"));
    }

    /**
     * Otro ejemplo: múltiples niveles de propagación
     */
    public Mono<String> procesoProfundo() {
        return Mono.deferContextual(ctx -> {
            String correlationId = ctx.getOrDefault("X-Corr-Id", "sin-id");
            return Mono.just("Paso 1: " + correlationId);
        }).flatMap(result -> Mono.deferContextual(ctx -> {
            String correlationId = ctx.getOrDefault("X-Corr-Id", "sin-id");
            return Mono.just(result + " -> Paso 2: " + correlationId);
        })).contextWrite(ctx -> ctx.put("X-Corr-Id", "ABC-123-XYZ"));
    }
}
