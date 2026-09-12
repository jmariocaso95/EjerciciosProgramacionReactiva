package com.example.demo.service;

import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

/**
 * Ejercicio 15: Evitar Sondeos Repetidos (distinctUntilChanged)
 *
 * Un ciclo infinito consulta la base de datos cada 10 segundos buscando stock bajo,
 * pero solo emite si los productos afectados cambian respecto al último sondeo.
 *
 * Sin distinctUntilChanged: emitiría 60 veces por hora aunque los datos sean iguales
 * Con distinctUntilChanged: emite solo cuando hay cambios en la lista
 *
 * Casos de uso:
 * - Monitoreo de alertas (solo notificar cambios)
 * - Evitar actualizaciones innecesarias a UI
 * - Reducir carga en listeners
 */
@Service
public class Ejercicio15DistintoHastaUnCambio {

    /**
     * Monitorea stock bajo cada 10 segundos
     * Solo emite cuando la lista de productos afectados cambia
     *
     * @return Flux de alertas cuando hay cambios en productos con stock bajo
     */
    public Flux<Alerta> monitorStockBajo() {
        return Flux.interval(Duration.ZERO, Duration.ofSeconds(10))
                // Consulta a BD que retorna lista de IDs
                .concatMap(tick -> buscarProductosConStockBajo())
                // Bloquea la emisión si la lista de IDs es exactamente igual a la de hace 10 segundos
                .distinctUntilChanged()
                // Descarta si la lista está vacía (todo ok)
                .filter(lista -> !lista.isEmpty())
                .map(Alerta::new);
    }

    /**
     * Consulta simulada a BD
     * En producción, esto sería un repository o llamada a servicio
     *
     * @return Mono con lista de IDs de productos con stock bajo
     */
    private Mono<List<Long>> buscarProductosConStockBajo() {
        // En este ejemplo siempre retorna los mismos IDs
        // En producción variaría según el stock real
        return Mono.just(List.of(1L, 5L, 12L));
    }

    /**
     * Variante: Monitoreo con máximo de notificaciones
     * Si la alerta no cambia, no emite más de 1 vez por minuto
     */
    public Flux<Alerta> monitorStockBajoConThrottle() {
        return monitorStockBajo()
                // Emite el primer evento, luego ignora eventos en los siguientes 60 segundos
                .sample(Duration.ofMinutes(1));
    }

    /**
     * Variante: Monitoreo que agrega cambios rápidos
     * Si hay cambios dentro de 5 segundos, los agrupa en una sola emisión
     */
    public Flux<Alerta> monitorStockBajoConDebouce() {
        return Flux.interval(Duration.ZERO, Duration.ofSeconds(10))
                .concatMap(tick -> buscarProductosConStockBajo())
                .distinctUntilChanged()
                .filter(lista -> !lista.isEmpty())
                .delaySequence(Duration.ofSeconds(5))
                .map(Alerta::new);
    }

    /**
     * Objeto de dominio para representar una alerta de stock bajo
     */
    public record Alerta(List<Long> productosAfectados) {
    }
}
