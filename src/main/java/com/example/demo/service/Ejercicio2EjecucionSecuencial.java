package com.example.demo.service;

import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

/**
 * Ejercicio 2: Ejecución Secuencial Estricta (concatMap)
 *
 * Evitar colisiones procesando una lista de ítems de manera estrictamente secuencial.
 * El ítem 2 no inicia hasta que el 1 termina.
 *
 * Características:
 * - concatMap: respeta orden y espera a que cada Mono complete
 * - Diferencia con flatMap: flatMap inicia todos concurrentemente
 * - Uso: operaciones que deben ocurrir en orden (ej. transacciones)
 */
@Service
public class Ejercicio2EjecucionSecuencial {

    /**
     * Reserva toda una lista de items de manera estrictamente secuencial
     * Item 2 no inicia hasta que Item 1 complete
     *
     * @param items Lista de items a reservar
     * @return Mono con lista de reservas en orden
     */
    public Mono<List<Reserva>> reservarTodo(List<ItemOrden> items) {
        return Flux.fromIterable(items)
                // concatMap respeta el orden y espera a que el Mono interno complete
                // Si usáramos flatMap, todos se ejecutarían concurrentemente
                .concatMap(item -> procesarReserva(item.productoId(), item.cantidad()))
                .collectList();
    }

    /**
     * Procesa una reserva individual (operación que toma tiempo)
     * Simula I/O como consulta a BD, llamada a API, etc.
     *
     * @param productoId ID del producto
     * @param cantidad Cantidad a reservar
     * @return Mono con el resultado de la reserva
     */
    private Mono<Reserva> procesarReserva(Long productoId, int cantidad) {
        return Mono.just(new Reserva(productoId, "RESERVADO"))
                // Simula operación I/O (consulta BD, bloqueo de stock, etc.)
                .delayElement(Duration.ofMillis(100));
    }

    /**
     * Variante: Procesar con timeout por item
     * Si algún item tarda más de 500ms, falla el todo
     */
    public Mono<List<Reserva>> reservarTodoConTimeout(List<ItemOrden> items) {
        return Flux.fromIterable(items)
                .concatMap(item -> procesarReserva(item.productoId(), item.cantidad())
                        .timeout(Duration.ofMillis(500)))
                .collectList();
    }

    /**
     * Comparación: flatMap (concurrente)
     * Todos los items se procesan al mismo tiempo
     * No garantiza orden de ejecución
     */
    public Mono<List<Reserva>> reservarTodoEnParalelo(List<ItemOrden> items) {
        return Flux.fromIterable(items)
                // flatMap inicia todos concurrentemente
                .flatMap(item -> procesarReserva(item.productoId(), item.cantidad()))
                .collectList();
    }

    /**
     * Modelo de dato - Item de una orden
     */
    public record ItemOrden(Long productoId, Integer cantidad) {
    }

    /**
     * Modelo de dato - Resultado de una reserva
     */
    public record Reserva(Long productoId, String estado) {
    }
}
