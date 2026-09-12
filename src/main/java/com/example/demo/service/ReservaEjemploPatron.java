package com.example.demo.service;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

/**
 * EJEMPLO EDUCATIVO: Patrón de ejecución secuencial estricta con concatMap
 * 
 * Objetivo: Evitar colisiones procesando una lista de ítems de manera 
 * ESTRICTAMENTE SECUENCIAL. El ítem 2 NO INICIA hasta que el 1 termina.
 */
public class ReservaEjemploPatron {

    // Records para el ejemplo
    record ItemOrden(Long productoId, Integer cantidad) {}
    record Reserva(Long productoId, String estado) {}

    /**
     * PATRÓN CORRECTO: concatMap para ejecución secuencial
     * 
     * GARANTÍAS:
     * ✅ Item 1 completa → Item 2 comienza
     * ✅ Item 2 completa → Item 3 comienza
     * ✅ Sin paralelismo, sin colisiones
     * ✅ Orden garantizado
     */
    public Mono<List<Reserva>> reservarTodo(List<ItemOrden> items) {
        return Flux.fromIterable(items)
                // concatMap = Concat (orden) + Map (transformación)
                // Espera a que el Mono interno complete antes de procesar el siguiente
                .concatMap(item -> procesarReserva(item.productoId(), item.cantidad()))
                .collectList();
    }

    private Mono<Reserva> procesarReserva(Long id, int cantidad) {
        // Simula operación I/O (BD, API externa, etc) que toma tiempo
        return Mono.just(new Reserva(id, "RESERVADO"))
                .delayElement(Duration.ofMillis(100));
    }

    // ============================================
    // COMPARATIVA: ANTI-PATRÓN (PARALELISMO NO DESEADO)
    // ============================================

    /**
     * ❌ ANTI-PATRÓN: flatMap procesa TODOS los items en PARALELO
     * 
     * PROBLEMAS:
     * ❌ Item 1, 2, 3 se procesan SIMULTÁNEAMENTE
     * ❌ Riesgo de colisiones en BD (race conditions)
     * ❌ Si hay límite de conexiones, puede fallar
     */
    public Mono<List<Reserva>> reservarTodoParalelo(List<ItemOrden> items) {
        return Flux.fromIterable(items)
                // flatMap = paraleliza automáticamente (EVITAR para reservas)
                .flatMap(item -> procesarReserva(item.productoId(), item.cantidad()))
                .collectList();
    }

    /**
     * ❌ ANTI-PATRÓN: flatMapSequential sigue siendo arriesgado
     * 
     * PROBLEMA:
     * ❌ flatMapSequential mantiene orden pero SIGUE procesando en paralelo
     * ❌ Riesgo de colisiones en BD si hay múltiples conexiones simultáneas
     */
    public Mono<List<Reserva>> reservarTodoFlatMapSequential(List<ItemOrden> items) {
        return Flux.fromIterable(items)
                // flatMapSequential = paraleliza pero mantiene orden de emisión
                // NO ES LO MISMO QUE SECUENCIAL ESTRICTO
                .flatMapSequential(item -> procesarReserva(item.productoId(), item.cantidad()))
                .collectList();
    }

    // ============================================
    // VARIANTE: blockingQueue (Reactor sin paralelismo)
    // ============================================

    /**
     * ✅ ALTERNATIVA: usar concatMap con backpressure controlado
     * 
     * Útil si quieres limitar la velocidad de procesamiento
     */
    public Mono<List<Reserva>> reservarTodoConRateLimiting(List<ItemOrden> items) {
        return Flux.fromIterable(items)
                .concatMap(item -> 
                    procesarReserva(item.productoId(), item.cantidad())
                            .delayElement(Duration.ofMillis(10))  // Rate limit
                )
                .collectList();
    }

    /**
     * RESUMEN DE OPERADORES:
     * 
     * ┌─────────────┬──────────────┬──────────────┐
     * │ Operador    │ Secuencial   │ Paralelo     │
     * ├─────────────┼──────────────┼──────────────┤
     * │ map         │ ✅ Sí        │ ❌ No        │
     * │ flatMap     │ ❌ No (*)    │ ✅ Sí        │
     * │ concatMap   │ ✅ Sí        │ ❌ No        │
     * │ flatMapSeq  │ ✅ Sí        │ ❌ No        │
     * └─────────────┴──────────────┴──────────────┘
     * 
     * (*) flatMap ordena solo si los Flux internos completan en orden,
     *     lo cual NO está garantizado en contextos reactivos.
     * 
     * Para RESERVAS → USA concatMap (es el estándar)
     */
}
