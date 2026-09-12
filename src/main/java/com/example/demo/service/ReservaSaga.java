package com.example.demo.service;

import com.example.demo.model.ItemOrden;
import com.example.demo.model.Producto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;

/**
 * ReservaSaga implementa el patrón Saga con ejecución SECUENCIAL ESTRICTA.
 * 
 * Garantías:
 * - Ítem N+1 NO INICIA hasta que Ítem N complete exitosamente
 * - concatMap preserva orden y espera a que cada Mono interno termine
 * - Evita colisiones de acceso concurrente al inventario
 * - Si un ítem falla, se dispara compensación para liberar ítems ya reservados
 */
public class ReservaSaga {
    private final InventarioPort inventario;
    private static final Logger log = LoggerFactory.getLogger(ReservaSaga.class);

    public ReservaSaga(InventarioPort inventario) {
        this.inventario = inventario;
    }

    /**
     * Reserva una lista de ítems de manera estrictamente secuencial.
     * 
     * Uso de concatMap:
     * - Respeta orden de items (item 1 → item 2 → item 3)
     * - Cada reserva se procesa UNA A LA VEZ
     * - Si item[i] falla, se aborta y se liberan todos los ya reservados
     * 
     * @param items lista de ítems a reservar
     * @param ordenId ID de la orden
     * @return Mono con lista de ítems reservados exitosamente
     */
    public Mono<List<ItemOrden>> reservarTodos(List<ItemOrden> items, Long ordenId) {
        return Mono.defer(() -> {
            List<ItemOrden> hechos = new ArrayList<>();
            return Flux.fromIterable(items)
                    // concatMap: secuencial estricto (no paralelo)
                    // El siguiente item espera a que procesarReserva(item actual) complete
                    .concatMap(i -> procesarReserva(i, ordenId)
                            .map(productoReservado -> new ItemOrden(
                                ordenId, 
                                i.getProductoId(), 
                                productoReservado.getCategory(),
                                i.getCantidad(), 
                                productoReservado.getPrice()
                            ))
                    )
                    .doOnNext(hechos::add)
                    .collectList()
                    .onErrorResume(ex -> {
                        log.warn("Reserva fallida en orden {}: {}. Compensando {} ítems", 
                                 ordenId, ex.getMessage(), hechos.size());
                        return liberarTodo(hechos, ordenId).then(Mono.error(ex));
                    });
        });
    }

    /**
     * Procesa la reserva de un único ítem.
     * Simula operación I/O/BD que toma tiempo.
     */
    private Mono<Producto> procesarReserva(ItemOrden item, Long ordenId) {
        return inventario.reservar(item.getProductoId(), item.getCantidad(), ordenId);
    }

    /**
     * Libera (compensa) una lista de ítems de manera secuencial.
     * Usado solo en caso de error durante la reserva.
     * 
     * Garantía: Ítem N+1 NO se libera hasta que Ítem N complete
     * Errores en liberación se registran pero no detienen el flujo
     */
    public Mono<Void> liberarTodo(List<ItemOrden> items, Long ordenId) {
        if(items == null || items.isEmpty()) {
            return Mono.empty();
        }
        return Flux.fromIterable(items)
                // concatMap: procesa liberaciones UNA A LA VEZ en orden
                .concatMap(i -> inventario.liberar(i.getProductoId(), i.getCantidad(), ordenId)
                        .onErrorResume(ex -> {
                            log.error("No se pudo liberar producto {} de orden {}: {}",  
                                     i.getProductoId(), ordenId, ex.toString());
                            return Mono.empty();
                        })
                )
                .then();
    }
}
