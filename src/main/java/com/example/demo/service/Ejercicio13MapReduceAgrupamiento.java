package com.example.demo.service;

import com.example.demo.dto.TotalCategoria;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * Ejercicio 13: Map-Reduce Reactivo en Memoria (groupBy y reduce)
 *
 * Sumar totales agrupando por categoría sobre un flujo de datos que llega dinámicamente.
 *
 * Flujo:
 * 1. groupBy(): separa el Flux en múltiples GroupedFlux (uno por cada categoría)
 * 2. flatMap con reduce(): suma los elementos de cada GroupedFlux independientemente
 * 3. collectList(): recolecta todos los totales en una lista final
 */
@Service
public class Ejercicio13MapReduceAgrupamiento {

    /**
     * Calcula el reporte agrupado de ventas por categoría
     * Procesa dinámicamente un flujo de items y agrupa por categoría
     *
     * @param flujoVentas Flux de items de ventas con categoría, cantidad y subtotal
     * @return Mono con lista de totales por categoría
     */
    public Mono<List<TotalCategoria>> reporteAgrupado(Flux<Item> flujoVentas) {
        return flujoVentas
                // 1. Separa el Flux original en múltiples GroupedFlux (uno por cada categoría diferente)
                // Cada GroupedFlux mantiene todos los items con la misma categoría
                .groupBy(Item::categoria)
                // 2. Para cada grupo (categoría):
                .flatMap(grupo -> grupo.reduce(
                                        // Estado inicial del acumulador: categoría con 0 unidades y 0 monto
                        new TotalCategoria(grupo.key(), 0, 0.0),
                                        // Función de reducción: suma unidades y monto de cada item
                        (acumulador, item) -> new TotalCategoria(
                                grupo.key(),
                                acumulador.unidades() + item.cantidad(),
                                                acumulador.monto() + item.subtotal()
                        )
                ))
                // 3. Recolecta todos los totales en una lista final
                .collectList();
    }

    /**
     * Versión con mapeo adicional: solo categorías con ventas > 1000
     */
    public Mono<List<TotalCategoria>> reporteAgrupadorFiltrado(Flux<Item> flujoVentas) {
        return reporteAgrupado(flujoVentas)
                .map(totales -> totales.stream()
                        .filter(total -> total.monto() > 1000)
                        .toList()
                );
    }

    /**
     * Versión con ordenamiento: categorías por monto descendente
     */
    public Mono<List<TotalCategoria>> reporteAgrupadorOrdenado(Flux<Item> flujoVentas) {
        return reporteAgrupado(flujoVentas)
                .map(totales -> totales.stream()
                        .sorted((a, b) -> Double.compare(b.monto(), a.monto()))
                        .toList()
                );
    }

    /**
     * Objeto de dominio para representar un item de venta
     */
    public record Item(String categoria, Integer cantidad, Double subtotal) {
    }
}
