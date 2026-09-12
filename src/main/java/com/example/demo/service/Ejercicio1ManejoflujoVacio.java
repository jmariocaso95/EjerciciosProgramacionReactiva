package com.example.demo.service;

import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * Ejercicio 1: Manejo de Flujos Vacíos (switchIfEmpty y defer)
 *
 * Si la reserva en BD no devuelve nada (vacío), determinamos dinámicamente
 * qué excepción lanzar evaluando si el producto existe.
 *
 * Características:
 * - switchIfEmpty: se ejecuta SOLO si el Mono anterior es vacío
 * - Mono.defer(): evaluación perezosa (lazy evaluation)
 * - Encadenamiento de validaciones
 */
@Service
public class Ejercicio1ManejoflujoVacio {

    /**
     * Reserva un producto verificando stock y existencia
     * Si no hay stock, diferencia si el producto no existe o solo no tiene stock
     *
     * @param id ID del producto a reservar
     * @param cantidad Cantidad a reservar
     * @return Mono con el producto después de reservar
     */
    public Mono<Producto> reservar(Long id, int cantidad) {
        // Simula consulta a BD: UPDATE productos SET stock = stock - :qty WHERE id = :id AND stock >= :qty
        return obtenerProductoConStockSuficiente(id, cantidad)
                // switchIfEmpty se ejecuta SOLO si el UPDATE retornó Mono.empty()
                .switchIfEmpty(Mono.defer(() -> verificarExistencia(id)
                        .flatMap(existe ->
                                existe
                                        ? Mono.error(new StockException("Sin stock suficiente para producto " + id))
                                        : Mono.error(new NotFoundException("El producto " + id + " no existe"))
                        )
                ));
    }

    /**
     * Intenta obtener un producto después de reservar stock
     * Retorna vacío si no hay stock suficiente o no existe
     *
     * @param id ID del producto
     * @param cantidad Cantidad requerida
     * @return Mono vacío o con el producto reservado
     */
    private Mono<Producto> obtenerProductoConStockSuficiente(Long id, int cantidad) {
        // Simulación: si id > 100, la reserva falla (no existe o sin stock)
        if (id > 100) {
            return Mono.empty();
        }
        // Simulación: reserva exitosa
        return Mono.just(new Producto(id, 50 - cantidad));
    }

    /**
     * Verifica si un producto existe en BD
     * Evaluación diferida (defer) para ejecutarse solo si se suscribe
     *
     * @param id ID del producto
     * @return Mono<Boolean> true si existe, false si no
     */
    private Mono<Boolean> verificarExistencia(Long id) {
        // Simulación: consulta SELECT COUNT(*) FROM productos WHERE id = :id
        return Mono.defer(() -> {
            // Productos con ID 101-200 existen pero sin stock
            // Productos con ID > 200 no existen
            boolean existe = id <= 200;
            System.out.println("Verificando existencia de producto " + id + ": " + existe);
            return Mono.just(existe);
        });
    }

    /**
     * Modelo de dato - Producto
     */
    public record Producto(Long id, Integer stock) {
    }

    /**
     * Excepción de dominio - Sin stock suficiente
     */
    public static class StockException extends RuntimeException {
        public StockException(String msg) {
            super(msg);
        }
    }

    /**
     * Excepción de dominio - Producto no encontrado
     */
    public static class NotFoundException extends RuntimeException {
        public NotFoundException(String msg) {
            super(msg);
        }
    }
}
