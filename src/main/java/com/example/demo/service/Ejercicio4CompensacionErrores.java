package com.example.demo.service;

import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * Ejercicio 4: Compensación de Errores (onErrorResume)
 *
 * Interceptar un error a mitad de un proceso para realizar un "rollback lógico"
 * (compensación) y re-emitir el error.
 *
 * Características:
 * - onErrorResume: captura error, ejecuta compensación, re-lanza
 * - Patrón Saga: deshacer transacciones parciales
 * - Mantiene la semántica de error al cliente
 * - Ideal para operaciones de múltiples pasos
 */
@Service
public class Ejercicio4CompensacionErrores {

    /**
     * Procesa una compra en múltiples pasos
     * Si falla el cobro, deshace el guardado antes de re-lanzar el error
     *
     * @param pedidoInicial Pedido a procesar
     * @return Mono con el pedido completado
     */
    public Mono<Pedido> procesarCompra(Pedido pedidoInicial) {
        return guardarPedido(pedidoInicial)
                .flatMap(this::cobrarTarjeta)
                // Si cobrarTarjeta falla, entramos al onErrorResume para deshacer
                .onErrorResume(error -> compensar(pedidoInicial)
                        // Después de compensar, re-lanzamos el error original
                        .then(Mono.error(error))
                );
    }

    /**
     * Paso 1: Guardar el pedido en BD
     * Transición: INICIAL → GUARDADO
     *
     * @param p Pedido a guardar
     * @return Mono con el pedido guardado
     */
    private Mono<Pedido> guardarPedido(Pedido p) {
        System.out.println("✓ Guardando pedido: " + p.id());
        return Mono.just(new Pedido(p.id(), "GUARDADO"));
    }

    /**
     * Paso 2: Cobrar la tarjeta del cliente
     * Transición: GUARDADO → COBRADO (o error)
     *
     * @param p Pedido a cobrar
     * @return Mono con el pedido cobrado, o error si falla
     */
    private Mono<Pedido> cobrarTarjeta(Pedido p) {
        System.out.println("✓ Cobrando tarjeta para pedido: " + p.id());
        // Simulación: 30% de probabilidad de fallo
        if (Math.random() < 0.3) {
            return Mono.error(new RuntimeException("Fondos insuficientes para pedido " + p.id()));
        }
        return Mono.just(new Pedido(p.id(), "COBRADO"));
    }

    /**
     * Compensación: Deshacer el guardado del pedido
     * Se ejecuta SOLO si algo falla después de guardar
     *
     * @param p Pedido a deshacer
     * @return Mono.empty() cuando termine la compensación
     */
    private Mono<Void> compensar(Pedido p) {
        System.out.println("⚠️  Compensando - Deshaciendo pedido: " + p.id());
        // Simulación: DELETE FROM pedidos WHERE id = p.id()
        return Mono.empty();
    }

    /**
     * Variante: Compra con múltiples compensaciones en cadena
     * Simula saga con 3 pasos: guardar → actualizar inventario → cobrar
     */
    public Mono<Pedido> procesarCompraCompleta(Pedido pedidoInicial) {
        return guardarPedido(pedidoInicial)
                .flatMap(this::actualizarInventario)
                .flatMap(this::cobrarTarjeta)
                // Si falla en cualquier punto, compensar en orden inverso
                .onErrorResume(error -> compensarInventario(pedidoInicial)
                        .then(compensar(pedidoInicial))
                        .then(Mono.error(error))
                );
    }

    /**
     * Paso intermedio: Actualizar inventario
     */
    private Mono<Pedido> actualizarInventario(Pedido p) {
        System.out.println("✓ Actualizando inventario para pedido: " + p.id());
        return Mono.just(new Pedido(p.id(), "INVENTARIO_ACTUALIZADO"));
    }

    /**
     * Compensación: Restaurar inventario
     */
    private Mono<Void> compensarInventario(Pedido p) {
        System.out.println("⚠️  Compensando - Restaurando inventario para pedido: " + p.id());
        return Mono.empty();
    }

    /**
     * Variante: onErrorResume con lógica de reintentos
     * Si falla, intenta reintentar antes de compensar
     */
    public Mono<Pedido> procesarCompraConReintentos(Pedido pedidoInicial) {
        return guardarPedido(pedidoInicial)
                .flatMap(p -> cobrarTarjeta(p)
                        .retry(2)  // Reintentar hasta 2 veces
                )
                .onErrorResume(error -> compensar(pedidoInicial)
                        .then(Mono.error(error))
                );
    }

    /**
     * Modelo de dato - Pedido
     */
    public record Pedido(Long id, String estado) {
    }
}
