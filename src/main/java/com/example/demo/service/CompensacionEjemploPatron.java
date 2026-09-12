package com.example.demo.service;

import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicReference;

/**
 * EJEMPLO EDUCATIVO: Patrón de compensación de errores con onErrorResume
 * 
 * Objetivo: Interceptar un error a mitad de un proceso para realizar un 
 * "rollback lógico" (compensación) y re-emitir el error.
 */
public class CompensacionEjemploPatron {

    record Pedido(Long id, String estado) {}

    /**
     * PATRÓN CORRECTO: onErrorResume + Compensación + Re-lanzar error
     * 
     * FLUJO:
     * 1. Paso 1: guardarPedido() - Si falla aquí → error al cliente
     * 2. Paso 2: cobrarTarjeta() - Si falla aquí → interceptamos con onErrorResume
     * 3. Compensación: Deshacemos el guardado (rollback lógico)
     * 4. Re-lanzamos: El error original llega al cliente
     * 
     * VENTAJAS:
     * ✅ No deja datos inconsistentes
     * ✅ El cliente siempre ve el error (no se oculta)
     * ✅ Auditoría: compensación es registrada
     * ✅ Idempotencia: llamadas repetidas son seguras
     */
    public Mono<Pedido> procesarCompra(Pedido pedidoInicial) {
        AtomicReference<Pedido> pedidoGuardado = new AtomicReference<>();
        
        return guardarPedido(pedidoInicial)
                // PASO 1: Guardamos el pedido en BD
                .doOnNext(pedidoGuardado::set)  // Conservamos referencia para compensar
                
                // PASO 2: Intentamos cobrar tarjeta
                .flatMap(this::cobrarTarjeta)
                
                // PATRÓN: Si cobrarTarjeta falla, interceptamos con onErrorResume
                .onErrorResume(error -> 
                    // Entramos aquí SOLO si cobrarTarjeta lanzó un error
                    // guardarPedido ya completó exitosamente
                    compensar(pedidoGuardado.get())
                        // Compensación se ejecuta (deshace el guardado)
                        .then(
                            // CRÍTICO: Re-lanzamos el error original
                            Mono.error(error)
                        )
                );
    }

    private Mono<Pedido> guardarPedido(Pedido p) {
        System.out.println("✅ Guardando pedido: " + p.id());
        return Mono.just(new Pedido(p.id(), "GUARDADO"));
    }

    private Mono<Pedido> cobrarTarjeta(Pedido p) {
        System.out.println("💳 Cobrando tarjeta para pedido: " + p.id());
        // Simula un error en la línea de cobro
        return Mono.error(new RuntimeException("Fondos insuficientes"));
    }

    private Mono<Void> compensar(Pedido p) {
        System.out.println("🔙 Deshaciendo pedido: " + p.id());
        // En producción: DELETE FROM pedidos WHERE id = p.id()
        return Mono.empty();
    }

    // ============================================
    // ANTI-PATRÓN #1: Sin compensación
    // ============================================

    /**
     * ❌ ANTI-PATRÓN: Sin onErrorResume (sin compensación)
     * 
     * PROBLEMA:
     * ❌ Si cobrarTarjeta falla, el pedido quedó guardado en BD
     * ❌ Estado inconsistente: "GUARDADO" pero "NO PAGADO"
     * ❌ Cliente nunca se entera de qué falló
     * ❌ Dinero en suspenso, inventario reservado, confusión
     */
    public Mono<Pedido> procesarCompraSinCompensacion(Pedido pedidoInicial) {
        return guardarPedido(pedidoInicial)
                .flatMap(this::cobrarTarjeta);
                // ❌ FALTA: .onErrorResume(...) para deshacer el guardado
    }

    // ============================================
    // ANTI-PATRÓN #2: Compensación pero sin re-lanzar error
    // ============================================

    /**
     * ❌ ANTI-PATRÓN: Compensación silenciosa (error se pierde)
     * 
     * PROBLEMA:
     * ❌ Compensación se ejecuta (deshace el guardado correctamente)
     * ❌ PERO el error se "traga" y NO se propaga al cliente
     * ❌ Cliente piensa que la compra fue exitosa
     * ❌ Cliente no sabe que el pago falló
     */
    public Mono<Pedido> procesarCompraCompensacionSilenciosa(Pedido pedidoInicial) {
        AtomicReference<Pedido> pedidoGuardado = new AtomicReference<>();
        
        return guardarPedido(pedidoInicial)
                .doOnNext(pedidoGuardado::set)
                .flatMap(this::cobrarTarjeta)
                .onErrorResume(error -> 
                    compensar(pedidoGuardado.get())
                        .then(
                            // ❌ FALTA: Mono.error(error)
                            // El error se pierde aquí, cliente ve "éxito"
                            Mono.just(pedidoGuardado.get())  // MALO
                        )
                );
    }

    // ============================================
    // VARIANTE: Reintentos + Compensación
    // ============================================

    /**
     * ✅ VARIANTE: Reintentos con compensación en caso de fallo final
     * 
     * Escenario: El servidor de pagos es intermitente
     * → Reintentar un par de veces
     * → Si sigue fallando después de 3 intentos → compensar
     */
    public Mono<Pedido> procesarCompraConReintentos(Pedido pedidoInicial) {
        AtomicReference<Pedido> pedidoGuardado = new AtomicReference<>();
        
        return guardarPedido(pedidoInicial)
                .doOnNext(pedidoGuardado::set)
                // Reintenta cobrarTarjeta hasta 3 veces si falla
                .flatMap(p -> cobrarTarjetaConReintentos(p, 3))
                .onErrorResume(error -> 
                    compensar(pedidoGuardado.get())
                        .then(Mono.error(error))
                );
    }

    private Mono<Pedido> cobrarTarjetaConReintentos(Pedido p, int intentos) {
        return cobrarTarjeta(p)
                .retry(intentos)  // Reintenta si falla (total: 1 + intentos intentos)
                .onErrorResume(error -> {
                    // Después de agotar reintentos, propagar el error
                    return Mono.error(new RuntimeException("Cobro falló tras " + (intentos + 1) + " intentos: " + error.getMessage()));
                });
    }

    // ============================================
    // VARIANTE: onErrorResume con diferentes estrategias
    // ============================================

    /**
     * ✅ VARIANTE: Diferentes estrategias según el tipo de error
     * 
     * - Si es error de validación → NO compensar (nunca se guardó)
     * - Si es error de cobro → compensar (pedido está en BD)
     * - Si es error de BD → compensar (pedido parcialmente guardado)
     */
    public Mono<Pedido> procesarCompraConEstrategias(Pedido pedidoInicial) {
        AtomicReference<Pedido> pedidoGuardado = new AtomicReference<>();
        
        return validarPedido(pedidoInicial)  // Si falla, NO hay nada que compensar
                .flatMap(this::guardarPedido)
                .doOnNext(pedidoGuardado::set)
                .flatMap(this::cobrarTarjeta)
                .onErrorResume(error -> {
                    // Estrategia según el tipo de error
                    if (error instanceof IllegalArgumentException) {
                        // Error de validación → no gastar recursos en compensar
                        return Mono.error(error);
                    } else if (error instanceof RuntimeException && error.getMessage().contains("Fondos")) {
                        // Error de pago → compensar y re-lanzar
                        return compensar(pedidoGuardado.get())
                                .then(Mono.error(new RuntimeException("Pago rechazado, pedido cancelado: " + error.getMessage())));
                    } else {
                        // Error desconocido → compensar por seguridad
                        return compensar(pedidoGuardado.get())
                                .then(Mono.error(new RuntimeException("Error interno, pedido cancelado: " + error.getMessage())));
                    }
                });
    }

    private Mono<Pedido> validarPedido(Pedido p) {
        if (p.id() == null || p.id() <= 0) {
            return Mono.error(new IllegalArgumentException("ID de pedido inválido"));
        }
        return Mono.just(p);
    }

    // ============================================
    // RESUMEN: onErrorResume vs onErrorReturn
    // ============================================

    /**
     * ┌────────────────────┬────────────────────────┬──────────────────────┐
     * │ Operador           │ Uso                    │ Parámetro            │
     * ├────────────────────┼────────────────────────┼──────────────────────┤
     * │ onErrorResume      │ Compensación compleja  │ Function<error, Mono>│
     * │ onErrorReturn      │ Valor por defecto      │ Valor estático       │
     * │ onErrorMap         │ Transformar error      │ Function<error, error>
     * │ doOnError          │ Logging/auditoría      │ Consumer<error>      │
     * │ retry              │ Reintentos             │ N intentos           │
     * └────────────────────┴────────────────────────┴──────────────────────┘
     * 
     * PARA COMPENSACIÓN: Usa onErrorResume
     * 
     * Patrón correcto:
     * 
     * return paso1()                    // Transacción 1
     *     .doOnNext(savePaso1)          // Guardamos estado
     *     .flatMap(paso2)               // Transacción 2
     *     .onErrorResume(error ->
     *         compensar(paso1Guardado)  // Deshacer paso 1
     *             .then(Mono.error(error))  // RE-LANZAR error
     *     );
     */
}
