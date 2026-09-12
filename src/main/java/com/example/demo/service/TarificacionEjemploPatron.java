package com.example.demo.service;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;

/**
 * EJEMPLO EDUCATIVO: Patrón de paralelismo I/O + cambio de hilo con Zip y publishOn
 * 
 * Objetivo: Lanzar múltiples peticiones de red SIMULTÁNEAMENTE, esperar a todas,
 * y luego calcular el resultado en un hilo de CPU (no en el hilo I/O).
 */
public class TarificacionEjemploPatron {

    record Orden(Double subtotal, Double impuesto, Integer riesgo) {}

    /**
     * PATRÓN CORRECTO: Zip + publishOn para paralelismo + cambio de hilo
     * 
     * FLUJO:
     * 1. Lanza 3 peticiones de red SIMULTÁNEAMENTE (flatMap)
     * 2. Mono.zip espera a que todas completen
     * 3. publishOn cambia a hilo de computación (Schedulers.parallel)
     * 4. map realiza cálculos en hilo de CPU
     * 
     * VENTAJAS:
     * ✅ I/O paralelo: mPrecio, mTasa, mRiesgo se ejecutan JUNTAS
     * ✅ Tiempo total ≈ max(200ms, 150ms, 300ms) = 300ms (no 650ms)
     * ✅ map() se ejecuta en hilo diferente (no bloquea I/O)
     * ✅ Máxima eficiencia de recursos
     */
    public Mono<Orden> tarificar(Long clienteId, Double base) {
        // PASO 1: Lanza 3 operaciones I/O simultáneamente
        
        // I/O #1: Consulta API de precios (simula 200ms)
        Mono<Double> mPrecio = Mono.just(base * 1.10)
                .delayElement(Duration.ofMillis(200))
                .name("PRECIO");

        // I/O #2: Consulta API de tasas impuesto (simula 150ms)
        Mono<Double> mTasa = Mono.just(0.19)
                .delayElement(Duration.ofMillis(150))
                .name("TASA");

        // I/O #3: Consulta servicio de riesgo (simula 300ms - el más lento)
        Mono<Integer> mRiesgo = Mono.just(15)
                .delayElement(Duration.ofMillis(300))
                .name("RIESGO");

        // PASO 2: Zip espera a que TODAS 3 completen (paralelismo garantizado)
        // Tiempo total: max(200, 150, 300) = 300ms
        // NO: 200 + 150 + 300 = 650ms (eso sería concatMap)
        return Mono.zip(mPrecio, mTasa, mRiesgo)
                // PASO 3: publishOn cambia el contexto de ejecución
                // De: Hilo I/O (Netty EventLoop) que vino del delayElement
                // A: Hilo de CPU del pool parallel()
                // Por qué: Los cálculos matemáticos son CPU-intensivos
                .publishOn(Schedulers.parallel())
                // PASO 4: map realiza cálculos en hilo de CPU
                .map(tupla -> {
                    Double subtotal = tupla.getT1();
                    Double tasa = tupla.getT2();
                    Integer riesgo = tupla.getT3();
                    
                    // Operación CPU: multiplicación y redondeo
                    Double impuesto = Math.round(subtotal * tasa * 100.0) / 100.0;
                    
                    return new Orden(subtotal, impuesto, riesgo);
                });
    }

    // ============================================
    // ANTI-PATRÓN: Sin Zip (secuencial)
    // ============================================

    /**
     * ❌ ANTI-PATRÓN: flatMap secuencial (sin Zip)
     * 
     * PROBLEMA:
     * ❌ mPrecio completa → LUEGO mTasa → LUEGO mRiesgo
     * ❌ Tiempo total: 200 + 150 + 300 = 650ms (muy lento)
     * ❌ Desperdicia oportunidad de paralelismo
     */
    public Mono<Orden> tarificarSecuencial(Long clienteId, Double base) {
        Mono<Double> mPrecio = Mono.just(base * 1.10).delayElement(Duration.ofMillis(200));
        
        return mPrecio.flatMap(precio ->
            Mono.just(0.19).delayElement(Duration.ofMillis(150)).flatMap(tasa ->
                Mono.just(15).delayElement(Duration.ofMillis(300)).map(riesgo ->
                    new Orden(precio, precio * tasa, riesgo)
                )
            )
        );
    }

    // ============================================
    // SIN publishOn: Cálculos en hilo I/O (MALO)
    // ============================================

    /**
     * ⚠️ PROBLEMA: Sin publishOn, map() se ejecuta en hilo I/O
     * 
     * RIESGO:
     * ⚠️ Si map() tiene cálculos pesados, bloquea el hilo I/O del reactor
     * ⚠️ Otros requests no pueden ser procesados durante esa computación
     * ⚠️ Derrota el propósito de reactividad
     */
    public Mono<Orden> tarificarSinPublishOn(Long clienteId, Double base) {
        Mono<Double> mPrecio = Mono.just(base * 1.10).delayElement(Duration.ofMillis(200));
        Mono<Double> mTasa = Mono.just(0.19).delayElement(Duration.ofMillis(150));
        Mono<Integer> mRiesgo = Mono.just(15).delayElement(Duration.ofMillis(300));

        return Mono.zip(mPrecio, mTasa, mRiesgo)
                // ❌ FALTA publishOn: map() se ejecuta en hilo Netty I/O
                // .publishOn(Schedulers.parallel())  // ESTO FALTA
                .map(tupla -> {
                    Double subtotal = tupla.getT1();
                    // Si aquí hay cálculos pesados (simulación, ML, etc)
                    // bloquean el hilo de I/O que podría procesar otros requests
                    return new Orden(subtotal, subtotal * tupla.getT2(), tupla.getT3());
                });
    }

    // ============================================
    // VARIANTE: Zip de múltiples items con flatMap
    // ============================================

    /**
     * ✅ VARIANTE REALISTA: Obtener precios de múltiples items EN PARALELO
     * 
     * Escenario: Una orden con 100 items
     * ❌ MALO: for (item : items) { precio = api.getPrecio(item) } // 100 requests secuenciales
     * ✅ BIEN: flatMap todas las peticiones en paralelo, luego Zip todos los resultados
     */
    public Mono<Double> calcularSubtotalParalelo(java.util.List<Long> productoIds) {
        // Lanza peticiones de precio para TODOS los items simultáneamente
        var precios = reactor.core.publisher.Flux.fromIterable(productoIds)
                .flatMap(productoId -> 
                    Mono.just(productoId * 10.0)  // Simula API.getPrecio(productoId)
                        .delayElement(Duration.ofMillis(50))
                )
                .collectList();

        // Paralelo: Obtiene descuentos disponibles
        var descuentos = Mono.just(0.15)
                .delayElement(Duration.ofMillis(100));

        // Paralelo: Verifica disponibilidad de shipping expedito
        var shippingExpedito = Mono.just(true)
                .delayElement(Duration.ofMillis(80));

        // Usa Zip para aguardar a TODOS (paralelismo)
        // Luego cambia hilo para cálculos
        return reactor.core.publisher.Mono.zip(precios, descuentos, shippingExpedito)
                .publishOn(Schedulers.parallel())
                .map(tupla -> {
                    java.util.List<Double> listPrecios = tupla.getT1();
                    double subtotal = listPrecios.stream().mapToDouble(Double::doubleValue).sum();
                    double descuento = tupla.getT2();
                    boolean expedito = tupla.getT3();
                    
                    // Aplicar descuento y cargo por shipping en hilo CPU
                    double conDescuento = subtotal * (1 - descuento);
                    double conShipping = conDescuento + (expedito ? 5.0 : 2.0);
                    return Math.round(conShipping * 100.0) / 100.0;
                });
    }

    // ============================================
    // RESUMEN: Zip vs flatMap vs concatMap
    // ============================================

    /**
     * ┌──────────────┬───────────────────┬──────────────┬─────────┐
     * │ Operador     │ Lanza peticiones  │ Espera todas │ Orden   │
     * ├──────────────┼───────────────────┼──────────────┼─────────┤
     * │ map          │ NO (secuencial)   │ N/A          │ ✅      │
     * │ flatMap      │ SÍ (paralelo)     │ NO (stream)  │ ❌      │
     * │ concatMap    │ NO (secuencial)   │ SÍ           │ ✅      │
     * │ flatMapSeq   │ SÍ (paralelo)     │ NO (stream)  │ ✅      │
     * │ Zip          │ NO (ya exist.)    │ SÍ           │ ✅      │
     * └──────────────┴───────────────────┴──────────────┴─────────┘
     * 
     * PARA PARALELISMO I/O:
     * ✅ Use flatMap para lanzar peticiones simultáneamente
     * ✅ Use Zip para aguardar a todas y combinar resultados
     * ✅ Use publishOn para cambiar a hilo de cómputo CPU
     * 
     * Ejemplo correcto:
     * 
     * Mono<A> aAsync = launchApiCall1(); // No espera
     * Mono<B> bAsync = launchApiCall2(); // No espera
     * Mono<C> cAsync = launchApiCall3(); // No espera
     * 
     * return Mono.zip(aAsync, bAsync, cAsync)     // Aguarda las 3
     *        .publishOn(Schedulers.parallel())    // Cambia hilo
     *        .map(tupla -> calcular(tupla));      // Calcula en CPU
     */
}
