package com.example.demo.service;

import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;

/**
 * Ejercicio 3: Paralelismo I/O y Cambio de Hilo (zip y publishOn)
 *
 * Lanzar múltiples peticiones de red al mismo tiempo, esperar a todas,
 * y luego calcular el resultado en un hilo de CPU optimizado.
 *
 * Características:
 * - Mono.zip(): combina múltiples Monos, espera a todos
 * - publishOn(): cambia el contexto de ejecución (thread)
 * - Tiempo total: equivalente al Mono más lento
 * - Ideal para I/O paralelizado + cálculo CPU-bound
 */
@Service
public class Ejercicio3ParalelismoIOCambioHilo {

    /**
     * Tarifica una orden obteniendo datos de múltiples fuentes en paralelo
     * Lanza 3 peticiones concurrentemente y luego calcula en hilo parallel
     *
     * @param clienteId ID del cliente
     * @param base Precio base de la orden
     * @return Mono con la orden tarificada completa
     */
    public Mono<Orden> tarificar(Long clienteId, Double base) {
        // Simula 3 peticiones de red/BD con diferentes latencias
        Mono<Double> mPrecio = Mono.just(base * 1.10)
                .delayElement(Duration.ofMillis(200));  // Simula API de precios

        Mono<Double> mTasa = Mono.just(0.19)
                .delayElement(Duration.ofMillis(150));  // Simula API de impuestos

        Mono<Integer> mRiesgo = Mono.just(15)
                .delayElement(Duration.ofMillis(300)); // Simula servicio de riesgo

        // Lanza los 3 Monos concurrentemente
        // Tarda el equivalente al más lento (300ms, no 200+150+300=650ms)
        return Mono.zip(mPrecio, mTasa, mRiesgo)
                // Cambia el contexto de ejecución para procesar matemáticas
                // en un hilo optimizado para CPU (parallel scheduler)
                .publishOn(Schedulers.parallel())
                .map(tupla -> {
                    Double subtotal = tupla.getT1();
                    Double tasa = tupla.getT2();
                    Integer riesgo = tupla.getT3();

                    // Cálculos (CPU-bound) se ejecutan en hilo parallel
                    Double impuesto = subtotal * tasa;
                    return new Orden(subtotal, impuesto, riesgo);
                });
    }

    /**
     * Variante: Usar subscribeOn para cambiar el hilo de suscripción
     * (punto de partida de la cadena)
     */
    public Mono<Orden> tarificarEnHilo(Long clienteId, Double base) {
        return tarificar(clienteId, base)
                // subscribeOn afecta hacia ARRIBA (punto de inicio)
                .subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * Comparación: Sin zip, secuencial
     * Tarda 200 + 150 + 300 = 650ms
     */
    public Mono<Orden> tarificarSecuencial(Long clienteId, Double base) {
        return Mono.just(base * 1.10)
                .delayElement(Duration.ofMillis(200))
                .flatMap(precio -> Mono.just(0.19)
                        .delayElement(Duration.ofMillis(150))
                        .flatMap(tasa -> Mono.just(15)
                                .delayElement(Duration.ofMillis(300))
                                .map(riesgo -> new Orden(precio, precio * tasa, riesgo))
                        )
                );
    }

    /**
     * Variante: zip con 4 o más elementos usando Mono.zip()
     */
    public Mono<Orden> tarificarConDatosAdicionales(Long clienteId, Double base) {
        Mono<Double> mPrecio = Mono.just(base * 1.10).delayElement(Duration.ofMillis(200));
        Mono<Double> mTasa = Mono.just(0.19).delayElement(Duration.ofMillis(150));
        Mono<Integer> mRiesgo = Mono.just(15).delayElement(Duration.ofMillis(300));
        Mono<Boolean> mEsCliente = Mono.just(true).delayElement(Duration.ofMillis(100));

        return Mono.zip(mPrecio, mTasa, mRiesgo, mEsCliente)
                .publishOn(Schedulers.parallel())
                .map(tupla -> {
                    Double subtotal = tupla.getT1();
                    Double tasa = tupla.getT2();
                    Integer riesgo = tupla.getT3();
                    Boolean esCliente = tupla.getT4();

                    // Aplicar descuento si es cliente
                    Double descuento = esCliente ? 0.05 : 0.0;
                    Double montoFinal = subtotal * (1 - descuento);

                    return new Orden(montoFinal, montoFinal * tasa, riesgo);
                });
    }

    /**
     * Modelo de dato - Orden tarificada
     */
    public record Orden(Double subtotal, Double impuesto, Integer riesgo) {
        public Double total() {
            return subtotal + impuesto;
        }
    }
}
