package com.example.demo.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import reactor.test.StepVerifier;
import reactor.core.publisher.Flux;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Ejercicio 14: Streaming Acumulado")
public class Ejercicio14StreamingAcumuladoTest {

    private final Ejercicio14StreamingAcumulado servicio = new Ejercicio14StreamingAcumulado();

    @Test
    @DisplayName("Debe emitir acumulados en tiempo real")
    public void debeEmitirAcumulados() {
        Flux<Ejercicio14StreamingAcumulado.Venta> ventas = Flux.just(
                new Ejercicio14StreamingAcumulado.Venta(100.0),
                new Ejercicio14StreamingAcumulado.Venta(200.0),
                new Ejercicio14StreamingAcumulado.Venta(150.0)
        );

        StepVerifier.create(servicio.streamingDeIngresos(ventas))
                .expectNextCount(3)
                .verifyComplete();
    }

    @Test
    @DisplayName("scan emite acumulados parciales")
    public void scanEmiteAcumuladosParciales() {
        Flux<Ejercicio14StreamingAcumulado.Venta> ventas = Flux.just(
                new Ejercicio14StreamingAcumulado.Venta(100.0),
                new Ejercicio14StreamingAcumulado.Venta(200.0)
        );

        StepVerifier.create(servicio.streamingDeIngresos(ventas))
                .assertNext(acc -> {
                    assertEquals(1, acc.totalOperaciones());
                    assertEquals(100.0, acc.totalMonto());
                })
                .assertNext(acc -> {
                    assertEquals(2, acc.totalOperaciones());
                    assertEquals(300.0, acc.totalMonto());
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("Debe calcular total acumulado correctamente")
    public void debeCaclularTotalAcumulado() {
        Flux<Ejercicio14StreamingAcumulado.Venta> ventas = Flux.just(
                new Ejercicio14StreamingAcumulado.Venta(50.0),
                new Ejercicio14StreamingAcumulado.Venta(50.0)
        );

        StepVerifier.create(servicio.streamingDeIngresos(ventas))
                .assertNext(acc -> assertEquals(50.0, acc.totalMonto()))
                .assertNext(acc -> assertEquals(100.0, acc.totalMonto()))
                .verifyComplete();
    }
}
