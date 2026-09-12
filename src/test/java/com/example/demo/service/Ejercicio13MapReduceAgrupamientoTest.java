package com.example.demo.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import reactor.test.StepVerifier;

import com.example.demo.dto.TotalCategoria;
import reactor.core.publisher.Flux;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Ejercicio 13: Map-Reduce Agrupamiento")
public class Ejercicio13MapReduceAgrupamientoTest {

    private final Ejercicio13MapReduceAgrupamiento servicio = new Ejercicio13MapReduceAgrupamiento();

    @Test
    @DisplayName("Debe agrupar items por categoría")
    public void debeAgruparPorCategoria() {
        Flux<Ejercicio13MapReduceAgrupamiento.Item> items = Flux.just(
                new Ejercicio13MapReduceAgrupamiento.Item("Electrónica", 2, 100.0),
                new Ejercicio13MapReduceAgrupamiento.Item("Electrónica", 3, 150.0),
                new Ejercicio13MapReduceAgrupamiento.Item("Ropa", 5, 50.0)
        );

        StepVerifier.create(servicio.reporteAgrupado(items))
                .assertNext(totales -> {
                    assertEquals(2, totales.size());
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("Debe sumar unidades por categoría")
    public void debeSumarUnidades() {
        Flux<Ejercicio13MapReduceAgrupamiento.Item> items = Flux.just(
                new Ejercicio13MapReduceAgrupamiento.Item("Electrónica", 10, 500.0),
                new Ejercicio13MapReduceAgrupamiento.Item("Electrónica", 20, 1000.0)
        );

        StepVerifier.create(servicio.reporteAgrupado(items))
                .assertNext(totales -> {
                    TotalCategoria electro = totales.stream()
                            .filter(t -> t.categoria().equals("Electrónica"))
                            .findFirst()
                            .orElse(null);
                    assertNotNull(electro);
                    assertEquals(30, electro.unidades());
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("Debe sumar montos por categoría")
    public void debeSumarMontos() {
        Flux<Ejercicio13MapReduceAgrupamiento.Item> items = Flux.just(
                new Ejercicio13MapReduceAgrupamiento.Item("Ropa", 1, 100.0),
                new Ejercicio13MapReduceAgrupamiento.Item("Ropa", 1, 200.0)
        );

        StepVerifier.create(servicio.reporteAgrupado(items))
                .assertNext(totales -> {
                    TotalCategoria ropa = totales.stream()
                            .filter(t -> t.categoria().equals("Ropa"))
                            .findFirst()
                            .orElse(null);
                    assertNotNull(ropa);
                    assertEquals(300.0, ropa.monto());
                })
                .verifyComplete();
    }
}
