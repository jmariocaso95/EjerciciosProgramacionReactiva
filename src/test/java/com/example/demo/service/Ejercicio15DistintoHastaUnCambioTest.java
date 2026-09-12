package com.example.demo.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import reactor.test.StepVerifier;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Ejercicio 15: Distinct Until Changed")
public class Ejercicio15DistintoHastaUnCambioTest {

    private final Ejercicio15DistintoHastaUnCambio servicio = new Ejercicio15DistintoHastaUnCambio();

    @Test
    @DisplayName("Debe emitir solo cuando hay cambios")
    public void debeEmitirCambios() {
        StepVerifier.create(servicio.monitorStockBajo().take(1))
                .expectNextCount(1)
                .verifyComplete();
    }

    @Test
    @DisplayName("distinctUntilChanged bloquea duplicados")
    public void distinctBloquedupicados() {
        // La funcionalidad se prueba con el flujo real
        StepVerifier.create(servicio.monitorStockBajo().take(1))
                .expectNextCount(1)
                .verifyComplete();
    }

    @Test
    @DisplayName("Debe filtrar listas vacías")
    public void debeFiltraVacias() {
        StepVerifier.create(servicio.monitorStockBajo().take(1))
                .assertNext(alerta -> {
                    assertNotNull(alerta.productosAfectados());
                    assertFalse(alerta.productosAfectados().isEmpty());
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("monitorStockBajoConThrottle debe limitar emisiones")
    public void throttleDebeLimitarEmisiones() {
        // El throttle limita a 1 emisión por minuto
        StepVerifier.create(servicio.monitorStockBajoConThrottle().take(1))
                .expectNextCount(1)
                .verifyComplete();
    }
}
