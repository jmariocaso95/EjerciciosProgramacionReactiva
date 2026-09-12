package com.example.demo.controller;

import com.example.demo.service.*;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * Controlador de ejemplo que demuestra cómo usar los ejercicios 5-15
 * 
 * NOTA: Este es un ejemplo educativo. En producción, separar en controladores específicos.
 */
@RestController
@RequestMapping("/api/ejercicios")
public class EjerciciosController {

    private final Ejercicio5ResilienciaRetryBackoff ej5;
    private final Ejercicio6ResilienciaTimeoutFallback ej6;
    private final Ejercicio7ContextoPropagacion ej7;
    private final Ejercicio8SinksReplay ej8;
    private final Ejercicio9SinksMulticast ej9;
    private final Ejercicio10TableroCompartido ej10;
    private final Ejercicio11ContrapresionDropping ej11;
    private final Ejercicio14StreamingAcumulado ej14;
    private final Ejercicio15DistintoHastaUnCambio ej15;

    public EjerciciosController(
            Ejercicio5ResilienciaRetryBackoff ej5,
            Ejercicio6ResilienciaTimeoutFallback ej6,
            Ejercicio7ContextoPropagacion ej7,
            Ejercicio8SinksReplay ej8,
            Ejercicio9SinksMulticast ej9,
            Ejercicio10TableroCompartido ej10,
            Ejercicio11ContrapresionDropping ej11,
            Ejercicio14StreamingAcumulado ej14,
            Ejercicio15DistintoHastaUnCambio ej15) {
        this.ej5 = ej5;
        this.ej6 = ej6;
        this.ej7 = ej7;
        this.ej8 = ej8;
        this.ej9 = ej9;
        this.ej10 = ej10;
        this.ej11 = ej11;
        this.ej14 = ej14;
        this.ej15 = ej15;
    }

    // ============ Ejercicio 5: Retry con Backoff ============
    @GetMapping("/5/precio/{productoId}")
    public Mono<?> ejercicio5(@PathVariable Long productoId) {
        return ej5.obtenerPrecio(productoId)
                .map(precio -> "Precio obtenido (con reintentos): " + precio)
                .onErrorReturn("Error permanente después de reintentos");
    }

    // ============ Ejercicio 6: Timeout y Fallback ============
    @GetMapping("/6/riesgo/{clienteId}")
    public Mono<Integer> ejercicio6(@PathVariable String clienteId) {
        return ej6.obtenerScoreRiesgo(clienteId)
                .map(score -> {
                    System.out.println("Score de riesgo: " + score);
                    return score;
                });
    }

    // ============ Ejercicio 7: Contexto ============
    @GetMapping("/7/operacion")
    public Mono<String> ejercicio7() {
        return ej7.endpointPeticion()
                .map(resultado -> "Resultado: " + resultado);
    }

    @GetMapping("/7/profundo")
    public Mono<String> ejercicio7Profundo() {
        return ej7.procesoProfundo()
                .map(resultado -> "Resultado profundo: " + resultado);
    }

    // ============ Ejercicio 8: Sinks Replay ============
    @PostMapping("/8/evento")
    public Mono<String> ejercicio8Emit(@RequestBody String mensaje) {
        ej8.emitir(com.example.demo.dto.EventoOrden.de(1L, com.example.demo.model.EstadoOrden.CONFIRMADA, mensaje));
        return Mono.just("Evento emitido");
    }

    @GetMapping(value = "/8/eventos", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<com.example.demo.dto.EventoOrden> ejercicio8Stream() {
        return ej8.escuchar();
    }

    // ============ Ejercicio 9: Sinks Multicast ============
    @PostMapping("/9/evento")
    public Mono<String> ejercicio9Emit(@RequestParam String tipo, @RequestParam Long productoId, @RequestParam int delta) {
        ej9.emitir(com.example.demo.model.EventoInventario.de(tipo, productoId, delta, 1L));
        return Mono.just("Evento multicast emitido (best-effort)");
    }

    @GetMapping(value = "/9/eventos", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<com.example.demo.model.EventoInventario> ejercicio9Stream() {
        return ej9.escuchar();
    }

    // ============ Ejercicio 10: Tablero Compartido ============
    @GetMapping(value = "/10/tablero", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> ejercicio10() {
        return ej10.stream();
    }

    // ============ Ejercicio 11: Contrapresión ============
    @PostMapping("/11/cronjob")
    public Mono<String> ejercicio11() {
        ej11.iniciarCronjob();
        return Mono.just("Cronjob iniciado con estrategia DROP");
    }

    // ============ Ejercicio 14: Streaming Acumulado ============
    @GetMapping(value = "/14/ingresos", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Ejercicio14StreamingAcumulado.Acumulado> ejercicio14() {
        // Crear flujo de ventas simulado
        Flux<Ejercicio14StreamingAcumulado.Venta> ventasSimuladas = Flux.interval(Duration.ofSeconds(1))
                .map(i -> new Ejercicio14StreamingAcumulado.Venta(100.0 + (i * 50)));

        return ej14.streamingDeIngresos(ventasSimuladas);
    }

    // ============ Ejercicio 15: Distinct Until Changed ============
    @GetMapping(value = "/15/alertas", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Ejercicio15DistintoHastaUnCambio.Alerta> ejercicio15() {
        return ej15.monitorStockBajo();
    }

    // ============ Health Check ============
    @GetMapping("/health")
    public Mono<String> health() {
        return Mono.just("Ejercicios 5-15 funcionando correctamente");
    }
}
