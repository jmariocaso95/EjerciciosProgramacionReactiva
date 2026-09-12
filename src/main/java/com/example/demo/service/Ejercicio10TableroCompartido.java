package com.example.demo.service;

import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.time.Duration;

/**
 * Ejercicio 10: Compartir Suscripciones (Cache de Hot Stream) (publish().refCount())
 *
 * Combinar múltiples fuentes de eventos, activarlas solo cuando haya oyentes
 * y mantener la conexión un tiempo de gracia antes de apagar las fuentes.
 *
 * Características:
 * - publish(): convierte un flujo frío en un flujo caliente compartido
 * - refCount(1): se conecta a los recursos cuando llega el 1er suscriptor
 * - refCount(1, Duration): espera N segundos de gracia tras perder al último
 *   suscriptor antes de apagar las fuentes
 */
@Service
public class Ejercicio10TableroCompartido {

    private final Flux<String> compartido;

    /**
     * Constructor que recibe dos fuentes de eventos y las une en un flujo compartido
     *
     * @param fuenteOrdenes Flux de eventos de órdenes
     * @param fuenteInventario Flux de eventos de inventario
     */
    public Ejercicio10TableroCompartido(Flux<String> fuenteOrdenes, Flux<String> fuenteInventario) {
        // Se convierte en un Publisher "Hot" compartido
        // - publish(): crea un ConnectableFlux (hot stream)
        // - refCount(1, Duration.ofSeconds(5)): se conecta al llegar 1 suscriptor,
        //   espera 5 segundos de gracia tras perder al último antes de desconectar
        this.compartido = Flux.merge(fuenteOrdenes, fuenteInventario)
                .publish()
                .refCount(1, Duration.ofSeconds(5));
    }

    /**
     * Retorna el flujo compartido
     * Todos los suscriptores comparten la misma fuente
     *
     * @return Flux compartido y caliente
     */
    public Flux<String> stream() {
        return compartido;
    }

    /**
     * Crear un tablero compartido con fuentes simuladas (para testing)
     */
    public static Flux<String> crearTableroSimulado() {
        Flux<String> ordenes = Flux.interval(Duration.ofSeconds(2))
                .map(i -> "Orden #" + i);

        Flux<String> inventario = Flux.interval(Duration.ofSeconds(3))
                .map(i -> "Actualización inventario #" + i);

        return Flux.merge(ordenes, inventario)
                .publish()
                .refCount(1, Duration.ofSeconds(5));
    }
}
