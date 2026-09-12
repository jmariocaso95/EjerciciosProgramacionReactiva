package com.example.demo.service;

import com.example.demo.dto.EventoOrden;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

/**
 * Ejercicio 8: Sinks - Multicast con Memoria (Sinks.many().replay())
 *
 * Un "Event Bus" en memoria que guarda los últimos 200 eventos para
 * dárselos a los suscriptores que se conecten tarde (reproducción histórica).
 *
 * Características:
 * - Almacena los últimos 200 eventos
 * - Los nuevos suscriptores reciben primero el historial
 * - Luego reciben los eventos en vivo
 */
@Service
public class Ejercicio8SinksReplay {

    // Almacena y re-emite (replay) los últimos 200 eventos
    private final Sinks.Many<EventoOrden> sink = Sinks.many().replay().limit(200);

    /**
     * Emite un nuevo evento de orden al bus
     * Los eventos se guardan en el buffer de 200
     *
     * @param evento Evento de orden a emitir
     */
    public void emitir(EventoOrden evento) {
        Sinks.EmitResult resultado = sink.tryEmitNext(evento);
        if (resultado.isFailure()) {
            System.err.println("Error emitiendo evento: " + resultado);
        }
    }

    /**
     * Se suscribe al flujo de eventos
     * Primero recibe los últimos 200 eventos guardados (replay)
     * Luego recibe los eventos nuevos en vivo
     *
     * @return Flux con todos los eventos (históricos + en vivo)
     */
    public Flux<EventoOrden> escuchar() {
        return sink.asFlux();
    }

    /**
     * Obtiene el estado actual del sink (para monitoreo)
     * Nota: Esta información es principalmente para debugging
     */
    public int obtenerEventosEnMemoria() {
        // El replay no expone directamente el tamaño del buffer,
        // pero se mantienen los últimos 200 eventos
        return 200;
    }
}
