package com.example.demo.service;

import com.example.demo.model.EventoInventario;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

/**
 * Ejercicio 9: Sinks - Emisión de Mejor Esfuerzo (Sinks.many().multicast().directBestEffort())
 *
 * Un bus de eventos donde si un suscriptor es lento o no hay nadie escuchando,
 * el evento simplemente se pierde (no bloquea la app).
 *
 * Características:
 * - NO almacena eventos (fire-and-forget)
 * - Si no hay suscriptores, el evento se descarta
 * - Si el suscriptor es lento, se descarta el siguiente evento
 * - No bloquea al productor
 * - Ideal para eventos que no son críticos (ej. logs, métricas, actualizaciones UI)
 */
@Service
public class Ejercicio9SinksMulticast {

    // Multicast puro con mejor esfuerzo
    // Si no hay suscriptores, descarta la emisión de inmediato
    private final Sinks.Many<EventoInventario> sink = Sinks.many().multicast().directBestEffort();

    /**
     * Emite un nuevo evento de inventario al bus
     * El evento se descarta si no hay suscriptores o si el suscriptor está ocupado
     *
     * @param evento Evento de inventario a emitir
     */
    public void emitir(EventoInventario evento) {
        Sinks.EmitResult resultado = sink.tryEmitNext(evento);
        if (resultado.isFailure()) {
            // Con directBestEffort(), los fallos son esperados si no hay suscriptores
            System.out.println("Evento descartado (sin suscriptores o buffer lleno): " + evento);
        }
    }

    /**
     * Se suscribe al flujo de eventos
     * Solo recibe los eventos nuevos que ocurren DESPUÉS de la suscripción
     * NO recibe eventos históricos
     *
     * @return Flux con solo los eventos nuevos
     */
    public Flux<EventoInventario> escuchar() {
        return sink.asFlux();
    }

    /**
     * Variante: Emite con callback de error
     * Permite capturar y loguear los casos donde la emisión falla
     *
     * @param evento Evento a emitir
     * @param onFailure Callback si falla la emisión
     */
    public void emitirConCallback(EventoInventario evento, Runnable onFailure) {
        Sinks.EmitResult resultado = sink.tryEmitNext(evento);
        if (resultado.isFailure()) {
            onFailure.run();
        }
    }
}
