package com.example.demo.service;

import com.example.demo.dto.EventoOrden;
import com.example.demo.model.EventoInventario;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

@Component
public class EventBus {

    private final Sinks.Many<EventoOrden> ordenes = Sinks.many().replay().limit(200);
    private final Sinks.Many<EventoInventario> inventario = Sinks.many().multicast().directBestEffort();

    public void publicar(EventoOrden evento) {
        ordenes.emitNext(evento, Sinks.EmitFailureHandler.FAIL_FAST);
    }

    public void publicar(EventoInventario evento) {
        inventario.emitNext(evento, Sinks.EmitFailureHandler.FAIL_FAST);
    }

    public Flux<EventoOrden> eventosOrden() { return ordenes.asFlux(); }

    public Flux<EventoInventario> eventosInventario() { return inventario.asFlux(); }
}
