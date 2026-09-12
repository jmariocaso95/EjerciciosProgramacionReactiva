package com.example.demo.service;

import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * Ejercicio 11: Contrapresión - Dropping (onBackpressureDrop)
 *
 * Si tienes un cronjob muy rápido pero el procesamiento es muy lento,
 * descarta los "ticks" nuevos en lugar de acumularlos en memoria.
 *
 * Casos de uso:
 * - Polling rápido con procesamiento lento
 * - Proteger memoria cuando no puedes procesar tan rápido como llegan eventos
 * - Mejor que dejar la memoria crecer indefinidamente
 */
@Service
public class Ejercicio11ContrapresionDropping {

    /**
     * Inicia un cronjob que se ejecuta cada 30 segundos
     * Si el procesamiento anterior aún está en curso, descarta el nuevo tick
     */
    public void iniciarCronjob() {
        Flux.interval(Duration.ofSeconds(30))
                // Si el flatMap (procesamiento) está ocupado con el tick anterior,
                // el nuevo tick simplemente se ignora y se loguea
                .onBackpressureDrop(tick -> {
                    System.out.println("⚠️ Cron retrasado. Tick ignorado: " + tick);
                    System.out.println("   El procesamiento anterior aún está en curso");
                })
                .flatMap(tick -> expirarReservasViejas())
                .subscribe(
                        v -> System.out.println("✓ Cron completado"),
                        error -> System.err.println("✗ Error en cron: " + error),
                        () -> System.out.println("Cron finalizado")
                );
    }

    /**
     * Tarea larga que puede tardar más de 30 segundos
     * Simula una tarea de expiración de reservas en base de datos
     *
     * @return Mono<Void> que completa cuando la tarea termina
     */
    private Mono<Void> expirarReservasViejas() {
        return Mono.delay(Duration.ofSeconds(45))
                .doOnSubscribe(sub -> System.out.println("▶ Iniciando expiración de reservas (45s)"))
                .then();
    }

    /**
     * Variante con buffer limitado en lugar de drop
     * Mantiene un buffer máximo de N elementos. Si se llena, descarta nuevos
     */
    public void iniciarCronjobConBuffer() {
        Flux.interval(Duration.ofSeconds(30))
                // Mantiene un buffer de 2 elementos. Si se llena, descarta nuevos
                .onBackpressureBuffer(2, item -> {
                    System.out.println("⚠️ Buffer lleno. Item descartado: " + item);
                })
                .flatMap(tick -> expirarReservasViejas())
                .subscribe();
    }

    /**
     * Variante con estrategia "latest"
     * Descarta elementos antiguos si hay backpresión, mantiene el más reciente
     */
    public void iniciarCronjobConLatest() {
        Flux.interval(Duration.ofSeconds(30))
                // Mantiene solo el evento más reciente
                .onBackpressureLatest()
                .flatMap(tick -> expirarReservasViejas())
                .subscribe();
    }
}
