package com.example.demo.service;

import com.example.demo.dto.ResultadoCarga;
import com.example.demo.model.Producto;
import com.example.demo.repository.ProductReactiveRepository;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * Ejercicio 12: Carga Masiva - Agrupación y Límite de Hilos (buffer y flatMap)
 *
 * Leer miles de registros, agruparlos en bloques de 500 y procesarlos permitiendo
 * solo 2 conexiones simultáneas a la base de datos.
 *
 * Flujo:
 * 1. buffer(500): agrupa elementos en lotes de 500
 * 2. flatMap(..., 2): procesa lotes en paralelo, pero máximo 2 simultáneos
 * 3. reduce(): suma todos los resultados parciales
 */
@Service
public class Ejercicio12CargaMasiva {

    private final ProductReactiveRepository repository;

    public Ejercicio12CargaMasiva(ProductReactiveRepository repository) {
        this.repository = repository;
    }

    /**
     * Importa masivamente productos desde un flujo HTTP
     * Agrupa en lotes de 500 y procesa solo 2 lotes simultáneamente
     *
     * @param streamHttp Flux de productos provenientes de HTTP
     * @return Mono con el resultado total de la carga (cantidad de registros procesados)
     */
    public Mono<ResultadoCarga> importarBulk(Flux<Producto> streamHttp) {
        return streamHttp
                // Crea listas (lotes) de 500 en 500 a partir del flujo original
                .buffer(500)
                // flatMap paraleliza el proceso, pero el "2" limita a solo 2 conexiones BD activas
                // Si llegan más lotes, esperan en cola hasta que uno termine
                .flatMap(this::guardarEnBaseDeDatos, 2)
                // Suma todos los resultados parciales al final
                .reduce(new ResultadoCarga(0, 0), ResultadoCarga::mas);
    }

    /**
     * Guarda un lote de productos en la base de datos
     * Simula una operación que toma tiempo I/O
     *
     * @param lote Lista de productos a guardar
     * @return Mono con el resultado (cantidad de registros guardados)
     */
    private Mono<ResultadoCarga> guardarEnBaseDeDatos(List<Producto> lote) {
        return repository.saveAll(lote)
                .collectList()
                .map(guardados -> {
                    System.out.println("✓ Guardados " + guardados.size() + " productos en BD");
                    return new ResultadoCarga(guardados.size(), 0);
                })
                .onErrorResume(error -> {
                    System.err.println("✗ Error guardando lote de " + lote.size() + " productos: " + error);
                    // En caso de error, retorna un resultado con los fallidos
                    return Mono.just(new ResultadoCarga(0, lote.size()));
                });
    }

    /**
     * Variante: importar con control de concurrencia más fino
     * Limita a 1 solo proceso concurrente (útil para bases de datos muy limitadas)
     */
    public Mono<ResultadoCarga> importarBulkSecuencial(Flux<Producto> streamHttp) {
        return streamHttp
                .buffer(500)
                // concatMap procesa secuencialmente (1 a la vez)
                .concatMap(this::guardarEnBaseDeDatos)
                .reduce(new ResultadoCarga(0, 0), ResultadoCarga::mas);
    }

    /**
     * Variante: importar con más paralelismo
     * Procesa 5 lotes simultáneamente
     */
    public Mono<ResultadoCarga> importarBulkAgresivo(Flux<Producto> streamHttp) {
        return streamHttp
                .buffer(500)
                .flatMap(this::guardarEnBaseDeDatos, 5)
                .reduce(new ResultadoCarga(0, 0), ResultadoCarga::mas);
    }
}
