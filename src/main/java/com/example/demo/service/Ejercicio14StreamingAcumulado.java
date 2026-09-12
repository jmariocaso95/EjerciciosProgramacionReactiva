package com.example.demo.service;

import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

/**
 * Ejercicio 14: Acumulación y Emisión en Tiempo Real (scan)
 *
 * A diferencia de reduce (que emite un único total al finalizar el Flux),
 * scan emite el total parcial paso a paso con cada elemento recibido.
 * Ideal para gráficos en vivo, dashboards, WebSocket o SSE.
 *
 * Comparación:
 * - reduce(): emite 1 elemento al final con el resultado total
 * - scan(): emite N elementos (uno después de procesar cada item), con totales parciales
 */
@Service
public class Ejercicio14StreamingAcumulado {

    /**
     * Flujo de ingresos acumulados en tiempo real
     * Emite el total parcial con cada nueva venta que llega
     * Ideal para enviarlo a través de SSE (Server-Sent Events) o WebSocket
     *
     * @param flujoInfinito Flux infinito de ventas que llegan dinámicamente
     * @return Flux de acumulados parciales (emite después de cada venta)
     */
    public Flux<Acumulado> streamingDeIngresos(Flux<Venta> flujoInfinito) {
        return flujoInfinito
                // scan mantiene el estado y emite cada resultado intermedio a los oyentes
                .scan(
                        new Acumulado(0, 0.0),  // Estado inicial
                        (acc, venta) -> new Acumulado(
                                acc.totalOperaciones() + 1,
                                acc.totalMonto() + venta.monto()
                        )
                )
                // Ignoramos la emisión "0, 0.0" inicial para no enviar basura al cliente
                .skip(1);
    }

    /**
     * Variante: emite el acumulado pero solo cuando supera cierto monto
     */
    public Flux<Acumulado> streamingDeIngresosConFiltro(Flux<Venta> flujoInfinito) {
        return streamingDeIngresos(flujoInfinito)
                .filter(acc -> acc.totalMonto() >= 100.0);
    }

    /**
     * Variante: limita la emisión a cada N operaciones (ej. cada 10 ventas)
     */
    public Flux<Acumulado> streamingDeIngresosConPasos(Flux<Venta> flujoInfinito) {
        return streamingDeIngresos(flujoInfinito)
                .filter(acc -> acc.totalOperaciones() % 10 == 0);
    }

    /**
     * Objeto de dominio para representar una venta
     */
    public record Venta(Double monto) {
    }

    /**
     * Objeto de dominio para representar el acumulado
     */
    public record Acumulado(Integer totalOperaciones, Double totalMonto) {
        public String formato() {
            return String.format("Operaciones: %d, Monto total: $%.2f", totalOperaciones, totalMonto);
        }
    }
}
