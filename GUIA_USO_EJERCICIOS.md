# Guía de Uso - Ejercicios 5-15

## ✅ Compilación Exitosa

Todos los ejercicios han sido implementados correctamente. La compilación con Gradle fue exitosa sin errores.

```bash
.\gradlew.bat build -x test
# BUILD SUCCESSFUL in 3s
```

---

## 📍 Ubicación de Archivos

Todos los ejercicios se encuentran en el paquete `com.example.demo.service`:

```
src/main/java/com/example/demo/service/
├── Ejercicio5ResilienciaRetryBackoff.java        (Retry con backoff)
├── Ejercicio6ResilienciaTimeoutFallback.java     (Timeout y fallback)
├── Ejercicio7ContextoPropagacion.java            (Contexto implícito)
├── Ejercicio8SinksReplay.java                    (Event bus con replay)
├── Ejercicio9SinksMulticast.java                 (Event bus best-effort)
├── Ejercicio10TableroCompartido.java             (Hot stream compartido)
├── Ejercicio11ContrapresionDropping.java         (Contrapresión)
├── Ejercicio12CargaMasiva.java                   (Bulk loading)
├── Ejercicio13MapReduceAgrupamiento.java         (GroupBy + Reduce)
├── Ejercicio14StreamingAcumulado.java            (Scan en tiempo real)
└── Ejercicio15DistintoHastaUnCambio.java         (Distinct changed)
```

---

## 🔌 Endpoints de Ejemplo

Se incluye un controlador `EjerciciosController` con ejemplos de uso:

```
GET    /api/ejercicios/5/precio/{productoId}        → Obtener precio con reintentos
GET    /api/ejercicios/6/riesgo/{clienteId}         → Score de riesgo con timeout
GET    /api/ejercicios/7/operacion                  → Operación con contexto
GET    /api/ejercicios/7/profundo                   → Contexto profundo
POST   /api/ejercicios/8/evento                     → Emitir evento (replay)
GET    /api/ejercicios/8/eventos                    → Stream eventos (SSE)
POST   /api/ejercicios/9/evento                     → Emitir evento (multicast)
GET    /api/ejercicios/9/eventos                    → Stream eventos (SSE)
GET    /api/ejercicios/10/tablero                   → Tablero compartido (SSE)
POST   /api/ejercicios/11/cronjob                   → Iniciar cronjob con drop
GET    /api/ejercicios/14/ingresos                  → Streaming acumulado (SSE)
GET    /api/ejercicios/15/alertas                   → Alertas de cambios (SSE)
GET    /api/ejercicios/health                       → Health check
```

---

## 💡 Ejemplos de Uso

### Ejercicio 5: Retry con Backoff

```java
@Service
public class MiServicio {
    private final Ejercicio5ResilienciaRetryBackoff retry;
    
    public Mono<CotizacionPrecio> obtenerPrecio() {
        return retry.obtenerPrecio(123L);  // Reintenta con backoff automático
    }
}
```

**Características:**
- Reintentos automáticos: 1, 200ms, 400ms, 800ms
- Solo reintenta `TransientException` (errores transitorios)
- Ideal para fallos de red temporales

---

### Ejercicio 6: Timeout y Fallback

```java
@Service
public class MiServicio {
    private final Ejercicio6ResilienciaTimeoutFallback timeout;
    
    public Mono<Integer> obtenerRiesgo() {
        return timeout.obtenerScoreRiesgo("cliente123");
        // Si tarda > 800ms → devuelve 50 (score seguro)
    }
}
```

**Casos de uso:**
- APIs lentas o impredecibles
- Protección de latencia máxima
- Fallback a valor seguro

---

### Ejercicio 7: Contexto Implícito

```java
// En la capa alta (Controller/Filter):
@GetMapping("/operacion")
public Mono<String> inicio() {
    return servicio.logOperacion()
            .contextWrite(ctx -> ctx.put("X-Corr-Id", "req-12345"));
}

// En la capa baja (Service):
public Mono<String> logOperacion() {
    return Mono.deferContextual(ctx -> {
        String id = ctx.getOrDefault("X-Corr-Id", "sin-id");
        System.out.println("[ " + id + " ] Operación procesada");
        return Mono.just("OK");
    });
}
```

**Ventajas:**
- Sin pasar parámetros explícitamente
- Disponible en toda la cadena
- Ideal para Correlation IDs, User context

---

### Ejercicio 8: Sink Replay (Event Bus con Historial)

```java
@Service
public class MiServicio {
    private final Ejercicio8SinksReplay bus;
    
    public void procesarOrden() {
        // Emitir evento
        bus.emitir(EventoOrden.de(1L, CONFIRMADA, "Orden procesada"));
    }
    
    public void escucharEventos() {
        // Nuevos suscriptores reciben los últimos 200 eventos + nuevos
        bus.escuchar().subscribe(evento -> {
            System.out.println("Evento: " + evento);
        });
    }
}
```

**Características:**
- Guarda los últimos 200 eventos
- Nuevos suscriptores reciben historial
- Ideal para auditoría y event sourcing

---

### Ejercicio 9: Sink Multicast (Event Bus Fire-and-Forget)

```java
@Service
public class MiServicio {
    private final Ejercicio9SinksMulticast bus;
    
    public void notificarInventario() {
        // Si no hay suscriptores, se descarta
        bus.emitir(EventoInventario.de("VENDIDO", 123L, -1, 1L));
    }
}
```

**Ventajas:**
- No ocupa memoria
- No bloquea si no hay suscriptores
- Ideal para logs y métricas

---

### Ejercicio 10: Tablero Compartido

```java
Flux<String> ordenes = obtenerOrdenes();     // Produce cada 2s
Flux<String> inventario = obtenerInventario(); // Produce cada 3s

var tablero = new Ejercicio10TableroCompartido(ordenes, inventario);

// Múltiples suscriptores comparten la misma fuente
tablero.stream().subscribe(System.out::println);  // Suscriptor 1
tablero.stream().subscribe(System.out::println);  // Suscriptor 2
// Las fuentes se activan con el 1er suscriptor
// Se desactivan 5 segundos después del último
```

---

### Ejercicio 11: Contrapresión - Dropping

```java
@Service
public class MiServicio {
    private final Ejercicio11ContrapresionDropping cp;
    
    public void iniciar() {
        // Cronjob cada 30s
        // Si procesamiento anterior (45s) aún no termina
        // → nuevo tick se descarta
        cp.iniciarCronjob();
    }
}
```

**Estrategias:**
- `DROP`: descarta nuevo
- `BUFFER(N)`: guarda N elementos
- `LATEST`: mantiene solo el más reciente

---

### Ejercicio 12: Carga Masiva

```java
@Service
public class ImportadorService {
    private final Ejercicio12CargaMasiva importer;
    
    public Mono<ResultadoCarga> importar() {
        Flux<Producto> productos = obtenerDelServidor();
        
        // Agrupa en lotes de 500 y procesa 2 simultáneamente
        return importer.importarBulk(productos);
        // Retorna: { guardados: 5000, fallidos: 0 }
    }
}
```

**Variantes:**
- `importarBulk()`: 2 procesos simultáneos (defecto)
- `importarBulkSecuencial()`: 1 a la vez
- `importarBulkAgresivo()`: 5 simultáneos

---

### Ejercicio 13: Map-Reduce - Agrupamiento

```java
@Service
public class ReportesService {
    private final Ejercicio13MapReduceAgrupamiento reducer;
    
    public Mono<List<TotalCategoria>> reporteVentas() {
        Flux<Item> ventas = obtenerVentas();
        
        // Agrupa por categoría y suma automáticamente
        return reducer.reporteAgrupado(ventas);
        // Retorna: [
        //   { categoria: "Electrónica", unidades: 150, monto: 45000.00 },
        //   { categoria: "Ropa", unidades: 320, monto: 12800.00 }
        // ]
    }
}
```

**Con filtrado y ordenamiento:**
```java
reducer.reporteAgrupadorFiltrado(ventas)    // > 1000
       .map(totales -> totales.stream()
           .filter(t -> t.monto() > 1000)
           .toList());

reducer.reporteAgrupadorOrdenado(ventas)    // Descendente
```

---

### Ejercicio 14: Streaming Acumulado

```java
@GetMapping(value = "/dashboard/ingresos", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
public Flux<Acumulado> streamingIngresos() {
    Flux<Venta> ventasEnVivo = obtenerVentasEnTiempoReal();
    
    return ej14.streamingDeIngresos(ventasEnVivo);
    // Emite cada segundo: { totalOperaciones: 1, totalMonto: 100.0 }
    // Luego:              { totalOperaciones: 2, totalMonto: 200.0 }
    // Luego:              { totalOperaciones: 3, totalMonto: 350.0 }
    // ...
}
```

**Diferencia scan vs reduce:**
- `scan()`: emite parciales (ideal para UI en vivo)
- `reduce()`: emite solo el final

---

### Ejercicio 15: Distinct Until Changed

```java
@Service
public class MonitorService {
    private final Ejercicio15DistintoHastaUnCambio monitor;
    
    public void iniciarMonitoreo() {
        // Consulta BD cada 10s
        // Emite solo si los productos afectados CAMBIAN
        monitor.monitorStockBajo()
               .subscribe(alerta -> notificar(alerta));
    }
}
```

**Ventajas:**
- Sin this: 60 notificaciones/hora (datos iguales)
- Con this: solo cambios reales
- Variantes: throttle, debounce

---

## 🧪 Testing

Para testear los servicios, usar Reactor Test:

```java
@Test
public void testEjercicio5() {
    StepVerifier.create(ej5.obtenerPrecio(123L))
        .expectNextCount(1)
        .verifyComplete();
}

@Test
public void testEjercicio14() {
    Flux<Venta> ventas = Flux.just(
        new Venta(100.0),
        new Venta(200.0),
        new Venta(150.0)
    );
    
    StepVerifier.create(ej14.streamingDeIngresos(ventas))
        .expectNext(new Acumulado(1, 100.0))
        .expectNext(new Acumulado(2, 300.0))
        .expectNext(new Acumulado(3, 450.0))
        .verifyComplete();
}
```

---

## 📊 Matriz de Patrones Reactivos

| Ejercicio | Patrón | Operador | Use Case |
|-----------|--------|----------|----------|
| 5 | Retry | `retryWhen` + `Retry.backoff()` | Errores transitorios |
| 6 | Timeout | `timeout` + `onErrorReturn` | Resiliencia |
| 7 | Context | `contextWrite` + `deferContextual` | Contexto implícito |
| 8 | Sink Replay | `Sinks.many().replay()` | Event bus con historial |
| 9 | Sink Multicast | `Sinks.many().multicast()` | Event bus fire-and-forget |
| 10 | Hot Stream | `publish().refCount()` | Compartir conexiones |
| 11 | Backpressure | `onBackpressureDrop` | Descartar cuando lento |
| 12 | Concurrencia | `buffer() + flatMap()` | Carga masiva |
| 13 | Map-Reduce | `groupBy() + reduce()` | Agregación por grupo |
| 14 | Scan | `scan()` | Acumulación en vivo |
| 15 | Distinct | `distinctUntilChanged()` | Cambios solamente |

---

## 🚀 Próximos Pasos

1. **Ejecutar la aplicación:**
   ```bash
   ./gradlew bootRun
   ```

2. **Probar los endpoints:**
   ```bash
   curl http://localhost:8080/api/ejercicios/health
   curl http://localhost:8080/api/ejercicios/5/precio/123
   curl http://localhost:8080/api/ejercicios/15/alertas
   ```

3. **Estudiar cada ejercicio:**
   - Leer el código fuente
   - Entender los comentarios
   - Modificar y experimentar

4. **Combinar patrones:**
   - Usar retry + timeout
   - Context + Sinks
   - GroupBy + Scan

---

## 📝 Notas Importantes

✅ **Inyección de Dependencias:** Los servicios usan `@Service` - se pueden inyectar directamente

✅ **Records:** Los DTOs usan records de Java para mayor claridad

✅ **Error Handling:** Todos los servicios incluyen manejo de errores

✅ **Documentación:** Cada clase tiene comentarios explicativos

✅ **Patrón Consistente:** Siguen los patrones existentes del proyecto

✅ **Sin Romper Estructura:** No se modificó código existente

---

## 📚 Recursos

- [Project Reactor Documentation](https://projectreactor.io/)
- [Spring WebFlux Docs](https://spring.io/projects/spring-webflux)
- [Reactor Patterns](https://github.com/reactor/reactor-core/wiki/Reactive-Streams)
- [RxJava vs Reactor](https://spring.io/blog/2019/02/13/efficient-request-handling-with-spring-webflux)
