# Ejercicios 5-15: Spring WebFlux y Project Reactor

## Resumen de Implementación

Todos los ejercicios han sido implementados siguiendo los patrones y estructura del proyecto existente, ubicados en el paquete `com.example.demo.service`.

---

## Ejercicio 5: Resiliencia - Reintento con Backoff (retryWhen)

**Archivo:** `Ejercicio5ResilienciaRetryBackoff.java`

**Concepto:** Reintentar un error de red transitorio esperando tiempos cada vez más largos (backoff exponencial).

**Características:**
- 1er intento inmediato
- Si falla: espera 200ms, 2do intento
- Si falla: espera 400ms, 3er intento  
- Si falla: espera 800ms, 4to intento
- Solo reintenta si es un `TransientException` (error transitorio)
- No reintenta otros errores como 404

**Método principal:**
```java
public Mono<CotizacionPrecio> obtenerPrecio(Long productoId)
```

**Casos de uso:**
- Fallos de conexión transitorios
- Recuperación automática de errores de red
- Protección contra picos temporales

---

## Ejercicio 6: Resiliencia - Timeout y Fallback

**Archivo:** `Ejercicio6ResilienciaTimeoutFallback.java`

**Concepto:** Proteger el sistema de dependencias lentas estableciendo un tiempo máximo y valor seguro.

**Características:**
- Si el servicio no responde en 800ms, lanza `TimeoutException`
- Si hay `TimeoutException`, devuelve un score "seguro" de 50
- No falla la aplicación por dependencias lentas

**Método principal:**
```java
public Mono<Integer> obtenerScoreRiesgo(String clienteId)
```

**Casos de uso:**
- Servicios de antifraude lentos
- APIs externas impredecibles
- Protección de latencia máxima

---

## Ejercicio 7: Propagación Implícita de Contexto

**Archivo:** `Ejercicio7ContextoPropagacion.java`

**Concepto:** Pasar variables a través de toda la cadena reactiva sin agregarlas a parámetros de métodos.

**Características:**
- `Mono.deferContextual()` para leer contexto
- `.contextWrite()` para escribir contexto
- El contexto se propaga automáticamente a través de la cadena
- Ideal para Correlation IDs, User IDs, Tokens, etc.

**Métodos principales:**
```java
public Mono<String> logOperacion()           // Lee del contexto
public Mono<String> endpointPeticion()       // Escribe en contexto
public Mono<String> procesoProfundo()        // Múltiples niveles
```

**Casos de uso:**
- Tracking de requests (Correlation IDs)
- Contexto de usuario en toda la cadena
- Logging distribuido
- Información de auditoría

---

## Ejercicio 8: Sinks - Multicast con Memoria (Replay)

**Archivo:** `Ejercicio8SinksReplay.java`

**Concepto:** Event Bus en memoria que guarda los últimos 200 eventos para suscriptores que se conectan tarde.

**Características:**
- Almacena último buffer de 200 eventos
- Nuevos suscriptores reciben primero el historial
- Luego reciben eventos en vivo
- Patrón: productor-consumidor desacoplado

**Métodos principales:**
```java
public void emitir(EventoOrden evento)      // Emite evento
public Flux<EventoOrden> escuchar()         // Se suscribe (con historial)
```

**Casos de uso:**
- Event sourcing
- Auditoría de eventos
- Sincronización de suscriptores tarde
- Replay de eventos históricos

---

## Ejercicio 9: Sinks - Emisión de Mejor Esfuerzo (Multicast)

**Archivo:** `Ejercicio9SinksMulticast.java`

**Concepto:** Bus de eventos donde eventos se pierden si no hay suscriptores o estos son lentos (fire-and-forget).

**Características:**
- NO almacena eventos (diferencia con Replay)
- Si no hay suscriptores, evento se descarta
- Si suscriptor es lento, evento se pierde
- No bloquea al productor
- Mejor esfuerzo (best-effort)

**Métodos principales:**
```java
public void emitir(EventoInventario evento)
public Flux<EventoInventario> escuchar()
```

**Casos de uso:**
- Logs y métricas
- Notificaciones UI
- Eventos que no son críticos
- Actualizaciones de estado no esenciales

---

## Ejercicio 10: Compartir Suscripciones (Hot Stream con refCount)

**Archivo:** `Ejercicio10TableroCompartido.java`

**Concepto:** Combinar fuentes, activarlas solo con oyentes y mantener conexión con tiempo de gracia.

**Características:**
- `.publish()` convierte flujo frío en caliente
- `.refCount(1, Duration.ofSeconds(5))`:
  - Se conecta al llegar 1er suscriptor
  - Espera 5 segundos de gracia tras perder último suscriptor
  - Desconecta si no hay nuevos suscriptores
- Múltiples suscriptores comparten misma fuente

**Métodos principales:**
```java
public Flux<String> stream()                           // Flujo compartido
public static Flux<String> crearTableroSimulado()     // Para testing
```

**Casos de uso:**
- Múltiples consumers de misma fuente
- Optimización de conexiones
- Reducción de overhead
- Tableros en tiempo real

---

## Ejercicio 11: Contrapresión - Dropping

**Archivo:** `Ejercicio11ContrapresionDropping.java`

**Concepto:** Descartar eventos cuando no se pueden procesar (en lugar de acumular en memoria).

**Características:**
- Cronjob rápido (30s) + procesamiento lento (45s)
- `onBackpressureDrop()`: descarta nuevos ticks
- `onBackpressureBuffer()`: buffer limitado
- `onBackpressureLatest()`: mantiene solo el más reciente

**Métodos principales:**
```java
public void iniciarCronjob()                    // Estrategia DROP
public void iniciarCronjobConBuffer()           // Estrategia BUFFER
public void iniciarCronjobConLatest()           // Estrategia LATEST
private Mono<Void> expirarReservasViejas()     // Tarea larga (45s)
```

**Casos de uso:**
- Polling con procesamiento lento
- Protección de memoria
- Mecanismos de priorización
- Trade-off entre pérdida y acumulación

---

## Ejercicio 12: Carga Masiva - Buffer y Límite de Hilos

**Archivo:** `Ejercicio12CargaMasiva.java`

**Concepto:** Leer miles de registros, agrupar en bloques de 500 y procesar con 2 conexiones BD simultáneas.

**Características:**
- `.buffer(500)`: agrupa en lotes de 500
- `.flatMap(..., 2)`: máximo 2 lotes simultáneos
- `.reduce()`: suma resultados parciales
- Variantes: secuencial (1) o agresivo (5)

**Métodos principales:**
```java
public Mono<ResultadoCarga> importarBulk(Flux<Producto> streamHttp)
public Mono<ResultadoCarga> importarBulkSecuencial(Flux<Producto> streamHttp)
public Mono<ResultadoCarga> importarBulkAgresivo(Flux<Producto> streamHttp)
```

**Casos de uso:**
- Importación masiva de datos
- Limitación de conexiones a BD
- Control de concurrencia
- Bulk operations

---

## Ejercicio 13: Map-Reduce Reactivo en Memoria (groupBy)

**Archivo:** `Ejercicio13MapReduceAgrupamiento.java`

**Concepto:** Sumar totales agrupando por categoría sobre flujo dinámico.

**Características:**
- `.groupBy()`: separa flujo en GroupedFlux por categoría
- `.flatMap(grupo.reduce())`: suma independientemente por grupo
- `.collectList()`: recolecta todos los totales
- Variantes: filtrado y ordenado

**Métodos principales:**
```java
public Mono<List<TotalCategoria>> reporteAgrupado(Flux<Item> flujoVentas)
public Mono<List<TotalCategoria>> reporteAgrupadorFiltrado(...)
public Mono<List<TotalCategoria>> reporteAgrupadorOrdenado(...)
```

**Casos de uso:**
- Reportes agrupados
- Agregación de datos
- Análisis por categoría
- Totales en tiempo real

---

## Ejercicio 14: Acumulación y Emisión en Tiempo Real (scan)

**Archivo:** `Ejercicio14StreamingAcumulado.java`

**Concepto:** Emitir totales parciales con cada elemento (a diferencia de reduce que emite al final).

**Características:**
- `.scan()` vs `.reduce()`:
  - scan: emite N elementos (parciales)
  - reduce: emite 1 elemento (final)
- Ideal para gráficos en vivo
- `.skip(1)` para ignorar elemento inicial (0,0)
- Variantes: con filtro y con pasos

**Métodos principales:**
```java
public Flux<Acumulado> streamingDeIngresos(Flux<Venta> flujoInfinito)
public Flux<Acumulado> streamingDeIngresosConFiltro(...)
public Flux<Acumulado> streamingDeIngresosConPasos(...)
```

**Casos de uso:**
- Dashboards en vivo
- Gráficos de actualización continua
- WebSocket / SSE
- Monitoreo en tiempo real

---

## Ejercicio 15: Evitar Sondeos Repetidos (distinctUntilChanged)

**Archivo:** `Ejercicio15DistintoHastaUnCambio.java`

**Concepto:** Consultar BD cada 10s pero emitir solo si cambian los datos.

**Características:**
- `.distinctUntilChanged()`: bloquea emisión si igual a anterior
- `.filter()`: descarta listas vacías
- Variantes: throttle y debouce
- Sin esto: 60 emisiones/hora aunque datos sean iguales

**Métodos principales:**
```java
public Flux<Alerta> monitorStockBajo()                  // Base
public Flux<Alerta> monitorStockBajoConThrottle()      // 1 por minuto
public Flux<Alerta> monitorStockBajoConDebouce()       // Agregado
```

**Casos de uso:**
- Alertas de cambios
- Reducción de notificaciones
- Evitar actualizaciones innecesarias
- Monitoreo eficiente

---

## Estructura del Proyecto

```
src/main/java/com/example/demo/
├── service/
│   ├── Ejercicio5ResilienciaRetryBackoff.java
│   ├── Ejercicio6ResilienciaTimeoutFallback.java
│   ├── Ejercicio7ContextoPropagacion.java
│   ├── Ejercicio8SinksReplay.java
│   ├── Ejercicio9SinksMulticast.java
│   ├── Ejercicio10TableroCompartido.java
│   ├── Ejercicio11ContrapresionDropping.java
│   ├── Ejercicio12CargaMasiva.java
│   ├── Ejercicio13MapReduceAgrupamiento.java
│   ├── Ejercicio14StreamingAcumulado.java
│   ├── Ejercicio15DistintoHastaUnCambio.java
│   └── ... (otros servicios)
├── dto/ (Records de datos)
├── model/ (Entidades)
├── controller/ (Endpoints REST)
└── repository/ (Acceso a datos)
```

---

## Patrones Reactivos Utilizados

| Patrón | Ejercicio | Propósito |
|--------|-----------|----------|
| **Retry** | 5 | Recuperación automática |
| **Timeout/Fallback** | 6 | Resiliencia |
| **Context** | 7 | Propagación de datos |
| **Replay Sink** | 8 | Event bus con historial |
| **Multicast Sink** | 9 | Event bus sin historial |
| **RefCount/Hot** | 10 | Compartir suscripciones |
| **Backpressure** | 11 | Control de presión |
| **Buffer/flatMap** | 12 | Carga masiva |
| **GroupBy/Reduce** | 13 | Agregación |
| **Scan** | 14 | Acumulación en vivo |
| **DistinctUntilChanged** | 15 | Filtrado de cambios |

---

## Compilación y Validación

✅ Proyecto compilado exitosamente con Gradle
✅ Todos los ejercicios integrados sin romper estructura existente
✅ Patrones consistentes con código existente
✅ Documentación completa en código

---

## Notas Importantes

1. **Inyección de Dependencias:** Los servicios usan `@Service` y se pueden inyectar en controladores
2. **WebClient:** Disponible en config para ejercicios 5 y 6
3. **Records:** Usados para DTOs como `EventoOrden`, `Acumulado`, etc.
4. **Error Handling:** Incluidos en ejercicios relevantes
5. **Testing:** Preparados para tests unitarios con Reactor Test
