package com.example.demo.service;

import com.example.demo.comon.AppProperties;
import com.example.demo.comon.DomainExceptions;
import com.example.demo.comon.ReactiveSupport;
import com.example.demo.dto.CrearOrdenRequest;
import com.example.demo.dto.EventoOrden;
import com.example.demo.model.EstadoOrden;
import com.example.demo.model.ItemOrden;
import com.example.demo.model.OrdenCompra;
import com.example.demo.repository.ItemOrdenRepository;
import com.example.demo.repository.OrdenCompraRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

@Service
public class OrdenCompraService {
    private final OrdenCompraRepository ordenes;
    private final ItemOrdenRepository items;
    private final EventBus bus;
    private final InventarioPort inventario;
    private final TransactionalOperator tx;
    private final ReservaSaga saga;
    private final AppProperties props;
    private final ServiciosExternosPort externos;

    private static final Logger log = LoggerFactory.getLogger(OrdenCompraService.class);


    public OrdenCompraService(OrdenCompraRepository ordenes, ItemOrdenRepository items, EventBus bus, InventarioPort inventario, TransactionalOperator tx, ReservaSaga saga, AppProperties props, ServiciosExternosPort externos) {
        this.ordenes = ordenes;
        this.items = items;
        this.bus = bus;
        this.inventario = inventario;
        this.tx = tx;
        this.saga = saga;
        this.props = props;
        this.externos = externos;
    }

    public Mono<OrdenCompra> crear(CrearOrdenRequest req, String idempotenteKey) {

        return validar(req)
                .then(buscarExistente(idempotenteKey))
                .switchIfEmpty(Mono.defer(() -> crearNuevaOrden(req, idempotenteKey)));
    }

    /**
     * Crea una nueva orden ejecutando un Saga de compensación.
     * 
     * FLUJO CON COMPENSACIÓN:
     * 1. Validar request
     * 2. Guardar orden en BD (estado GUARDADA)
     * 3. Reservar stock en inventario (estado RESERVADA)
     * 4. Obtener precios + impuestos + riesgo en paralelo (I/O)
     * 5. Validar riesgo
     * 6. Persistir orden final (estado CONFIRMADA)
     * 
     * Si CUALQUIER paso falla:
     * → onErrorResume intercepta el error
     * → Ejecuta compensación (libera stock reservado)
     * → Re-emite el error original al cliente
     */
    private Mono<OrdenCompra> crearNuevaOrden(CrearOrdenRequest req, String idempotenteKey) {
        AtomicReference<List<ItemOrden>> reservados = new AtomicReference<>(List.of());

        return ordenes.save(OrdenCompra.nueva(req.clienteId(), req.region(), idempotenteKey))
                .flatMap(orden -> {
                    List<ItemOrden> pedidos = req.items().stream()
                            .map(itemReq -> new ItemOrden(orden.getId(), itemReq.productoId(), null, itemReq.cantidad(), null))
                            .toList();

                    return ReactiveSupport.traced("reserva", saga.reservarTodos(pedidos, orden.getId()))
                            .doOnNext(reservados::set)
                            .doOnNext(reservadosOk -> {
                                orden.setItems(reservadosOk);
                                orden.setEstado(EstadoOrden.RESERVADA);
                                orden.setExpiraEn(Instant.now().plus(props.reservationTtl()));
                            })
                            .flatMap(reservadosOk -> ReactiveSupport.traced("precios+impuesto+fraude",
                                    tarificar(orden, reservadosOk)))
                            .flatMap(o -> o.getRiskScore() > props.riskThreshold()
                                    ? Mono.error(new DomainExceptions.RiesgoAltoException(o.getRiskScore()))
                                    : Mono.just(o))
                            .flatMap(this::persistir)
                            .doOnNext(o -> bus.publicar(EventoOrden.de(o.getId(), o.getEstado(),
                                    "Stock reservado, total " + o.getTotal())))
                            // PATRÓN COMPENSACIÓN: Intercepta cualquier error y compensa
                            .onErrorResume(ex -> compensar(orden, reservados.get(), ex));

                });

    }

    /**
     * Ejecuta compensación (rollback lógico) en caso de error.
     * 
     * PATRÓN onErrorResume + Compensación:
     * 
     * 1. Intercepta error (ex parámetro)
     * 2. Libera stock reservado (saga.liberarTodo)
     * 3. Marca orden como RECHAZADA o COMPENSADA según el tipo de error
     * 4. Guarda cambios de estado en BD (para auditoría)
     * 5. Publica evento de error (para tracking)
     * 6. RE-LANZA el error original con Mono.error(ex)
     *    → El cliente recibe la excepción
     *    → Los hilos superiores ven el error
     *    → NO se silencia la excepción
     * 
     * IMPORTANTE: Siempre hacer .then(Mono.error(ex)) al final
     * para que el error NO se "perca" en la compensación.
     * 
     * @param orden Orden que falló (estado parcial)
     * @param reservados Items que fueron reservados con éxito
     * @param ex Error original que se debe compensar
     * @return Mono que emite el error original después de compensar
     */
    private Mono<OrdenCompra> compensar(OrdenCompra orden, List<ItemOrden> reservados, Throwable ex) {
        // Determina estado final según el tipo de error
        EstadoOrden estadoFinal = ex instanceof DomainExceptions.RiesgoAltoException 
            ? EstadoOrden.RECHAZADA      // Riesgo alto → cliente vio el error, todo OK
            : EstadoOrden.COMPENSADA;   // Otro error → se hizo compensación

        // PASO 1: Libera stock (rollback del inventario)
        // usa .then() para encadenar operaciones sin pasar valores
        return saga.liberarTodo(reservados, orden.getId())
                // PASO 2: Actualiza estado de orden en BD
                .then(Mono.defer(() -> {
                    orden.setEstado(estadoFinal);
                    orden.setExpiraEn(null);  // No expira una orden rechazada
                    return ordenes.save(orden);
                }))
                // PASO 3: Publica evento de compensación para auditoría/tracking
                .doOnNext(o -> bus.publicar(EventoOrden.de(o.getId(), estadoFinal, ex.getMessage())))
                // PASO 4: RE-EMITE el error original (CRÍTICO)
                // Sin .then(Mono.error(ex)), el error se "pierda" y el cliente vería éxito
                .then(Mono.error(ex));
    }

    private Mono<OrdenCompra> persistir(OrdenCompra orden) {
        List<ItemOrden> aGuardar = orden.getItems().stream()
                .map(i -> i.conOrden(orden.getId()))
                .toList();

        Mono<OrdenCompra> escritura = items.deleteAll(items.findByOrdenId(orden.getId()))
                .thenMany(items.saveAll(aGuardar))
                .collectList()
                .flatMap(guardados -> ordenes.save(orden).doOnNext(o -> o.setItems(guardados)));

        return escritura.as(tx::transactional);
    }

    /**
     * PATRÓN: Paralelismo I/O + Cambio de hilo para computación
     * 
     * Ejecuta 3 llamadas externas SIMULTÁNEAMENTE:
     * 1. externos.precio() - obtiene precios dinámicos de cada item (I/O)
     * 2. externos.tasaImpuesto() - trae tasa por región (I/O)
     * 3. externos.scoreRiesgo() - calcula riesgo del cliente (I/O)
     * 
     * Tiempo total: MAX(precio, tasa, riesgo) en lugar de suma de tiempos
     * 
     * Luego:
     * - Usa publishOn(Schedulers.parallel()) para cambiar a hilo de CPU
     * - Realiza cálculos matemáticos (redondeos, sumas) en hilo paralelo
     * - Evita bloquear hilo de I/O del reactor
     * 
     * @param orden Orden siendo tarificada
     * @param reservadosOk Items ya reservados con precios catálogo
     * @return Mono con Orden tarificada (precios, impuestos, riesgo)
     */
    private Mono<OrdenCompra> tarificar(OrdenCompra orden, List<ItemOrden> reservadosOk) {
        var estimado = reservadosOk.stream()
                .mapToDouble(ItemOrden::totalLinea)
                .sum();

        // PARALELO #1: Obtiene precios dinámicos de TODOS los items simultáneamente
        // flatMap lanza N peticiones concurrentes (sin esperar a que terminen)
        Mono<List<ItemOrden>> conPrecios = Flux.fromIterable(reservadosOk)
                .flatMap(i -> externos.precio(i.getProductoId())
                        .map(q -> i.conPrecio(q.precioUnitario()))
                        .onErrorResume(ex -> {
                            log.warn("Precio dinámico no disponible para {} ({}); fallback catálogo", i.getProductoId(), ex.getClass().getSimpleName());
                            return Mono.just(i);
                        }))
                .collectList();

        // PARALELO #2: Obtiene tasa de impuesto por región (I/O a API/BD)
        Mono<Double> tasa = externos.tasaImpuesto(orden.getRegion());
        
        // PARALELO #3: Calcula score de riesgo del cliente (I/O a servicio externo)
        Mono<Integer> riesgo = externos.scoreRiesgo(String.valueOf(orden.getClienteId()), estimado);

        // Mono.zip: Espera a que TODOS los 3 Monos completen
        // Tiempo total ≈ max(conPrecios, tasa, riesgo)
        // NO es secuencial (paralelismo garantizado)
        return Mono.zip(conPrecios, tasa, riesgo)
                // publishOn: Cambia contexto de hilo
                // De: hilo I/O del reactor (Netty EventLoop)
                // A: hilo de CPU del pool parallel() para cálculos intensivos
                .publishOn(Schedulers.parallel())
                .map(t -> {
                    // Computación CPU en hilo paralelo (no bloquea I/O)
                    List<ItemOrden> tarificados = t.getT1();
                    double subtotal = redondear(tarificados.stream().mapToDouble(ItemOrden::totalLinea).sum());
                    double impuesto = redondear(subtotal * t.getT2());
                    orden.setItems(tarificados);
                    orden.setSubtotal(subtotal);
                    orden.setImpuesto(impuesto);
                    orden.setTotal(redondear(subtotal + impuesto));
                    orden.setRiskScore(t.getT3());
                    return orden;
                });
    }

    private static double redondear(double v) {
        return Math.round(v * 100.0) / 100.0;
    }

    private Mono<OrdenCompra> buscarExistente(String idempotenteKey) {
        if(idempotenteKey == null || idempotenteKey.isBlank()) {
            return Mono.empty();
        }
        return ordenes.findByIdempotencyKey(idempotenteKey)
                .flatMap(this::conItems);
    }

    private Mono<OrdenCompra> conItems(OrdenCompra ordenCompra) {
        return items.findByOrdenId(ordenCompra.getId())
                .collectList()
                .doOnNext(ordenCompra::setItems)
                .thenReturn(ordenCompra);
    }

    private Mono<Void> validar(CrearOrdenRequest req) {
        if(Objects.requireNonNull(req.items()).isEmpty()) {
            return Mono.error(new DomainExceptions.ValidacionException("La orden debe tener al menos un item"));
        }
        if(req.clienteId() == null) {
            return Mono.error(new DomainExceptions.ValidacionException("La orden debe tener un cliente asociado"));
        }
        boolean cantiadadMala = req.items().stream()
                .anyMatch(item -> item.productoId() == null || item.cantidad() == null || item.cantidad() <= 0);
        if (cantiadadMala) {
            return Mono.error(new DomainExceptions.ValidacionException("Todos los items deben tener un producto y una cantidad mayor a 0"));
        }
        return Mono.empty();
    }

    public Flux<EventoOrden> eventos(Long id){
      Flux<EventoOrden> heartbeat = Flux.interval(Duration.ofSeconds(15))
              .map(i -> EventoOrden.heartbeat(id));
      return obtener(id).flatMapMany(o -> {
          Mono<EventoOrden> actual = Mono.just(EventoOrden.de(o.getId(), o.getEstado(), "estado actual"));
          Flux<EventoOrden> vivo = bus.eventosOrden().filter(e -> id.equals(e.ordenId()));
          return Flux.merge(actual, vivo, heartbeat)
                  .takeUntil(e -> e.estado() != null && e.estado().esTerminado())
                  .doOnCancel(() -> log.info("Suscripción a eventos de orden {} cancelada", id))
                  ;
      });
    }

    public Mono<OrdenCompra> confirmar(Long id) {
        return obtener(id)
                .flatMap(o -> o.getEstado() != EstadoOrden.RECHAZADA
                        ? Mono.error(new DomainExceptions.EstadoInvalidoException("La orden esta en "+o.getEstado()))
                        : Flux.fromIterable(o.getItems())
                        .concatMap(i -> inventario.vender(i.getProductoId(), i.getCantidad(), id))
                        .then(Mono.defer(() -> {
                            o.setEstado(EstadoOrden.CONFIRMADA);
                            o.setExpiraEn(null);
                            return ordenes.save(o);
                        }))
                        .doOnNext(saved -> saved.setItems(o.getItems()))
                        .as(tx::transactional))
                .doOnNext(o -> bus.publicar(EventoOrden.de(o.getId(), o.getEstado(), "orden confirmada")));

    }

    private Mono<OrdenCompra> obtener(Long id) {
        return ordenes.findById(id)
                .switchIfEmpty(Mono.error(new DomainExceptions.OrdenNoExisteException(id)))
                .flatMap(this::conItems);
    }
}
