package com.example.demo;

import com.example.demo.controller.ClienteController;
import com.example.demo.model.Cliente;
import com.example.demo.service.ClienteService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.mockito.Mockito.*;

@WebFluxTest(ClienteController.class)
public class ClienteControllerTest {

    @Autowired
    private WebTestClient webClient;

    @MockitoBean
    private ClienteService clienteService;

    @Test
    void testObtenerClientes() {
        when(clienteService.getAllClientes()).thenReturn(Flux.just(
                new Cliente(1L, "Juan", "juan@mail.com"),
                new Cliente(2L, "Ana", "ana@mail.com")
        ));

        webClient.get()
                .uri("/clientes")
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(Cliente.class)
                .hasSize(2);
    }

    @Test
    void testGuardarCliente() {
        Cliente cliente = new Cliente(null, "Pedro", "pedro@mail.com");
        Cliente saved = new Cliente(3L, "Pedro", "pedro@mail.com");

        when(clienteService.saveCliente(cliente)).thenReturn(Mono.just(saved));

        webClient.post()
                .uri("/clientes")
                .bodyValue(cliente)
                .exchange()
                .expectStatus().isOk()
                .expectBody(Cliente.class)
                .isEqualTo(saved);
    }
}
