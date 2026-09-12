package com.example.demo.controller;

import com.example.demo.model.Cliente;
import com.example.demo.model.Orden;
import com.example.demo.service.ClienteService;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;


@RestController
@RequestMapping("/api/clientes")
public class ClienteController {
    @Autowired
    private ClienteService clienteService;

    @GetMapping
    public Flux<Cliente> getAllClientes() {
        return clienteService.getAllClientes();
    }

    @PostMapping
    public Mono<Cliente> guardarCliente(@Valid @RequestBody Cliente cliente) {
        return clienteService.saveCliente(cliente);
    }



}
