package com.example.demo.comon;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class ReactiveSupport {
    private static final Logger log = LoggerFactory.getLogger(ReactiveSupport.class);

    public static <T> Mono<T> traced(String step, Mono<T> source) {
        String cid = "unknown";
        return source
                    .doOnSubscribe(s -> log.info("[{}] {} → inicio", cid, step))
                    .doOnSuccess(v -> log.info("[{}] {} → ok", cid, step))
                    .doOnError(e -> log.warn("[{}] {} → error {}", cid, step, e.toString()))
                    .doFinally(sig -> log.debug("[{}] {} → fin ({})", cid, step, sig));
    }
}
