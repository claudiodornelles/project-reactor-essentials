package com.claudiodornelles.reactive.test;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

class OperatorsTest {

    private static final Logger log = LoggerFactory.getLogger(OperatorsTest.class);

    @Test
    void subscriberOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4)
                .map(integer -> {
                    log.info("Map 1 -> Number {} on Thread {}", integer, Thread.currentThread().getName());
                    return integer;
                })
                .subscribeOn(Schedulers.single()) // take effect on the entire flux
                .map(integer -> {
                    log.info("Map 2 -> Number {} on Thread {}", integer, Thread.currentThread().getName());
                    return integer;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1, 2, 3, 4)
                .verifyComplete();
    }

    @Test
    void publishOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4)
                .map(integer -> {
                    log.info("Map 1 -> Number {} on Thread {}", integer, Thread.currentThread().getName());
                    return integer;
                })
                .publishOn(Schedulers.boundedElastic()) // take effect on what is above this given line (it will only create threads for the second Map
                .map(integer -> {
                    log.info("Map 2 -> Number {} on Thread {}", integer, Thread.currentThread().getName());
                    return integer;
                });

        flux.subscribe();
        flux.subscribe();

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1, 2, 3, 4)
                .verifyComplete();
    }

    @Test
    void multipleSubscribeOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4)
                .subscribeOn(Schedulers.boundedElastic()) // take effect on the entire flux
                .map(integer -> {
                    log.info("Map 1 -> Number {} on Thread {}", integer, Thread.currentThread().getName());
                    return integer;
                })
                .subscribeOn(Schedulers.single()) // take effect on the entire flux
                .map(integer -> {
                    log.info("Map 2 -> Number {} on Thread {}", integer, Thread.currentThread().getName());
                    return integer;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1, 2, 3, 4)
                .verifyComplete();
    }

    @Test
    void multiplePublishOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4)
                .publishOn(Schedulers.single()) // take effect on what is above this given line (it will only create threads for the second Map
                .map(integer -> {
                    log.info("Map 1 -> Number {} on Thread {}", integer, Thread.currentThread().getName());
                    return integer;
                })
                .publishOn(Schedulers.boundedElastic()) // take effect on what is above this given line (it will only create threads for the second Map
                .map(integer -> {
                    log.info("Map 2 -> Number {} on Thread {}", integer, Thread.currentThread().getName());
                    return integer;
                });

        flux.subscribe();
        flux.subscribe();

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1, 2, 3, 4)
                .verifyComplete();
    }

    @Test
    void publishAndSubscribeOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4)
                .publishOn(Schedulers.single()) // take effect on what is above this given line (it will only create threads for the second Map
                .map(integer -> {
                    log.info("Map 1 -> Number {} on Thread {}", integer, Thread.currentThread().getName());
                    return integer;
                })
                .subscribeOn(Schedulers.boundedElastic()) // take effect on what is above this given line (it will only create threads for the second Map
                .map(integer -> {
                    log.info("Map 2 -> Number {} on Thread {}", integer, Thread.currentThread().getName());
                    return integer;
                });

        flux.subscribe();
        flux.subscribe();

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1, 2, 3, 4)
                .verifyComplete();
    }

    @Test
    void subscribeAndPublishOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4)
                .subscribeOn(Schedulers.single()) // take effect on what is above this given line (it will only create threads for the second Map
                .map(integer -> {
                    log.info("Map 1 -> Number {} on Thread {}", integer, Thread.currentThread().getName());
                    return integer;
                })
                .publishOn(Schedulers.boundedElastic()) // take effect on what is above this given line (it will only create threads for the second Map
                .map(integer -> {
                    log.info("Map 2 -> Number {} on Thread {}", integer, Thread.currentThread().getName());
                    return integer;
                });

        flux.subscribe();
        flux.subscribe();

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1, 2, 3, 4)
                .verifyComplete();
    }
}
