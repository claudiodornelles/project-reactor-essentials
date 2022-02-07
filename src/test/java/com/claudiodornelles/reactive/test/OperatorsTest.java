package com.claudiodornelles.reactive.test;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.blockhound.BlockHound;
import reactor.blockhound.BlockingOperationError;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

class OperatorsTest {

    private static final Logger log = LoggerFactory.getLogger(OperatorsTest.class);

    @BeforeAll
    static void beforeAll() {
        BlockHound.install();
    }

    @Test
    void blockHoundWorks() {
        try {
            FutureTask<?> task = new FutureTask<>(() -> {
                Thread.sleep(0);
                return "";
            });
            Schedulers.parallel().schedule(task);

            task.get(10, TimeUnit.SECONDS);
            Assertions.fail("should fail");
        } catch (Exception e) {
            Assertions.assertTrue(e.getCause() instanceof BlockingOperationError);
        }
    }

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

    @Test
    void subcribeOnIO() {
        Mono<List<String>> monoList = Mono.fromCallable(
                        () -> Files.readAllLines(Path.of("text-file")))
                .log()
                .subscribeOn(Schedulers.boundedElastic());

        StepVerifier.create(monoList)
                .expectSubscription()
                .thenConsumeWhile(list -> {
                    Assertions.assertFalse(list.isEmpty());
                    log.info("Size {}", list.size());
                    return true;
                })
                .verifyComplete();

    }

    @Test
    void switchIfEmptyOperator() {
        Flux<Object> emptyFlux = emptyFlux()
                .switchIfEmpty(Flux.just("not empty anymore"))
                .log();

        StepVerifier.create(emptyFlux)
                .expectSubscription()
                .expectNext("not empty anymore")
                .expectComplete()
                .verify();
    }

    @Test
    void deferOperator() throws Exception {
        Mono<Long> mono = Mono.just(System.currentTimeMillis());
        Mono<Long> deferMono = Mono.defer(
                () -> Mono.just(System.currentTimeMillis())
        );

        mono.subscribe(l -> log.info("mono time: {}", l));
        Thread.sleep(100);
        mono.subscribe(l -> log.info("mono time: {}", l));
        Thread.sleep(100);
        mono.subscribe(l -> log.info("mono time: {}", l));
        Thread.sleep(100);
        mono.subscribe(l -> log.info("mono time: {}", l));
        Thread.sleep(100);
        mono.subscribe(l -> log.info("mono time: {}", l));

        deferMono.subscribe(l -> log.info("deferMono time: {}", l));
        Thread.sleep(100);
        deferMono.subscribe(l -> log.info("deferMono time: {}", l));
        Thread.sleep(100);
        deferMono.subscribe(l -> log.info("deferMono time: {}", l));
        Thread.sleep(100);
        deferMono.subscribe(l -> log.info("deferMono time: {}", l));
        Thread.sleep(100);
        deferMono.subscribe(l -> log.info("deferMono time: {}", l));

        AtomicLong atomicLong = new AtomicLong();
        deferMono.subscribe(atomicLong::set);
        Assertions.assertTrue(atomicLong.get() > 0);
    }

    private Flux<Object> emptyFlux() {
        return Flux.empty();
    }

    @Test
    void concatOperator() {
        Flux<String> flux1 = Flux.just("a", "b");
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> concatFlux = Flux.concat(flux1, flux2).log();

        StepVerifier.create(concatFlux)
                .expectSubscription()
                .expectNext("a", "b", "c", "d")
                .expectComplete()
                .verify();
    }

    @Test
    void concatWithOperator() {
        Flux<String> flux1 = Flux.just("a", "b");
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> concatFlux = flux1.concatWith(flux2).log();

        StepVerifier.create(concatFlux)
                .expectSubscription()
                .expectNext("a", "b", "c", "d")
                .expectComplete()
                .verify();
    }

    @Test
    void combineLatestOperator() {
        Flux<String> flux1 = Flux.just("a", "b").delayElements(Duration.ofSeconds(1));
        Flux<String> flux2 = Flux.just("c", "d").delayElements(Duration.ofMillis(500));

        Flux<String> combineLatest = Flux.combineLatest(flux1,
                        flux2,
                        (s, s2) -> s.toUpperCase() + s2.toUpperCase())
                .log();

        // Cannot guarantee order, it depends on how fast the fluxes emmit their values
        StepVerifier.create(combineLatest)
                .expectSubscription()
                .expectNext("AC", "AD", "BD")
                .expectComplete()
                .verify();
    }

    @Test
    void mergeOperator() {
        Flux<String> flux1 = Flux.just("a", "b", "c", "d").delayElements(Duration.ofMillis(250));
        Flux<String> flux2 = Flux.just("e", "f", "g", "h").delayElements(Duration.ofMillis(500));

        Flux<String> mergeFlux = Flux.merge(flux1, flux2).log();

        StepVerifier.create(mergeFlux)
                .expectSubscription()
                .expectNext("a", "e", "b", "c", "f", "d", "g", "h")
                .expectComplete()
                .verify();
    }

    @Test
    void mergeWithOperator() {
        Flux<String> flux1 = Flux.just("a", "b", "c", "d").delayElements(Duration.ofMillis(250));
        Flux<String> flux2 = Flux.just("e", "f", "g", "h").delayElements(Duration.ofMillis(500));

        Flux<String> mergeFlux = flux1.mergeWith(flux2).log();

        StepVerifier.create(mergeFlux)
                .expectSubscription()
                .expectNext("a", "e", "b", "c", "f", "d", "g", "h")
                .expectComplete()
                .verify();
    }

    @Test
    void mergeSequentialOperator() {
        Flux<String> flux1 = Flux.just("a", "b", "c", "d").delayElements(Duration.ofMillis(250));
        Flux<String> flux2 = Flux.just("e", "f", "g", "h").delayElements(Duration.ofMillis(500));

        Flux<String> mergeFlux = Flux.mergeSequential(flux1, flux2, flux1).log();

        StepVerifier.create(mergeFlux)
                .expectSubscription()
                .expectNext("a", "b", "c", "d", "e", "f", "g", "h", "a", "b", "c", "d")
                .expectComplete()
                .verify();
    }

    @Test
    void concatDelayErrorOperator() {
        Flux<String> flux1 = Flux.just("a", "b", "c")
                .map(s -> {
                    if (s.equals("b")) {
                        throw new IllegalArgumentException();
                    }
                    return s;
                });
        Flux<String> flux2 = Flux.just("d", "e", "f");

        Flux<String> concatFlux = Flux.concatDelayError(flux1, flux2).log();

        StepVerifier.create(concatFlux)
                .expectSubscription()
                .expectNext("a", "d", "e", "f")
                .expectError(IllegalArgumentException.class)
                .verify();
    }

    @Test
    void mergeDelayErrorOperator() {
        Flux<String> flux1 = Flux.just("a", "b", "c", "d")
                .map(s -> {
                    if (s.equals("b")) {
                        throw new IllegalArgumentException();
                    }
                    return s;
                });
        Flux<String> flux2 = Flux.just("e", "f", "g", "h");

        Flux<String> mergeFlux = Flux.mergeDelayError(1, flux1, flux2).log();

        StepVerifier.create(mergeFlux)
                .expectSubscription()
                .expectNext("a", "e", "f", "g", "h")
                .expectError(IllegalArgumentException.class)
                .verify();
    }

    @Test
    void flatMapOperator() {
        Flux<String> flux = Flux.just("a", "b", "c", "d");

        Flux<String> flatFlux = flux.map(String::toUpperCase)
                .flatMap(this::findByName) // This operator does not necessarily preserve original ordering, as inner element are flattened as they arrive.
                .log();

        StepVerifier.create(flatFlux)
                .expectSubscription()
                .expectNext("nameB1", "nameB2", "nameB1", "nameB2", "nameB1", "nameB2", "nameA1", "nameA2")
                .verifyComplete();
    }

    @Test
    void flatMapSequentialOperator() {
        Flux<String> flux = Flux.just("a", "b", "c", "d");

        Flux<String> flatFlux = flux.map(String::toUpperCase)
                .flatMapSequential(this::findByName) // This operator queues elements from late inners until all elements from earlier inners have been emitted, thus emitting inner sequences as a whole, in an order that matches their source's order.
                .log();

        StepVerifier.create(flatFlux)
                .expectSubscription()
                .expectNext("nameA1", "nameA2", "nameB1", "nameB2", "nameB1", "nameB2", "nameB1", "nameB2")
                .verifyComplete();
    }

    private Flux<String> findByName(String name) {
        return name.equals("A") ? Flux.just("nameA1", "nameA2").delayElements(Duration.ofMillis(100)) : Flux.just("nameB1", "nameB2");
    }

    @Test
    void zipOperator() {
        Flux<String> titleFlux = Flux.just("Grand Blue", "Baki");
        Flux<String> studioFlux = Flux.just("Zero-G", "TMS Entertainment");
        Flux<Integer> episodesFlux = Flux.just(12, 24);

        Flux<Anime> animeFlux = Flux.zip(titleFlux, studioFlux, episodesFlux)
                .flatMap(tuple -> Flux.just(new Anime(tuple.getT1(), tuple.getT2(), tuple.getT3())));

        StepVerifier.create(animeFlux)
                .expectSubscription()
                .expectNext(
                        new Anime("Grand Blue", "Zero-G", 12),
                        new Anime("Baki", "TMS Entertainment", 24))
                .expectComplete()
                .verify();
    }

    @Test
    void zipWithOperator() {
        Flux<String> titleFlux = Flux.just("Grand Blue", "Baki");
        Flux<Integer> episodesFlux = Flux.just(12, 24);

        Flux<Anime> animeFlux = titleFlux.zipWith(episodesFlux)
                .flatMap(tuple -> Flux.just(new Anime(tuple.getT1(), null, tuple.getT2())));

        StepVerifier.create(animeFlux)
                .expectSubscription()
                .expectNext(
                        new Anime("Grand Blue", null, 12),
                        new Anime("Baki", null, 24))
                .expectComplete()
                .verify();
    }

    static class Anime {
        private final String title;
        private final String studio;
        private final int episodes;

        public Anime(String title, String studio, int episodes) {
            this.title = title;
            this.studio = studio;
            this.episodes = episodes;
        }

        @Override
        public String toString() {
            return "Anime{" +
                    "episodes=" + episodes +
                    ", studio='" + studio + '\'' +
                    ", title='" + title + '\'' +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Anime anime = (Anime) o;

            if (episodes != anime.episodes) return false;
            if (!Objects.equals(title, anime.title)) return false;
            return Objects.equals(studio, anime.studio);
        }

        @Override
        public int hashCode() {
            int result = title != null ? title.hashCode() : 0;
            result = 31 * result + (studio != null ? studio.hashCode() : 0);
            result = 31 * result + episodes;
            return result;
        }
    }

}
