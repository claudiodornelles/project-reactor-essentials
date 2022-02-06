package com.claudiodornelles.reactive.test;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;

class FluxTest {

    private static final Logger log = LoggerFactory.getLogger(FluxTest.class);

    @Test
    void fluxSubscriber() {
        Flux<String> flux = Flux.just("Claudio", "Ramon", "Trejes", "Dornelles")
                .log();

        StepVerifier.create(flux)
                .expectNext("Claudio", "Ramon", "Trejes", "Dornelles")
                .verifyComplete();
    }

    @Test
    void fluxSubscriberNumbers() {
        Flux<Integer> flux = Flux.range(1, 5)
                .log();

        StepVerifier.create(flux)
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();
    }

    @Test
    void fluxSubscriberFromList() {
        Flux<Integer> flux = Flux.fromIterable(List.of(1, 2, 3, 4, 5))
                .log();

        StepVerifier.create(flux)
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();
    }

    @Test
    void fluxSubscriberNumbersError() {
        Flux<Integer> flux = Flux.range(1, 5)
                .map(number -> {
                    if (number == 4) throw new IndexOutOfBoundsException("index error");
                    return number;
                })
                .log();

        flux.subscribe(
                element -> log.info("Element: {}", element),
                error -> log.error("Something went wrong.", error),
                () -> log.info("Completed"),
                subscription -> subscription.request(3)
        );

        StepVerifier.create(flux)
                .expectNext(1, 2, 3)
                .expectError(IndexOutOfBoundsException.class)
                .verify();
    }

    @Test
    void fluxSubscriberNumbersUglyBackpressure() {
        Flux<Integer> flux = Flux.range(1, 10)
                .log();

        flux.subscribe(
                new Subscriber<>() {

                    private int count = 0;
                    private Subscription subscription;
                    private final int requestCount = 2;

                    @Override
                    public void onSubscribe(Subscription s) {
                        this.subscription = s;
                        subscription.request(requestCount);
                    }

                    @Override
                    public void onNext(Integer integer) {
                        count++;
                        if (count >= 2) {
                            count = 0;
                            subscription.request(2);
                        }
                    }

                    @Override
                    public void onError(Throwable t) {

                    }

                    @Override
                    public void onComplete() {

                    }
                }
        );

        StepVerifier.create(flux)
                .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .verifyComplete();
    }

    @Test
    void fluxSubscriberNumbersNotSoUglyBackpressure() {
        Flux<Integer> flux = Flux.range(1, 10)
                .log();

        flux.subscribe(
                new BaseSubscriber<>() {

                    private int count = 0;
                    private final int requestCount = 2;

                    @Override
                    protected void hookOnSubscribe(Subscription subscription) {
                        subscription.request(requestCount);
                    }

                    @Override
                    protected void hookOnNext(Integer value) {
                        count++;
                        if (count == requestCount) {
                            count = 0;
                            request(requestCount);
                        }
                    }
                }
        );

        StepVerifier.create(flux)
                .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .verifyComplete();
    }

    @Test
    void fluxSubscriberPrettyBackpressure() {
        Flux<Integer> flux = Flux.range(1, 10)
                .log()
                .limitRate(3);

        StepVerifier.create(flux)
                .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .verifyComplete();
    }

    @Test
    void fluxSubscriberIntervalOne() throws InterruptedException {
        Flux<Long> fluxInterval = Flux.interval(Duration.ofMillis(100))
                .take(10)
                .log();

        fluxInterval.subscribe(
                element -> log.info("Element: {}", element)
        );

        Thread.sleep(3000);
    }

    @Test
    void fluxSubscriberIntervalTwo() throws InterruptedException {
        StepVerifier.withVirtualTime(this::getInterval)
                .expectSubscription()
                .thenAwait(Duration.ofDays(2))
                .expectNext(0L)
                .expectNext(1L)
                .thenCancel()
                .verify();
    }

    @Test
    void fluxSubscriberIntervalThree() throws InterruptedException {
        StepVerifier.withVirtualTime(this::getInterval)
                .expectSubscription()
                .thenAwait(Duration.ofDays(1))
                .expectNext(0L)
                .thenAwait(Duration.ofDays(1))
                .expectNext(1L)
                .thenCancel()
                .verify();
    }

    @Test
    void fluxSubscriberIntervalFour() throws InterruptedException {
        StepVerifier.withVirtualTime(this::getInterval)
                .expectSubscription()
                .expectNoEvent(Duration.ofDays(1))
                .thenAwait(Duration.ofDays(1))
                .expectNext(0L)
                .thenAwait(Duration.ofDays(1))
                .expectNext(1L)
                .thenCancel()
                .verify();
    }

    private Flux<Long> getInterval() {
        return Flux.interval(Duration.ofDays(1))
                .take(10)
                .log();
    }

    @Test
    void connectableFlux() throws InterruptedException {
        ConnectableFlux<Integer> flux = Flux.range(1, 10)
                .log()
                .delayElements(Duration.ofMillis(100))
                .publish();

//        flux.connect();
//
//        log.info("Thread sleeping for 300ms");
//        Thread.sleep(300);
//
//        flux.subscribe(
//                element -> log.info("Sub1 element: {}", element)
//        );
//
//        log.info("Thread sleeping for more 200ms");
//        Thread.sleep(200);
//
//        flux.subscribe(
//                element -> log.info("Sub2 element: {}", element)
//        );

        StepVerifier.create(flux)
                .then(flux::connect)
                .thenConsumeWhile(element -> element <= 5)
                .expectNext(6, 7, 8, 9, 10)
                .expectComplete()
                .verify();
    }

    @Test
    void connectableFluxAutoConnect() {
        Flux<Integer> flux = Flux.range(1, 5)
                .log()
                .delayElements(Duration.ofMillis(100))
                .publish()
                .autoConnect(2);

        StepVerifier
                .create(flux)
                .then(flux::subscribe)
                .expectNext(1, 2, 3, 4, 5)
                .expectComplete()
                .verify();
    }
}
