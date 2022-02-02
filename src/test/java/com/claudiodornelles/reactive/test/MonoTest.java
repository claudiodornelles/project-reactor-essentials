package com.claudiodornelles.reactive.test;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/*
 * Reactive Streams (Pattern)
 * 1. Asynchronous
 * 2. Non-blocking
 * 3. Backpressure
 * 4. Publisher <- (subscribe) Subscriber
 * 5. Subscription is created
 * 6. Publisher (calls onSubscribe() with the subscription) -> Subscriber
 * 7. Subscription <- (request N elements) Subscriber
 * 8. Publisher (calls onNext()) until:
 *  1. Publisher sends all objects requested.
 *  2. Publisher sends objects it has. (onComplete()) -> subcriber and subscription will be canceled
 *  3. If there is an error (onError() is called) -> subcriber and subscription will be canceled
 */
class MonoTest {

    private static final Logger log = LoggerFactory.getLogger(MonoTest.class);

    @Test
    void monoSubscriber() {
        String name = "Claudio Dornelles";
        Mono<String> mono = Mono.just(name).log();

        mono.subscribe();

        StepVerifier.create(mono)
                .expectNext(name)
                .verifyComplete();
    }

    @Test
    void monoSubscriberConsumer() {
        String name = "Claudio Dornelles";
        Mono<String> mono = Mono.just(name).log();

        mono.subscribe(string -> log.info("Stream Value: {}", string));

        StepVerifier.create(mono)
                .expectNext(name)
                .verifyComplete();
    }

    @Test
    void monoSubscriberConsumerError() {
        String name = "Claudio Dornelles";
        Mono<String> mono = Mono.just(name)
                .map(string -> {
                    throw new RuntimeException("Testing mono with error");
                });

        mono.subscribe(string -> log.info("Stream Value: {}", string), error -> log.error("Something went wrong", error));

        StepVerifier.create(mono)
                .expectError(RuntimeException.class)
                .verify();
    }

    @Test
    void monoSubscriberConsumerComplete() {
        String name = "Claudio Dornelles";
        Mono<String> mono = Mono.just(name).log().map(String::toUpperCase);

        mono.subscribe(
                string -> log.info("Stream Value: {}", string),
                error -> log.error("Somenthing went wrong", error),
                () -> log.info("Stream complete")
        );

        StepVerifier.create(mono)
                .expectNext(name.toUpperCase())
                .verifyComplete();
    }

    @Test
    void monoSubscriberConsumerCompleteSubscription() {
        String name = "Claudio Dornelles";
        Mono<String> mono = Mono.just(name).log().map(String::toUpperCase);

        mono.subscribe(
                string -> log.info("Stream Value: {}", string),
                error -> log.error("Somenthing went wrong", error),
                () -> log.info("Stream complete"),
                Subscription::cancel
        );
    }

    @Test
    void monoDoOnMethods() {
        String name = "Claudio Dornelles";
        Mono<Object> mono = Mono.just(name)
                .log()
                .map(String::toUpperCase)
                .doOnSubscribe(subscription -> log.info("Subscribed"))
                .doOnRequest(longNumber -> log.info("Request Received"))
                .doOnNext(element -> log.info("Sending New Element: {}", element))
                .doOnSuccess(element -> log.info("Successfully Sent: {}", element))
                .flatMap(element -> Mono.empty())
                .doOnNext(element -> log.info("Sending New Element: {}", element))
                .doOnSuccess(element -> log.info("Successfully Sent: {}", element));

        mono.subscribe(
                string -> log.info("Received New Element: {}", string),
                error -> log.error("Somenthing Went Wrong", error),
                () -> log.info("Stream Complete")
        );
    }

    @Test
    void monoDoOnError() {
        String message = "... this is a message sent by onErrorResume() with a new Mono.just()";
        Mono<Object> mono = Mono.error(new IllegalArgumentException("Illegal argument exception error"))
                .doOnError(e -> log.error("Error: {}", e.getMessage()))
                .onErrorResume(error -> {
                    log.info("Executing onErrorResume(), this was the error message: {}", error.getMessage());
                    return Mono.just(message);
                })
                .log();

        StepVerifier.create(mono)
                .expectNext(message)
                .verifyComplete();
    }

    @Test
    void monoOnErrorReturn() {
        String message = "Executing onErrorReturn: Something bad happened";
        Mono<Object> mono = Mono.error(new IllegalArgumentException("Illegal argument exception error"))
                .onErrorReturn(message)
                .log();

        StepVerifier.create(mono)
                .expectNext(message)
                .verifyComplete();
    }
}

