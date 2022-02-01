package com.claudiodornelles.reactive.test;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

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
        Mono<String> mono = Mono.just(name);
        log.info("Mono {}", mono);
    }
}

