package io.surisoft.websocket.handler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.net.URI;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static java.time.Duration.ofMillis;
import static org.springframework.web.reactive.socket.WebSocketMessage.Type.PONG;
import static reactor.core.publisher.Flux.interval;
import static reactor.core.publisher.Flux.range;

public class EchoHandler implements WebSocketHandler {
	KafkaCreator kafkaCreator;

	public EchoHandler(KafkaCreator kafkaCreator) {
		this.kafkaCreator = kafkaCreator;
	}

	private static final Logger log = LoggerFactory.getLogger(EchoHandler.class);

	int initialDelay = 15000;

	int fixedDelay = 15000;

	private final ObjectMapper objectMapper = new ObjectMapper();
	private final Sinks.Many<Event> chatHistory = Sinks.many().replay().limit(1000);

	@Override
	public Mono<Void> handle(WebSocketSession session) {
		AtomicReference<Event> lastReceivedEvent = new AtomicReference<>();

		String clientId = extractClient(session);
		String sessionId = session.getId();

        Flux<String> caches = caches(clientId, sessionId);
		Mono<Void> sendCaches = session.send(caches.map(session::textMessage));
		return Flux.merge(sendCaches).then();
	}

	protected Flux<String> caches(String clientId, String sessionId) {
		return Flux.<String>create(emitter -> {
			ConsumerRunner consumerRunner = new ConsumerRunner();
			consumerRunner.setConsumer(kafkaCreator.createConsumer(clientId));
			consumerRunner.setProcessor(consumerRecord -> {
				log.info("consumerRecord = " + consumerRecord);
				if(consumerRecord.key().equals(clientId)) {
					Event e = new Event();
					e.setUuid(UUID.randomUUID().toString());
					e.setSender(clientId);
					e.setContent(consumerRecord.value());
					e.setType(EventType.IMPORTANT);
					emitter.next(Objects.requireNonNull(toString(e)));
				}
			});
			kafkaCreator.getTaskExecutor().execute(consumerRunner);
			emitter.onDispose(() -> {
				consumerRunner.shutdown();
			});
		});
    }

	protected Flux<WebSocketMessage> createPingsPublisher(WebSocketSession webSocketSession) {
		return interval(ofMillis(initialDelay), ofMillis(fixedDelay))
				.map(delay -> webSocketSession.pingMessage(DataBufferFactory::allocateBuffer));
	}

	protected Flux<WebSocketMessage> createWebsocketActivePublisher(WebSocketSession webSocketSession) {
		// Immediately send the {"active": true} message only once:
		return range(1, 1).map(delay -> webSocketSession.textMessage("websocketsActiveMessage"));
	}

	protected void handlePongs(WebSocketSession webSocketSession) {
		webSocketSession.receive()
				.filter(m -> m.getType() == PONG)
				.map(WebSocketMessage::getPayloadAsText)
				.map(this::toEvent)
				.doOnNext(event -> {
					event.setUuid(UUID.randomUUID().toString());
				})
				.subscribe();
	}

	private Event toEvent(String message) {
		try {
			log.info(message);
			return objectMapper.readValue(message, Event.class);
		} catch(JsonProcessingException e) {

		}
		return null;
	}


	private String toString(Event event) {
		try {
			return objectMapper.writeValueAsString(event);
		} catch(JsonProcessingException e) {

		}
		return null;

	}

	private static String extractClient(WebSocketSession webSocketSession) {
		Map<String, String> paramsMap = new HashMap<>();
		if (webSocketSession != null) {
			webSocketSession.getHandshakeInfo();
			URI uri = webSocketSession.getHandshakeInfo().getUri();
			String[] allParams = uri.getQuery().split("&");
			log.info(Arrays.toString(allParams));
			return allParams[0].split("=")[1];
		}
		return null;
	}
}