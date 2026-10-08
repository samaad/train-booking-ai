package org.presight.ai.ticketing;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.zxing.BinaryBitmap;
import com.google.zxing.client.j2se.BufferedImageLuminanceSource;
import com.google.zxing.common.HybridBinarizer;
import com.google.zxing.qrcode.QRCodeReader;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.UUID;
import javax.imageio.ImageIO;
import org.junit.jupiter.api.Test;
import org.presight.ai.ticketing.render.QrTokens;
import org.presight.ai.ticketing.storage.ObjectStore;
import org.presight.ai.ticketing.ticket.Ticket;
import org.presight.ai.ticketing.ticket.TicketService;
import org.presight.ai.ticketing.ticket.TicketService.IssueTicket;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient
@Import(TicketApiTest.Containers.class)
public class TicketApiTest {

    static final org.presight.ai.common.security.testing.TestIssuer issuer = new org.presight.ai.common.security.testing.TestIssuer();

    @DynamicPropertySource
    static void oidc(DynamicPropertyRegistry registry) {
        issuer.register(registry);
    }

    WebTestClient raw;

    @Autowired
    void tokens(WebTestClient raw) {
        this.raw = raw;
        this.client = raw.mutate().defaultHeader("Authorization", "Bearer " + issuer.passenger("user-1")).build();
    }


    @TestConfiguration(proxyBeanMethods = false)
    public static class Containers {
        @Bean
        @ServiceConnection
        KafkaContainer kafka() {
            return new KafkaContainer("apache/kafka:4.3.1");
        }

        @Bean
        @ServiceConnection
        PostgreSQLContainer postgres() {
            return new PostgreSQLContainer("postgres:18.6-alpine");
        }
    }

    public static final GenericContainer<?> s3 = new GenericContainer<>("adobe/s3mock:5.2.3").withExposedPorts(9090);

    static {
        s3.start();
    }

    @DynamicPropertySource
    static void objectStore(DynamicPropertyRegistry registry) {
        registry.add("ticketing.object-store.endpoint", () -> "http://" + s3.getHost() + ":" + s3.getMappedPort(9090));
    }

    WebTestClient client;

    @Autowired
    TicketService tickets;

    @Autowired
    QrTokens qrTokens;

    @Autowired
    ObjectStore store;

    @Test
    void issuanceIsIdempotentPerBooking() {
        UUID booking = UUID.randomUUID();
        Ticket first = tickets.issue(command(booking, "ABC234")).block();
        Ticket again = tickets.issue(command(booking, "ABC234")).block();

        assertThat(again.id()).isEqualTo(first.id());
        client.get().uri("/tickets/ABC234").exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.status").isEqualTo("ISSUED")
                .jsonPath("$.seatNo").isEqualTo("B4")
                .jsonPath("$.qrToken").isEqualTo(first.qrToken());
    }

    @Test
    void qrCodeEncodesASignedVerifiableToken() throws Exception {
        Ticket ticket = tickets.issue(command(UUID.randomUUID(), "QRT234")).block();

        byte[] png = client.get().uri("/tickets/QRT234/qr").exchange()
                .expectStatus().isOk()
                .expectHeader().contentType("image/png")
                .expectHeader().valueEquals("Cache-Control", "no-store")
                .expectBody(byte[].class).returnResult().getResponseBody();

        String scanned = new QRCodeReader().decode(new BinaryBitmap(new HybridBinarizer(
                new BufferedImageLuminanceSource(ImageIO.read(new ByteArrayInputStream(png)))))).getText();
        assertThat(scanned).isEqualTo(ticket.qrToken());
        assertThat(qrTokens.verify(scanned)).hasValueSatisfying(claims ->
                assertThat(claims).startsWith("QRT234|" + ticket.bookingId() + "|ER101|"));

        String tampered = scanned.substring(0, scanned.indexOf('.')) + ".AAAA";
        assertThat(qrTokens.verify(tampered)).isEmpty();
    }

    @Test
    void pdfIsRenderedOnceAndCachedInTheObjectStore() {
        tickets.issue(command(UUID.randomUUID(), "PDF234")).block();

        byte[] pdf = client.get().uri("/tickets/PDF234/pdf").exchange()
                .expectStatus().isOk()
                .expectHeader().contentType("application/pdf")
                .expectBody(byte[].class).returnResult().getResponseBody();
        assertThat(new String(pdf, 0, 5, StandardCharsets.US_ASCII)).isEqualTo("%PDF-");

        assertThat(store.get("tickets/PDF234.pdf").block()).isEqualTo(pdf);
        byte[] again = client.get().uri("/tickets/PDF234/pdf").exchange()
                .expectBody(byte[].class).returnResult().getResponseBody();
        assertThat(again).isEqualTo(pdf); // served from the store, not re-rendered
    }

    @Test
    void voidedTicketsLoseTheirQrAndPdf() {
        UUID booking = UUID.randomUUID();
        tickets.issue(command(booking, "VDT234")).block();
        tickets.voidTicket(booking).block();
        tickets.voidTicket(booking).block(); // idempotent

        client.get().uri("/tickets/VDT234").exchange()
                .expectBody()
                .jsonPath("$.status").isEqualTo("VOIDED")
                .jsonPath("$.qrToken").doesNotExist();
        client.get().uri("/tickets/VDT234/qr").exchange()
                .expectStatus().isEqualTo(409)
                .expectBody().jsonPath("$.code").isEqualTo("TICKET_VOIDED");
        client.get().uri("/tickets/VDT234/pdf").exchange().expectStatus().isEqualTo(409);
    }

    @Test
    void ticketsArePrivateToTheirOwner() {
        tickets.issue(command(UUID.randomUUID(), "WNR234")).block();
        WebTestClient other = raw.mutate().defaultHeader("Authorization", "Bearer " + issuer.passenger("user-2")).build();

        client.get().uri("/tickets/WNR234").exchange().expectStatus().isOk(); // the owner sees it (so the 404s below mean something)
        other.get().uri("/tickets/WNR234").exchange().expectStatus().isNotFound(); // a PNR is guessable: no oracle
        other.get().uri("/tickets/WNR234/qr").exchange().expectStatus().isNotFound();
        raw.get().uri("/tickets/WNR234").exchange().expectStatus().isUnauthorized();
        raw.mutate().defaultHeader("Authorization", "Bearer " + issuer.withRoles("desk-1", "SUPPORT")).build()
                .get().uri("/tickets/WNR234").exchange().expectStatus().isOk();
    }

    @Test
    void unknownAndMalformedPnrsAre404() {
        client.get().uri("/tickets/ZZZ999").exchange()
                .expectStatus().isNotFound()
                .expectBody().jsonPath("$.code").isEqualTo("TICKET_NOT_FOUND");
        client.get().uri("/tickets/not-a-pnr").exchange().expectStatus().isNotFound();
    }

    private static IssueTicket command(UUID booking, String pnr) {
        return new IssueTicket(booking, "user-1", pnr, "Aisha Khan", "ER101", LocalDate.now().plusDays(5), "BUSINESS", "B4",
                "DXB", "FUJ", OffsetDateTime.parse(LocalDate.now().plusDays(5) + "T07:55:00+04:00"));
    }
}
