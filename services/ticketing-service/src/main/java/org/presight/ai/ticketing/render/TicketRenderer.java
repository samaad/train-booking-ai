package org.presight.ai.ticketing.render;

import com.google.zxing.BarcodeFormat;
import com.google.zxing.EncodeHintType;
import com.google.zxing.WriterException;
import com.google.zxing.client.j2se.MatrixToImageWriter;
import com.google.zxing.common.BitMatrix;
import com.google.zxing.qrcode.QRCodeWriter;
import com.google.zxing.qrcode.decoder.ErrorCorrectionLevel;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import org.openpdf.text.Document;
import org.openpdf.text.Font;
import org.openpdf.text.FontFactory;
import org.openpdf.text.Image;
import org.openpdf.text.PageSize;
import org.openpdf.text.Paragraph;
import org.openpdf.text.pdf.PdfWriter;
import org.presight.ai.ticketing.TicketingProperties;
import org.presight.ai.ticketing.ticket.Ticket;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * QR and PDF rendering. Both are CPU-bound, so they run on the parallel scheduler — never on a
 * Netty event-loop thread, which must stay free for I/O.
 */
@Component
public class TicketRenderer {

    private static final int QR_SIZE = 320;
    private static final DateTimeFormatter WHEN = DateTimeFormatter.ofPattern("EEE d MMM yyyy, HH:mm");

    private final ZoneId zone;

    public TicketRenderer(TicketingProperties properties) {
        this.zone = properties.zone();
    }

    public Mono<byte[]> qrPng(Ticket ticket) {
        return Mono.fromCallable(() -> png(ticket.qrToken())).subscribeOn(Schedulers.parallel());
    }

    public Mono<byte[]> pdf(Ticket ticket) {
        return Mono.fromCallable(() -> renderPdf(ticket)).subscribeOn(Schedulers.parallel());
    }

    private static byte[] png(String content) throws WriterException, IOException {
        BitMatrix matrix = new QRCodeWriter().encode(content, BarcodeFormat.QR_CODE, QR_SIZE, QR_SIZE,
                Map.of(EncodeHintType.ERROR_CORRECTION, ErrorCorrectionLevel.M, EncodeHintType.MARGIN, 1));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        MatrixToImageWriter.writeToStream(matrix, "PNG", out);
        return out.toByteArray();
    }

    private byte[] renderPdf(Ticket t) throws WriterException, IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Document doc = new Document(PageSize.A5);
        try {
            PdfWriter.getInstance(doc, out);
            doc.open();
            Font title = FontFactory.getFont(FontFactory.HELVETICA_BOLD, 18);
            Font body = FontFactory.getFont(FontFactory.HELVETICA, 11);
            doc.add(new Paragraph("Train ticket  " + t.pnr(), title));
            doc.add(new Paragraph(" "));
            doc.add(new Paragraph("Passenger: " + t.passengerName(), body));
            doc.add(new Paragraph("Train: " + t.trainNumber() + "   Class: " + t.fareClass() + "   Seat: " + t.seatNo(), body));
            doc.add(new Paragraph(t.fromStation() + " -> " + t.toStation(), body)); // WinAnsi has no arrow glyph
            doc.add(new Paragraph("Departs: " + WHEN.format(t.departure().atZoneSameInstant(zone)), body));
            Image qr = Image.getInstance(png(t.qrToken()));
            qr.scaleToFit(200, 200);
            doc.add(qr);
        } finally {
            if (doc.isOpen()) {
                doc.close();
            }
        }
        return out.toByteArray();
    }
}
