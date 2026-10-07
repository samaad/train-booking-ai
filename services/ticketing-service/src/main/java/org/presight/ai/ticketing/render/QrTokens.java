package org.presight.ai.ticketing.render;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Optional;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.presight.ai.ticketing.TicketingProperties;
import org.springframework.stereotype.Component;

/**
 * The QR payload: {@code base64url(claims).base64url(HMAC-SHA256(claims))}. A gate scanner holding
 * the key can verify a ticket offline; it cannot learn about voiding offline, so gates must still
 * check status online when they can.
 */
@Component
public class QrTokens {

    private static final Base64.Encoder B64 = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder B64D = Base64.getUrlDecoder();

    private final SecretKeySpec key;

    public QrTokens(TicketingProperties properties) {
        this.key = new SecretKeySpec(properties.qrSecret().getBytes(StandardCharsets.UTF_8), "HmacSHA256");
    }

    public String sign(String claims) {
        String body = B64.encodeToString(claims.getBytes(StandardCharsets.UTF_8));
        return body + '.' + B64.encodeToString(mac(body));
    }

    /** The claims, if and only if the signature is valid. Constant-time comparison. */
    public Optional<String> verify(String token) {
        int dot = token.indexOf('.');
        if (dot <= 0 || dot != token.lastIndexOf('.')) {
            return Optional.empty();
        }
        String body = token.substring(0, dot);
        byte[] given;
        try {
            given = B64D.decode(token.substring(dot + 1));
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
        return MessageDigest.isEqual(mac(body), given)
                ? Optional.of(new String(B64D.decode(body), StandardCharsets.UTF_8))
                : Optional.empty();
    }

    private byte[] mac(String body) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(key);
            return mac.doFinal(body.getBytes(StandardCharsets.US_ASCII));
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new IllegalStateException("HmacSHA256 unavailable", e);
        }
    }
}
