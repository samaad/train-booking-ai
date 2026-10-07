package org.presight.ai.profile.profile;

import io.r2dbc.spi.Parameters;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Readable;
import org.presight.ai.profile.profile.Profile.SeatPreference;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

@Repository
class ProfileRepository {

    private final DatabaseClient db;

    ProfileRepository(DatabaseClient db) {
        this.db = db;
    }

    /** First sight of a user creates their profile from token claims; concurrent first requests are safe. */
    Mono<Profile> findOrCreate(String userId, String displayName, String email) {
        return db.sql("""
                        INSERT INTO profile (user_id, display_name, email) VALUES (:userId, :name, :email)
                        ON CONFLICT (user_id) DO NOTHING
                        """)
                .bind("userId", userId)
                .bind("name", Parameters.in(R2dbcType.VARCHAR, displayName))
                .bind("email", Parameters.in(R2dbcType.VARCHAR, email))
                .then()
                .then(find(userId));
    }

    Mono<Profile> find(String userId) {
        return db.sql("SELECT * FROM profile WHERE user_id = :userId").bind("userId", userId)
                .map(ProfileRepository::profile).one();
    }

    /** Optimistic update of the user-editable fields. Email and loyalty are not user-editable. */
    Mono<Long> update(String userId, long expectedVersion, String displayName, String phone, String preferredFareClass,
                      SeatPreference seatPreference, boolean emailNotifications, boolean smsNotifications) {
        return db.sql("""
                        UPDATE profile
                        SET display_name = :name, phone = :phone, preferred_fare_class = :fareClass,
                            seat_preference = :seat, email_notifications = :emailOn, sms_notifications = :smsOn,
                            version = version + 1, updated_at = now()
                        WHERE user_id = :userId AND version = :version
                        """)
                .bind("userId", userId)
                .bind("version", expectedVersion)
                .bind("name", Parameters.in(R2dbcType.VARCHAR, displayName))
                .bind("phone", Parameters.in(R2dbcType.VARCHAR, phone))
                .bind("fareClass", Parameters.in(R2dbcType.VARCHAR, preferredFareClass))
                .bind("seat", seatPreference.name())
                .bind("emailOn", emailNotifications)
                .bind("smsOn", smsNotifications)
                .fetch()
                .rowsUpdated();
    }

    private static Profile profile(Readable row) {
        return new Profile(
                row.get("user_id", String.class),
                row.get("display_name", String.class),
                row.get("email", String.class),
                row.get("phone", String.class),
                row.get("preferred_fare_class", String.class),
                SeatPreference.valueOf(row.get("seat_preference", String.class)),
                Boolean.TRUE.equals(row.get("email_notifications", Boolean.class)),
                Boolean.TRUE.equals(row.get("sms_notifications", Boolean.class)),
                row.get("loyalty_number", String.class),
                row.get("loyalty_points", Integer.class),
                row.get("version", Long.class));
    }
}
