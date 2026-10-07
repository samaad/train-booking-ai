package org.presight.ai.profile.profile;

public record Profile(
        String userId,
        String displayName,
        String email,
        String phone,
        String preferredFareClass,
        SeatPreference seatPreference,
        boolean emailNotifications,
        boolean smsNotifications,
        String loyaltyNumber,
        int loyaltyPoints,
        long version) {

    public enum SeatPreference { WINDOW, AISLE, NO_PREFERENCE }
}
