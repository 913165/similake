package org.similake.utils;

import org.similake.creteria.FilterCriteria;
import org.similake.model.Payload;

import java.util.List;

public class Utils {
    public static Object parseValue(String value) {
        // Try parsing as number first
        try {
            if (value.contains(".")) {
                return Double.parseDouble(value);
            }
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            // If not a number, return as string
            return value;
        }
    }



    public static boolean filterPayload(Payload payload, List<FilterCriteria> filters) {
        return filters.stream().allMatch(filter -> {
            Object pointValue = payload.getMetadata().get(filter.getField());
            if (pointValue == null) {
                return false;
            }

            return compareValues(pointValue, filter.getValue(), filter.getOperator());
        });
    }
    private static boolean compareValues(Object pointValue, Object filterValue, String operator) {
        // Convert to comparable if numbers
        if (pointValue instanceof Number && filterValue instanceof Number) {
            double point = ((Number) pointValue).doubleValue();
            double filter = ((Number) filterValue).doubleValue();

            return switch (operator.toLowerCase()) {
                case "eq" -> point == filter;
                case "ne" -> point != filter;
                case "gt" -> point > filter;
                case "lt" -> point < filter;
                case "gte" -> point >= filter;
                case "lte" -> point <= filter;
                default -> false;
            };
        }

        // String comparison
        if (pointValue instanceof String && filterValue instanceof String) {
            String point = (String) pointValue;
            String filter = (String) filterValue;

            return switch (operator.toLowerCase()) {
                case "eq" -> point.equalsIgnoreCase(filter);
                case "ne" -> !point.equalsIgnoreCase(filter);
                case "like" -> point.toLowerCase().contains(filter.toLowerCase());
                default -> false;
            };
        }

        // Default equals comparison for other types
        return operator.equals("eq") && pointValue.equals(filterValue);
    }
}