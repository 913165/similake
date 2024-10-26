package org.similake.creteria;

// DTOs
public class FilterCriteria {
    private String field;       // field name (brand, price, etc)
    private String operator;    // eq, gt, lt, ne, etc
    private Object value;       // actual value to compare

    // getters, setters

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }
}