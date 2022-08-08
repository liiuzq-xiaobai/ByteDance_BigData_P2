package api.operators;

public enum ChainingStrategy {
    ALWAYS,
    NEVER,
    HEAD,
    HEAD_WITH_SOURCES;

    public static final ChainingStrategy DEFAULT_CHAINING_STRATEGY = ALWAYS;

    private ChainingStrategy() {
    }
}