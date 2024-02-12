import java.util.Objects;

public record ClientRequestPart(String clientId, int requestId, int part, String fileName, int reviewsPerWorker, boolean terminate) { }
