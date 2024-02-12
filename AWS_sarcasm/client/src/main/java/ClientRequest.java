public record ClientRequest(
        int requestId,
        String inputFileName,
        String outputFileName,
        Box<Integer> partsCount,
        Box<ClientMainClass.Status> status
) {
    public void incrementPartsCount() {
        partsCount.set(partsCount.get() + 1);
    }

    public void decrementPartsCount() {
        partsCount.set(partsCount.get() - 1);
        if(partsCount.get() <= 0){
            status.set(ClientMainClass.Status.DONE);
        }
    }

    public boolean isDone() {
        return status.get() == ClientMainClass.Status.DONE;
    }
}
