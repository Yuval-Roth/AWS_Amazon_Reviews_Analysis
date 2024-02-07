import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class InterruptableLineReader {

    public class ReadInterruptionException extends Exception {
        public ReadInterruptionException() {
            super(null, null, false, false);
        }
    }

    private AtomicBoolean isInterrupted;

    public InterruptableLineReader() {
        this.isInterrupted = new AtomicBoolean(false);
    }

    public String readLine() throws ReadInterruptionException, IOException {
        try{
            while(System.in.available() == 0){
                if(isInterrupted.get()){
                    throw new ReadInterruptionException();
                }
                try {
                    Thread.sleep(50);
                } catch (InterruptedException ignored) {}
            }
            StringBuilder sb = new StringBuilder();
            int c;
            while( (c = System.in.available()) > 0 && c != '\n'){
                sb.append((char) System.in.read());
            }
            while(System.in.available() > 0){
                System.in.read();
            }
            return sb.toString();
        } catch(ReadInterruptionException e){
            isInterrupted.set(false);
            throw e;
        }
    }

    public void interrupt(){
        isInterrupted.set(true);
    }
}
